/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.client.deployment.application;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.deployment.application.executors.EmbeddedExecutorServiceLoader;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.dispatcher.AbstractDispatcherBootstrap;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherBootstrap;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.SerializedThrowable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link DispatcherBootstrap} used for running the user's {@code main()} in "Application Mode" (see FLIP-85).
 *
 * <p>This dispatcher bootstrap submits the recovered {@link JobGraph job graphs} for re-execution
 * (in case of recovery from a failure), and then submits the remaining jobs of the application for execution.
 *
 * <p>To achieve this, it works in conjunction with the
 * {@link org.apache.flink.client.deployment.application.executors.EmbeddedExecutor EmbeddedExecutor} which decides
 * if it should submit a job for execution (in case of a new job) or the job was already recovered and is running.
 */
@Internal
public class ApplicationDispatcherBootstrap extends AbstractDispatcherBootstrap {

	private static final Logger LOG = LoggerFactory.getLogger(ApplicationDispatcherBootstrap.class);

	private final PackagedProgram application;

	private final Collection<JobGraph> recoveredJobs;

	private final Configuration configuration;

	private final CompletableFuture<Void> applicationStatusFuture;

	private CompletableFuture<Void> applicationExecutionFuture;

	public ApplicationDispatcherBootstrap(
			final PackagedProgram application,
			final Collection<JobGraph> recoveredJobs,
			final Configuration configuration) {
		this.configuration = checkNotNull(configuration);
		this.recoveredJobs = checkNotNull(recoveredJobs);
		this.application = checkNotNull(application);

		this.applicationStatusFuture = new CompletableFuture<>();
	}

	public CompletableFuture<Void> getApplicationStatusFuture() {
		return applicationStatusFuture;
	}

	@Override
	public void initialize(final Dispatcher dispatcher) {
		checkNotNull(dispatcher);
		launchRecoveredJobGraphs(dispatcher, recoveredJobs);

		runApplicationAndShutdownClusterAsync(
				dispatcher,
				dispatcher.getRpcService().getScheduledExecutor());
		// TODO: 06.04.20 Do we need the main thread executor?
	}

	@Override
	public void stop() {
		if (applicationExecutionFuture != null) {
			applicationExecutionFuture.cancel(true);
		}
	}

	@VisibleForTesting
	CompletableFuture<Acknowledge> runApplicationAndShutdownClusterAsync(
			final DispatcherGateway dispatcher,
			final ScheduledExecutor scheduledExecutor) {

		applicationExecutionFuture = CompletableFuture
				.supplyAsync(() -> runApplicationAndGetJobIDs(dispatcher, scheduledExecutor, true), scheduledExecutor)
				.thenCompose(applicationIds -> getApplicationResult(dispatcher, applicationIds, scheduledExecutor));

		applicationExecutionFuture.whenComplete((r, t) -> {
					if (t != null) {
						LOG.warn("Application FAILED: ", t);
						applicationStatusFuture.completeExceptionally(t);
					} else {
						LOG.info("Application completed SUCCESSFULLY");
						applicationStatusFuture.complete(null);
					}
				});

		return applicationStatusFuture.handle((r, t) -> dispatcher.shutDownCluster().join());
	}

	@VisibleForTesting
	List<JobID> runApplicationAndGetJobIDs(
			final DispatcherGateway dispatcher,
			final ScheduledExecutor scheduledExecutor,
			final boolean enforceSingleJobExecution) {
		final List<JobID> applicationJobIds = runApplication(dispatcher, application, configuration, scheduledExecutor, enforceSingleJobExecution);
		if (applicationJobIds.isEmpty()) {
			throw new CompletionException(new ApplicationExecutionException("The application contains no execute() calls."));
		}
		return applicationJobIds;
	}

	private List<JobID> runApplication(
			final DispatcherGateway dispatcherGateway,
			final PackagedProgram program,
			final Configuration configuration,
			final ScheduledExecutor scheduledExecutor,
			final boolean enforceSingleJobExecution) {

		final List<JobID> applicationJobIds =
				new ArrayList<>(getRecoveredJobIds(recoveredJobs));

		final PipelineExecutorServiceLoader executorServiceLoader =
				new EmbeddedExecutorServiceLoader(applicationJobIds, dispatcherGateway, scheduledExecutor);

		try {
			ClientUtils.executeProgram(executorServiceLoader, configuration, program, enforceSingleJobExecution);
		} catch (ProgramInvocationException e) {
			LOG.warn("Could not execute application: ", e);
			throw new CompletionException("Could not execute application.", e);
		}

		return applicationJobIds;
	}

	@VisibleForTesting
	CompletableFuture<Void> getApplicationResult(
			final DispatcherGateway dispatcherGateway,
			final Collection<JobID> applicationJobIds,
			final ScheduledExecutor scheduledExecutor) {
		final CompletableFuture<?>[] jobResultFutures = applicationJobIds
				.stream()
				.map(jobId -> getJobResult(dispatcherGateway, jobId, scheduledExecutor))
				.toArray(CompletableFuture<?>[]::new);

		final CompletableFuture<Void> allStatusFutures = CompletableFuture.allOf(jobResultFutures);
		Stream.of(jobResultFutures)
				.forEach(f -> f.exceptionally(e -> {
					allStatusFutures.completeExceptionally(e);
					return null;
				}));
		return allStatusFutures;
	}

	private CompletableFuture<Void> getJobResult(final DispatcherGateway dispatcherGateway, final JobID jobId, final ScheduledExecutor scheduledExecutor) {
		final CompletableFuture<Void> jobFuture = new CompletableFuture<>();
		getJobStatus(dispatcherGateway, jobId, scheduledExecutor)
				.whenComplete((result, throwable) -> {
					if (throwable != null) {
						LOG.warn("Job {} FAILED: {}", jobId, throwable);
						jobFuture.completeExceptionally(throwable);
					} else {
						final Optional<SerializedThrowable> optionalThrowable = result.getSerializedThrowable();
						if (optionalThrowable.isPresent()) {
							final SerializedThrowable t = optionalThrowable.get();
							LOG.warn("Job {} FAILED: {}", jobId, t.getFullStringifiedStackTrace());
							jobFuture.completeExceptionally(t);
						} else {
							jobFuture.complete(null);
						}
					}
				});
		return jobFuture;
	}

	public CompletableFuture<JobResult> getJobStatus(
			final DispatcherGateway dispatcherGateway,
			final JobID jobId,
			final ScheduledExecutor scheduledExecutor) {
		final Time timeout = Time.milliseconds(configuration.getLong(WebOptions.TIMEOUT));

		return pollJobResultAsync(
				() -> dispatcherGateway.requestJobStatus(jobId, timeout),
				() -> dispatcherGateway.requestJobResult(jobId, timeout),
				scheduledExecutor,
				100L
		);
	}

	private CompletableFuture<JobResult> pollJobResultAsync(
			final Supplier<CompletableFuture<JobStatus>> jobStatusSupplier,
			final Supplier<CompletableFuture<JobResult>> jobResultSupplier,
			final ScheduledExecutor scheduledExecutor,
			final long retryMsTimeout) {
		return pollJobResultAsync(jobStatusSupplier, jobResultSupplier, scheduledExecutor, new CompletableFuture<>(), retryMsTimeout, 0);
	}

	private CompletableFuture<JobResult> pollJobResultAsync(
			final Supplier<CompletableFuture<JobStatus>> jobStatusSupplier,
			final Supplier<CompletableFuture<JobResult>> jobResultSupplier,
			final ScheduledExecutor scheduledExecutor,
			final CompletableFuture<JobResult> resultFuture,
			final long retryMsTimeout,
			final long attempt) {

		jobStatusSupplier.get().whenComplete((jobStatus, throwable) -> {
			if (throwable != null) {
				resultFuture.completeExceptionally(throwable);
			} else {
				if (jobStatus.isGloballyTerminalState()) {
					jobResultSupplier.get().whenComplete((jobResult, t) -> {

						if  (t != null) {
							resultFuture.completeExceptionally(t);
						} else {
							resultFuture.complete(jobResult);
						}
					});
				} else {
					scheduledExecutor.schedule(() -> {
						pollJobResultAsync(jobStatusSupplier, jobResultSupplier, scheduledExecutor, resultFuture, retryMsTimeout, attempt + 1);
					}, retryMsTimeout, TimeUnit.MILLISECONDS);
				}
			}
		});

		return resultFuture;
	}

	private List<JobID> getRecoveredJobIds(final Collection<JobGraph> recoveredJobs) {
		return recoveredJobs
				.stream()
				.map(JobGraph::getJobID)
				.collect(Collectors.toList());
	}
}
