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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.deployment.application.executors.EmbeddedExecutorServiceLoader;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.dispatcher.AbstractDispatcherBootstrap;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherBootstrap;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.SerializedThrowable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
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

	private CompletableFuture<Void> applicationExecutionFuture;

	public ApplicationDispatcherBootstrap(
			final PackagedProgram application,
			final Collection<JobGraph> recoveredJobs,
			final Configuration configuration) {
		this.configuration = checkNotNull(configuration);
		this.recoveredJobs = checkNotNull(recoveredJobs);
		this.application = checkNotNull(application);
	}

	@Override
	public void initialize(final Dispatcher dispatcher) {
		checkNotNull(dispatcher);
		launchRecoveredJobGraphs(dispatcher, recoveredJobs);

		runApplicationToCompletionAsync(
				dispatcher,
				dispatcher.getRpcService().getExecutor());

		recoveredJobs.clear();
	}

	@Override
	public void stop() {
		if (applicationExecutionFuture != null) {
			applicationExecutionFuture.cancel(true);
		}
	}

	@VisibleForTesting
	CompletableFuture<Void> runApplicationToCompletionAsync(final DispatcherGateway dispatcher, final Executor executor) {
		applicationExecutionFuture = CompletableFuture
				.supplyAsync(() -> runApplicationAndGetJobIDs(dispatcher), executor)
				.thenCompose(applicationIds -> getApplicationResult(dispatcher, applicationIds));

		applicationExecutionFuture.whenComplete((r, t) -> {
					if (t != null) {
						LOG.warn("Application FAILED: ", t);
					} else {
						LOG.info("Application completed SUCCESSFULLY");
					}
					dispatcher.shutDownCluster();
				});
		return applicationExecutionFuture;
	}

	@VisibleForTesting
	List<JobID> runApplicationAndGetJobIDs(final DispatcherGateway dispatcher) {
		final List<JobID> applicationJobIds = runApplication(dispatcher, application, configuration);
		if (applicationJobIds.isEmpty()) {
			throw new CompletionException(new ApplicationExecutionException("The application contains no execute() calls."));
		}
		return applicationJobIds;
	}

	private List<JobID> runApplication(
			final DispatcherGateway dispatcherGateway,
			final PackagedProgram program,
			final Configuration configuration) {

		final List<JobID> applicationJobIds =
				new ArrayList<>(getRecoveredJobIds(recoveredJobs));

		final PipelineExecutorServiceLoader executorServiceLoader =
				new EmbeddedExecutorServiceLoader(applicationJobIds, dispatcherGateway);

		try {
			ClientUtils.executeProgram(executorServiceLoader, configuration, program, true);
		} catch (ProgramInvocationException e) {
			LOG.warn("Could not execute application: ", e);
			throw new CompletionException("Could not execute application.", e);
		}

		return applicationJobIds;
	}

	@VisibleForTesting
	CompletableFuture<Void> getApplicationResult(
			final DispatcherGateway dispatcherGateway,
			final Collection<JobID> applicationJobIds) {
		final CompletableFuture<?>[] jobResultFutures = applicationJobIds
				.stream()
				.map(jobId -> getJobResult(dispatcherGateway, jobId))
				.toArray(CompletableFuture<?>[]::new);

		final CompletableFuture<Void> allStatusFutures = CompletableFuture.allOf(jobResultFutures);
		Stream.of(jobResultFutures)
				.forEach(f -> f.exceptionally(e -> {
					allStatusFutures.completeExceptionally(e);
					return null;
				}));
		return allStatusFutures;
	}

	private CompletableFuture<Void> getJobResult(final DispatcherGateway dispatcherGateway, final JobID jobId) {
		final Time timeout = Time.milliseconds(configuration.getLong(WebOptions.TIMEOUT));

		final CompletableFuture<Void> jobFuture = new CompletableFuture<>();
		dispatcherGateway
				.requestJobResult(jobId, timeout)
				.thenAccept(result -> {
					final Optional<SerializedThrowable> optionalThrowable = result.getSerializedThrowable();
					if (optionalThrowable.isPresent()) {
						final SerializedThrowable t = optionalThrowable.get();
						LOG.warn("Job {} FAILED: {}", jobId, t.getFullStringifiedStackTrace());
						jobFuture.completeExceptionally(t);
					} else {
						jobFuture.complete(null);
					}
				});
		return jobFuture;
	}

	private List<JobID> getRecoveredJobIds(final Collection<JobGraph> recoveredJobs) {
		return recoveredJobs
				.stream()
				.map(JobGraph::getJobID)
				.collect(Collectors.toList());
	}
}
