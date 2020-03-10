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

package org.apache.flink.application;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.util.SerializedThrowable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

/**
 * Javadoc.
 */
@Internal
public class EmbeddedApplicationHandler implements ApplicationHandler {

	private static final Logger LOG = LoggerFactory.getLogger(EmbeddedApplicationHandler.class);

	private final Configuration configuration;

	private final PackagedProgram executable;

	public EmbeddedApplicationHandler(
			final Configuration configuration,
			final PackagedProgram executable) {
		this.configuration = requireNonNull(configuration);
		this.executable = requireNonNull(executable);
	}

	@Override
	public void launch(final Collection<JobID> recoveredJobIds, final DispatcherGateway dispatcherGateway) {
		requireNonNull(recoveredJobIds);
		requireNonNull(dispatcherGateway);

		tryExecuteApplication(recoveredJobIds, dispatcherGateway)
					.whenComplete((r, t) -> dispatcherGateway.shutDownCluster());
	}

	private CompletableFuture<Void> tryExecuteApplication(
			final Collection<JobID> recoveredJobIds,
			final DispatcherGateway dispatcherGateway) {
		final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();

		final List<JobID> applicationJobIds = tryExecuteJobs(recoveredJobIds, dispatcherGateway, terminationFuture);
		monitorApplicationStatus(dispatcherGateway, applicationJobIds, terminationFuture);
		return terminationFuture;
	}

	private List<JobID> tryExecuteJobs(
			final Collection<JobID> recoveredJobIds,
			final DispatcherGateway dispatcherGateway,
			final CompletableFuture<Void> terminationFuture) {

		final List<JobID> applicationJobIds = new ArrayList<>(recoveredJobIds);
		final PipelineExecutorServiceLoader executorServiceLoader =
				new EmbeddedExecutorServiceLoader(applicationJobIds, recoveredJobIds, dispatcherGateway);

		try {
			ClientUtils.executeProgram(executorServiceLoader, configuration, executable);
		} catch (ProgramInvocationException e) {
			LOG.warn("Could not execute application: ", e);

			terminationFuture.completeExceptionally(
					new ApplicationSubmissionException("Could not execute application.", e));
		}

		return applicationJobIds;
	}

	private void monitorApplicationStatus(
			final DispatcherGateway dispatcherGateway,
			final List<JobID> applicationIds,
			final CompletableFuture<Void> terminationFuture) {
		if (applicationIds.isEmpty()) {
			LOG.warn("Submitted application with no execute() calls.");

			terminationFuture.completeExceptionally(
					new ApplicationSubmissionException("Submitted application with no execute() calls."));
		}

		final CompletableFuture<?>[] allTerminationFutures = applicationIds
				.stream()
				.map(jobId -> monitorJobStatus(dispatcherGateway, jobId, terminationFuture))
				.toArray(CompletableFuture<?>[]::new);

		CompletableFuture.allOf(allTerminationFutures)
				.whenComplete((r, t) -> {
					if (t != null) {
						LOG.info("Application FAILED: ", t);
						terminationFuture.completeExceptionally(t);
					} else {
						LOG.info("Application SUCCEEDED.");
						terminationFuture.complete(null);
					}
				});
	}

	private CompletableFuture<Void> monitorJobStatus(
			final DispatcherGateway dispatcherGateway,
			final JobID jobId,
			final CompletableFuture<Void> terminationFuture) {

		final Time timeout = Time.milliseconds(configuration.getLong(WebOptions.TIMEOUT));

		return dispatcherGateway
				.requestJobResult(jobId, timeout)
				.thenAccept(result -> {
					if (result.getSerializedThrowable().isPresent()) {
						SerializedThrowable t = result.getSerializedThrowable().get();
						LOG.info("Job {} FAILED: ", jobId, t);
						terminationFuture.completeExceptionally(t);
					} else {
						LOG.info("Job {} SUCCEEDED.", jobId);
					}
				});
	}
}
