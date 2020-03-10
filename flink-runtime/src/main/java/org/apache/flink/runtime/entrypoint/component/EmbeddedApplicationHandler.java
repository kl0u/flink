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

package org.apache.flink.runtime.entrypoint.component;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.runner.application.ApplicationHandler;
import org.apache.flink.runtime.dispatcher.runner.application.EmbeddedExecutorServiceLoader;
import org.apache.flink.runtime.jobmaster.JobResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

/**
 * Javadoc.
 */
@Internal
public class EmbeddedApplicationHandler implements ApplicationHandler {

	private static final Logger LOG = LoggerFactory.getLogger(EmbeddedApplicationHandler.class);

	private final JobID jobId;

	private final Configuration configuration;

	private final Executable executable;

	public EmbeddedApplicationHandler(
			final JobID jobId,
			final Configuration configuration,
			final Executable executable) {
		this.jobId = requireNonNull(jobId);
		this.configuration = requireNonNull(configuration);
		this.executable = requireNonNull(executable);
	}

	@Override
	public JobID getJobId() {
		return jobId;
	}

	@Override
	public void submit(final DispatcherGateway dispatcherGateway) throws JobSubmissionException {
		applicationHandlerHelper(dispatcherGateway, false);
	}

	@Override
	public void recover(DispatcherGateway dispatcherGateway) throws JobExecutionException {
		applicationHandlerHelper(dispatcherGateway, true);
	}

	private void applicationHandlerHelper(final DispatcherGateway dispatcherGateway, final boolean onRecovery) throws JobSubmissionException {
		final PipelineExecutorServiceLoader executorServiceLoader =
				new EmbeddedExecutorServiceLoader(jobId, dispatcherGateway, onRecovery);

		try {
			executable.execute(executorServiceLoader, configuration);
		} catch (Exception e) {
			LOG.warn("Could not execute program: ", e);
			throw new JobSubmissionException(jobId, "Could not execute application (id= " + jobId + ")", e);
		} finally {

			// We are out of the user's main, either due to a failure or
			// due to finishing or cancelling the execution.
			// In any case, it is time to shutdown the cluster

			handleJobExecutionResult(dispatcherGateway);
		}
	}

	private CompletableFuture<Void> handleJobExecutionResult(final DispatcherGateway dispatcherGateway) {
		requireNonNull(dispatcherGateway);

		final Time timeout = Time.milliseconds(configuration.getLong(WebOptions.TIMEOUT));
		final CompletableFuture<JobResult> jobResultFuture = dispatcherGateway.requestJobResult(jobId, timeout);

		return jobResultFuture.thenAccept((JobResult result) -> {
			ApplicationStatus status = result.getSerializedThrowable().isPresent() ?
					ApplicationStatus.FAILED : ApplicationStatus.SUCCEEDED;

			LOG.debug("Shutting down application cluster because the application finished with status={}.", status);
			dispatcherGateway.shutDownCluster();
		});
	}
}
