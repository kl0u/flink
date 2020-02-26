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

package org.apache.flink.yarn.entrypoint;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.runner.application.ApplicationHandler;
import org.apache.flink.runtime.dispatcher.runner.application.EmbeddedApplicationExecutorServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc.
 */
@Internal
public class EmbeddedApplicationSubmitter implements ApplicationHandler {

	private static final Logger LOG = LoggerFactory.getLogger(EmbeddedApplicationSubmitter.class);

	private final JobID jobId;

	private final Configuration configuration;

	private final PackagedProgram executable;

	public EmbeddedApplicationSubmitter(
			final JobID jobId,
			final Configuration configuration,
			final PackagedProgram executable) {
		this.jobId = checkNotNull(jobId);
		this.configuration = checkNotNull(configuration);
		this.executable = checkNotNull(executable);
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
				new EmbeddedApplicationExecutorServiceLoader(jobId, dispatcherGateway, onRecovery);

		try {
			ClientUtils.executeProgram(executorServiceLoader, configuration, executable);
		} catch (ProgramInvocationException e) {
			LOG.warn("Could not execute program: ", e);
			throw new JobSubmissionException(jobId, "Could not execute application (id= " + jobId + ")", e);
		} finally {

			// We are out of the user's main, either due to a failure or
			// due to finishing or cancelling the execution.
			// In any case, it is time to shutdown the cluster

			dispatcherGateway
					.shutDownCluster()
					.thenRun(() -> LOG.info("Cluster was shutdown."));
		}
	}
}
