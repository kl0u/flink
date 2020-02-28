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

package org.apache.flink.container.entrypoint;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.runner.application.ApplicationHandler;
import org.apache.flink.runtime.dispatcher.runner.application.EmbeddedExecutorServiceLoader;
import org.apache.flink.runtime.entrypoint.component.Executable;
import org.apache.flink.runtime.entrypoint.component.ExecutableExtractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * Javadoc.
 * THIS IS SIMILAR TO CLASSPATHRETRIEVER
 */
@Internal
public class EmbeddedApplicationHandler implements ApplicationHandler {

	private static final Logger LOG = LoggerFactory.getLogger(ClassPathJobGraphRetriever.class);

	private final Configuration configuration;

	private final JobID jobId;

	private final String[] programArguments;

	@Nullable
	private final String jobClassName;

	@Nullable
	private final File userLibDirectory;

	public EmbeddedApplicationHandler(
			final Configuration configuration,
			final JobID jobId,
			final String[] programArguments,
			@Nullable final String jobClassName,
			@Nullable final File userLibDirectory) {

		this.configuration = requireNonNull(configuration);
		this.jobId = requireNonNull(jobId, "jobId");
		this.programArguments = requireNonNull(programArguments, "programArguments");

		this.jobClassName = jobClassName;
		this.userLibDirectory = userLibDirectory;
	}

	@Override
	public JobID getJobId() {
		return jobId;
	}

	@Override
	public void recover(DispatcherGateway dispatcherGateway) throws JobExecutionException {
		applicationHandlerHelper(dispatcherGateway, true);
	}

	@Override
	public void submit(final DispatcherGateway dispatcherGateway) throws JobSubmissionException {
		applicationHandlerHelper(dispatcherGateway, false);
	}

	private void applicationHandlerHelper(final DispatcherGateway dispatcherGateway, final boolean onRecovery) throws JobSubmissionException {
		final ExecutableExtractor executableExtractor = getExecutableExtractor();
		final Executable executable = createExecutable(executableExtractor);
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

			dispatcherGateway
					.shutDownCluster()
					.thenRun(() -> LOG.info("Cluster was shutdown."));
		}
	}

	private Executable createExecutable(final ExecutableExtractor executableExtractor) throws JobSubmissionException {
		requireNonNull(executableExtractor, "executable extractor");
		try {
			return executableExtractor.createExecutable();
		} catch (Exception e) {
			LOG.error("Failed to create executable for job {}.", jobId, e);
			throw new JobSubmissionException(jobId, "Failed to create executable.", e);
		}
	}

	private ExecutableExtractor getExecutableExtractor() throws JobSubmissionException {
		final ExecutableExtractorImpl.Builder executableBuilder = ExecutableExtractorImpl
				.newBuilder(programArguments)
				.setJobClassName(jobClassName);

		if (userLibDirectory != null) {
			executableBuilder.setUserLibDirectory(userLibDirectory);
		}

		try {
			return executableBuilder.build();
		} catch (IOException e) {
			LOG.error("Failed to find user lib dir for job {}.", jobId, e);
			throw new JobSubmissionException(jobId, "Failed to find user lib dir.", e);
		}
	}
}
