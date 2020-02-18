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
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.dispatcher.ArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.MemoryArchivedExecutionGraphStore;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.entrypoint.application.EmbeddedApplicationExecutor;
import org.apache.flink.yarn.entrypoint.application.EmbeddedApplicationExecutorServiceLoader;
import org.apache.flink.yarn.entrypoint.application.ProgramUtils;

import org.apache.hadoop.yarn.api.ApplicationConstants;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Javadoc.
 */
@Internal
public class YarnApplicationClusterEntrypoint extends ClusterEntrypoint {

	public YarnApplicationClusterEntrypoint(final Configuration configuration) {
		super(configuration);
	}

	@Override
	protected DispatcherResourceManagerComponentFactory createDispatcherResourceManagerComponentFactory(Configuration configuration) {
		return DefaultDispatcherResourceManagerComponentFactory
				.createApplicationComponentFactory(YarnResourceManagerFactory.getInstance());
	}

	@Override
	protected ArchivedExecutionGraphStore createSerializableExecutionGraphStore(Configuration configuration, ScheduledExecutor scheduledExecutor) {
		return new MemoryArchivedExecutionGraphStore(); // TODO: 30.01.20 think about the fault-tolerance in this case.
	}

	public static void main(String[] args) throws IOException, ProgramInvocationException {
		// startup checks and logging
		EnvironmentInformation.logEnvironmentInfo(LOG, YarnApplicationClusterEntrypoint.class.getSimpleName(), args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		Map<String, String> env = System.getenv();

		final String workingDirectory = env.get(ApplicationConstants.Environment.PWD.key());
		Preconditions.checkArgument(
				workingDirectory != null,
				"Working directory variable (%s) not set",
				ApplicationConstants.Environment.PWD.key());

		try {
			YarnEntrypointUtils.logYarnEnvironmentInformation(env, LOG);
		} catch (IOException e) {
			LOG.warn("Could not log YARN environment information.", e);
		}

		final Configuration configuration = YarnEntrypointUtils.loadConfiguration(workingDirectory, env);

		final YarnApplicationClusterEntrypoint yarnApplicationClusterEntrypoint =
				new YarnApplicationClusterEntrypoint(configuration);
		ClusterEntrypoint.runClusterEntrypoint(yarnApplicationClusterEntrypoint);

		final PackagedProgram executable = ProgramUtils.getExecutable(configuration, env);

		configuration.set(DeploymentOptions.TARGET, EmbeddedApplicationExecutor.NAME);
		configuration.set(DeploymentOptions.ATTACHED, true);

		final CompletableFuture<LeaderGatewayRetriever<DispatcherGateway>> dispatcherRetrieverFuture =
				yarnApplicationClusterEntrypoint.getDispatcherGatewayRetrieverFuture();

		final PipelineExecutorServiceLoader executorServiceLoader =
				new EmbeddedApplicationExecutorServiceLoader(dispatcherRetrieverFuture);

		try {
			// TODO: 05.02.20 rename clientUtils to submitUtils or sth...
			ClientUtils.executeProgram(executorServiceLoader, configuration, executable);
		} catch (Exception e) {
			LOG.warn("Could not execute program: ", e);
		} finally {

			// We are out of the user's main, either due to a failure or
			// due to finishing or cancelling the execution.
			// In any case, it is time to shutdown the cluster

			dispatcherRetrieverFuture
					.thenCompose(LeaderGatewayRetriever::getFuture)
					.thenCompose(RestfulGateway::shutDownCluster)
					.thenRun(() -> LOG.warn("Cluster was shutdown."));
		}
	}
}
