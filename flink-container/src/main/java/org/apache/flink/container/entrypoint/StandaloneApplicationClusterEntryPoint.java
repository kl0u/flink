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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.application.ApplicationDispatcherFactory;
import org.apache.flink.application.ApplicationDispatcherLeaderProcessFactoryFactory;
import org.apache.flink.application.EmbeddedApplicationExecutor;
import org.apache.flink.application.EmbeddedApplicationHandler;
import org.apache.flink.application.ExecutableExtractor;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.runtime.dispatcher.runner.DefaultDispatcherRunnerFactory;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.JobClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManagerFactory;
import org.apache.flink.runtime.rest.JobRestEndpointFactory;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;

import javax.annotation.Nullable;

import java.io.IOException;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.runtime.util.ClusterEntrypointUtils.tryFindUserLibDirectory;

/**
 * Javadoc.
 */
@Internal
public class StandaloneApplicationClusterEntryPoint extends JobClusterEntrypoint {

	private final String[] programArguments;

	private final String jobClassName;

	public StandaloneApplicationClusterEntryPoint(
			final Configuration configuration,
			final String[] programArguments,
			@Nullable String jobClassName) {
		super(configuration);
		this.programArguments = requireNonNull(programArguments, "programArguments");
		this.jobClassName = jobClassName;
	}

	@Override
	protected DispatcherResourceManagerComponentFactory createDispatcherResourceManagerComponentFactory(Configuration configuration) throws Exception {
		final ExecutableExtractor executableExtractor = getExecutableExtractor();
		final PackagedProgram executable = executableExtractor.createExecutable();

		final EmbeddedApplicationHandler applicationHandler =
				new EmbeddedApplicationHandler(configuration, executable);

		return new DefaultDispatcherResourceManagerComponentFactory(
				new DefaultDispatcherRunnerFactory(
						ApplicationDispatcherLeaderProcessFactoryFactory
								.create(ApplicationDispatcherFactory.INSTANCE, applicationHandler)),
				StandaloneResourceManagerFactory.INSTANCE,
				JobRestEndpointFactory.INSTANCE);
	}

	private ExecutableExtractor getExecutableExtractor() throws IOException {
		final ExecutableExtractorImpl.Builder executableBuilder = ExecutableExtractorImpl
				.newBuilder(programArguments)
				.setJobClassName(jobClassName);

		tryFindUserLibDirectory().ifPresent(executableBuilder::setUserLibDirectory);
		return executableBuilder.build();
	}

	public static void main(String[] args) {
		// startup checks and logging
		EnvironmentInformation.logEnvironmentInfo(LOG, StandaloneJobClusterEntryPoint.class.getSimpleName(), args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		final CommandLineParser<StandaloneJobClusterConfiguration> commandLineParser = new CommandLineParser<>(new StandaloneJobClusterConfigurationParserFactory());
		StandaloneJobClusterConfiguration clusterConfiguration = null;

		try {
			clusterConfiguration = commandLineParser.parse(args);
		} catch (Exception e) {
			LOG.error("Could not parse command line arguments {}.", args, e);
			commandLineParser.printHelp(StandaloneJobClusterEntryPoint.class.getSimpleName());
			System.exit(1);
		}

		Configuration configuration = loadConfiguration(clusterConfiguration);
		setDefaultExecutionModeIfNotConfigured(configuration);
		configuration.set(DeploymentOptions.TARGET, EmbeddedApplicationExecutor.NAME);
		configuration.set(DeploymentOptions.ATTACHED, true);

		StandaloneApplicationClusterEntryPoint entrypoint = new StandaloneApplicationClusterEntryPoint(
				configuration,
				clusterConfiguration.getArgs(),
				clusterConfiguration.getJobClassName());

		ClusterEntrypoint.runClusterEntrypoint(entrypoint);
	}

	@VisibleForTesting
	static void setDefaultExecutionModeIfNotConfigured(Configuration configuration) {
		if (isNoExecutionModeConfigured(configuration)) {
			// In contrast to other places, the default for standalone job clusters is ExecutionMode.DETACHED
			configuration.setString(ClusterEntrypoint.EXECUTION_MODE, ExecutionMode.DETACHED.toString());
		}
	}

	private static boolean isNoExecutionModeConfigured(Configuration configuration) {
		return configuration.getString(ClusterEntrypoint.EXECUTION_MODE, null) == null;
	}
}
