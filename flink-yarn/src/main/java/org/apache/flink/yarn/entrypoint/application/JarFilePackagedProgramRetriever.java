/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.yarn.entrypoint.application;

import org.apache.flink.annotation.Internal;
import org.apache.flink.application.AbstractPackagedProgramRetriever;
import org.apache.flink.client.cli.ExecutionConfigAccessor;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Javadoc.
 */
@Internal
public class JarFilePackagedProgramRetriever extends AbstractPackagedProgramRetriever {

	private static final Logger LOG = LoggerFactory.getLogger(JarFilePackagedProgramRetriever.class);

	private final Configuration configuration;

	public JarFilePackagedProgramRetriever(
			final Configuration configuration,
			@Nullable final File userLibDirectory) throws IOException {
		super(userLibDirectory);
		this.configuration = requireNonNull(configuration);
	}

	@Override
	public PackagedProgram getPackagedProgram() throws ProgramInvocationException {
		final ExecutionConfigAccessor configAccessor = ExecutionConfigAccessor.fromConfiguration(configuration);

		final Collection<URL> classpathURLs = getUserClassPaths();
		final Collection<URL> configClasspathURLs = configAccessor.getClasspaths();

		final String urlsString = classpathURLs.stream().map(URL::toString).collect(Collectors.joining(",", "[", "]"));
		final String configUrlsString = configClasspathURLs.stream().map(URL::toString).collect(Collectors.joining(",", "[", "]"));

		LOG.info("DIR: " + urlsString);
		LOG.info("CONFIG: " + configUrlsString);

		final Set<URL> classpaths = new HashSet<>(classpathURLs);
		classpaths.addAll(configAccessor.getClasspaths());

		final String entryPointClass = configAccessor.getMainClassName();

		LOG.info("MAIN: " + entryPointClass);

		final String[] programArgs = configAccessor.getProgramArgs().toArray(new String[0]);
		final SavepointRestoreSettings savepointRestoreSettings = configAccessor.getSavepointRestoreSettings();

		// TODO: 04.03.20 what do we do with the rest of the jars and how do we differentiate the MAIN jar from the rest???
		final List<URL> jars = configAccessor.getJars();
		final File jar = new File(jars.get(0).getPath());

		return PackagedProgram.newBuilder()
				.setJarFile(jar)
				.setUserClassPaths(new ArrayList<>(classpaths))
				.setEntryPointClassName(entryPointClass)
				.setConfiguration(configuration)
				.setSavepointRestoreSettings(savepointRestoreSettings)
				.setArguments(programArgs)
				.build();
	}
}
