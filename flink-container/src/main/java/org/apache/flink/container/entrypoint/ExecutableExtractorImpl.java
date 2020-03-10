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
import org.apache.flink.application.AbstractExecutableExtractor;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.function.Supplier;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Javadoc.
 */
@Internal
public class ExecutableExtractorImpl extends AbstractExecutableExtractor {

	private static final Logger LOG = LoggerFactory.getLogger(ExecutableExtractorImpl.class);

	private final String[] programArguments;

	private final Supplier<Iterable<File>> jarsOnClassPath;

	@Nullable
	private final String jobClassName;

	@Nullable
	private final File userLibDirectory;

	private ExecutableExtractorImpl(
			@Nonnull String[] programArguments,
			@Nonnull Supplier<Iterable<File>> jarsOnClassPath,
			@Nullable String jobClassName,
			@Nullable File userLibDirectory) throws IOException {
		super(userLibDirectory);
		this.userLibDirectory = userLibDirectory;
		this.programArguments = requireNonNull(programArguments, "programArguments");
		this.jobClassName = jobClassName;
		this.jarsOnClassPath = requireNonNull(jarsOnClassPath);
	}

	@Override
	public PackagedProgram createExecutable() throws Exception {
		final String entryClass = getJobClassNameOrScanClassPath();
		try {
			return PackagedProgram.newBuilder()
					.setUserClassPaths(new ArrayList<>(getUserClassPaths()))
					.setEntryPointClassName(entryClass)
					.setArguments(programArguments)
					.build();
		} catch (ProgramInvocationException e) {
			throw new FlinkException("Could not load the provided entrypoint class.", e);
		}
	}

	private String getJobClassNameOrScanClassPath() throws FlinkException {
		if (jobClassName != null) {
			if (userLibDirectory != null) {
				// check that we find the entrypoint class in the user lib directory.
				if (!userClassPathContainsJobClass(jobClassName)) {
					throw new FlinkException(
							String.format(
									"Could not find the provided job class (%s) in the user lib directory (%s).",
									jobClassName,
									userLibDirectory));
				}
			}
			return jobClassName;
		}

		try {
			return scanClassPathForJobJar();
		} catch (IOException | NoSuchElementException | IllegalArgumentException e) {
			throw new FlinkException("Failed to find job JAR on class path. Please provide the job class name explicitly.", e);
		}
	}

	private boolean userClassPathContainsJobClass(String jobClassName) {
		for (URL userClassPath : getUserClassPaths()) {
			try (final JarFile jarFile = new JarFile(userClassPath.getFile())) {
				if (jarContainsJobClass(jobClassName, jarFile)) {
					return true;
				}
			} catch (IOException e) {
				ExceptionUtils.rethrow(
						e,
						String.format(
								"Failed to open user class path %s. Make sure that all files on the user class path can be accessed.",
								userClassPath));
			}
		}
		return false;
	}

	private boolean jarContainsJobClass(String jobClassName, JarFile jarFile) {
		return jarFile
				.stream()
				.map(JarEntry::getName)
				.filter(fileName -> fileName.endsWith(FileUtils.CLASS_FILE_EXTENSION))
				.map(FileUtils::stripFileExtension)
				.map(fileName -> fileName.replaceAll(Pattern.quote(File.separator), FileUtils.PACKAGE_SEPARATOR))
				.anyMatch(name -> name.equals(jobClassName));
	}

	private String scanClassPathForJobJar() throws IOException {
		final Iterable<File> jars;
		if (userLibDirectory == null) {
			LOG.info("Scanning system class path for job JAR");
			jars = jarsOnClassPath.get();
		} else {
			LOG.info("Scanning user class path for job JAR");
			jars = getUserClassPaths()
					.stream()
					.map(url -> new File(url.getFile()))
					.collect(Collectors.toList());
		}

		final JarManifestParser.JarFileWithEntryClass jobJar = JarManifestParser.findOnlyEntryClass(jars);
		LOG.info("Using {} as job jar", jobJar);
		return jobJar.getEntryClass();
	}

	@VisibleForTesting
	enum JarsOnClassPath implements Supplier<Iterable<File>> {
		INSTANCE;

		static final String JAVA_CLASS_PATH = "java.class.path";
		static final String PATH_SEPARATOR = "path.separator";
		static final String DEFAULT_PATH_SEPARATOR = ":";

		@Override
		public Iterable<File> get() {
			String classPath = System.getProperty(JAVA_CLASS_PATH, "");
			String pathSeparator = System.getProperty(PATH_SEPARATOR, DEFAULT_PATH_SEPARATOR);

			return Arrays.stream(classPath.split(pathSeparator))
					.filter(JarsOnClassPath::notNullAndNotEmpty)
					.map(File::new)
					.filter(File::isFile)
					.collect(Collectors.toList());
		}

		private static boolean notNullAndNotEmpty(String string) {
			return string != null && !string.equals("");
		}
	}

	static class Builder {

		private final String[] programArguments;

		@Nullable
		private String jobClassName;

		@Nullable
		private File userLibDirectory;

		private Supplier<Iterable<File>> jarsOnClassPath = JarsOnClassPath.INSTANCE;

		private Builder(String[] programArguments) {
			this.programArguments = requireNonNull(programArguments);
		}

		Builder setJobClassName(@Nullable String jobClassName) {
			this.jobClassName = jobClassName;
			return this;
		}

		Builder setUserLibDirectory(File userLibDirectory) {
			this.userLibDirectory = userLibDirectory;
			return this;
		}

		Builder setJarsOnClassPath(Supplier<Iterable<File>> jarsOnClassPath) {
			this.jarsOnClassPath = jarsOnClassPath;
			return this;
		}

		ExecutableExtractorImpl build() throws IOException {
			return new ExecutableExtractorImpl(
					programArguments,
					jarsOnClassPath,
					jobClassName,
					userLibDirectory);
		}
	}

	static Builder newBuilder(String[] programArguments) {
		return new Builder(programArguments);
	}
}
