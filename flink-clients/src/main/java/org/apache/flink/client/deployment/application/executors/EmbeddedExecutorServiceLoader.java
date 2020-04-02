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

package org.apache.flink.client.deployment.application.executors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.PipelineExecutorFactory;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;

import java.util.Collection;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * A {@link PipelineExecutorServiceLoader} that always returns an {@link EmbeddedExecutorFactory}.
 * This is useful when running the user's main on the cluster.
 */
@Internal
public class EmbeddedExecutorServiceLoader implements PipelineExecutorServiceLoader {

	private final Collection<JobID> applicationJobIds;

	private final DispatcherGateway dispatcherGateway;


	/**
	 * Creates a {@link EmbeddedExecutorServiceLoader}.
	 *
	 * @param applicationJobIds a list initialized with the recovered jobs (if any). This list is also going to be filled
	 *                          by the {@link EmbeddedExecutor} with the job ids of the new jobs that will be submitted. The
	 *                          list can later be used for monitoring the status of the submitted/recovered jobs.
	 * @param dispatcherGateway the dispatcher of the cluster which is going to be used to submit jobs.
	 */
	public EmbeddedExecutorServiceLoader(
			final Collection<JobID> applicationJobIds,
			final DispatcherGateway dispatcherGateway) {
		this.applicationJobIds = requireNonNull(applicationJobIds);
		this.dispatcherGateway = requireNonNull(dispatcherGateway);
	}

	@Override
	public PipelineExecutorFactory getExecutorFactory(final Configuration configuration) {
		return new EmbeddedExecutorFactory(applicationJobIds, dispatcherGateway);
	}

	@Override
	public Stream<String> getExecutorNames() {
		return Stream.<String>builder().add(EmbeddedExecutor.NAME).build();
	}
}
