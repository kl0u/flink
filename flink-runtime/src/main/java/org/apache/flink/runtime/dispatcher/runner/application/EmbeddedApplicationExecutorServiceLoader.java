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

package org.apache.flink.runtime.dispatcher.runner.application;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.PipelineExecutorFactory;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;

import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc.
 */
@Internal
public class EmbeddedApplicationExecutorServiceLoader implements PipelineExecutorServiceLoader {

	private final JobID jobId;

	private final DispatcherGateway dispatcherGateway;

	public EmbeddedApplicationExecutorServiceLoader(
			final JobID jobId,
			final DispatcherGateway dispatcherGateway) {
		this.jobId = checkNotNull(jobId);
		this.dispatcherGateway = checkNotNull(dispatcherGateway);
	}

	@Override
	public PipelineExecutorFactory getExecutorFactory(final Configuration configuration) {
		return new EmbeddedApplicationExecutorFactory(jobId, dispatcherGateway);
	}

	@Override
	public Stream<String> getExecutorNames() {
		return Stream.<String>builder().add(EmbeddedApplicationExecutor.NAME).build();
	}
}
