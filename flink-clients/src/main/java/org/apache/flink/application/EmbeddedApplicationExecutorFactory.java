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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.core.execution.PipelineExecutorFactory;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc.
 */
@Internal
public class EmbeddedApplicationExecutorFactory implements PipelineExecutorFactory {

	private final JobID jobId;

	private final DispatcherGateway dispatcherGateway;

	private final boolean inRecovery;

	public EmbeddedApplicationExecutorFactory(
			final JobID jobId,
			final DispatcherGateway dispatcherGateway,
			final boolean inRecovery) {
		this.jobId = checkNotNull(jobId);
		this.dispatcherGateway = checkNotNull(dispatcherGateway);
		this.inRecovery = inRecovery;
	}

	@Override
	public String getName() {
		return EmbeddedApplicationExecutor.NAME;
	}

	@Override
	public boolean isCompatibleWith(final Configuration configuration) {
		return true;
	}

	@Override
	public PipelineExecutor getExecutor(final Configuration configuration) {
		return new EmbeddedApplicationExecutor(jobId, configuration, dispatcherGateway, inRecovery);
	}
}
