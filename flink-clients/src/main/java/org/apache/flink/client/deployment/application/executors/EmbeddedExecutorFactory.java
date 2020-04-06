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
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.core.execution.PipelineExecutorFactory;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;

import java.util.Collection;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An {@link PipelineExecutorFactory} for the {@link EmbeddedExecutor}.
 */
@Internal
public class EmbeddedExecutorFactory implements PipelineExecutorFactory {

	private final Collection<JobID> applicationJobIds;

	private final DispatcherGateway dispatcherGateway;

	private final ScheduledExecutor retryExecutor;

	public EmbeddedExecutorFactory(
			final Collection<JobID> applicationJobIds,
			final DispatcherGateway dispatcherGateway,
			final ScheduledExecutor retryExecutor) {
		this.applicationJobIds = requireNonNull(applicationJobIds);
		this.dispatcherGateway = requireNonNull(dispatcherGateway);
		this.retryExecutor = checkNotNull(retryExecutor);
	}

	@Override
	public String getName() {
		return EmbeddedExecutor.NAME;
	}

	@Override
	public boolean isCompatibleWith(final Configuration configuration) {
		// this is always false because we simply have a special executor loader
		// for this one that does not check for compatibility.
		return false;
	}

	@Override
	public PipelineExecutor getExecutor(final Configuration configuration) {
		requireNonNull(configuration);
		return new EmbeddedExecutor(applicationJobIds, dispatcherGateway, retryExecutor);
	}
}
