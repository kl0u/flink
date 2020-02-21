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

package org.apache.flink.yarn.entrypoint.application;

import org.apache.flink.annotation.Internal;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc.
 */
@Internal
public class EmbeddedClusterClientFactory implements ClusterClientFactory<String> {

	private final DispatcherGateway dispatcherGateway;

	public EmbeddedClusterClientFactory(final DispatcherGateway dispatcherGateway) {
		this.dispatcherGateway = checkNotNull(dispatcherGateway);
	}

	@Override
	public boolean isCompatibleWith(Configuration configuration) {
		return EmbeddedApplicationExecutor.NAME.equalsIgnoreCase(configuration.get(DeploymentOptions.TARGET));
	}

	@Override
	public ClusterDescriptor<String> createClusterDescriptor(final Configuration configuration) {
		return new EmbeddedClusterDescriptor(configuration, dispatcherGateway);
	}

	@Nullable
	@Override
	public String getClusterId(Configuration configuration) {
		return EmbeddedApplicationExecutor.NAME;
	}

	@Override
	public ClusterSpecification getClusterSpecification(Configuration configuration) {
		throw new UnsupportedOperationException("The EmbeddedClusterClientFactory assumes an already running cluster.");
	}
}
