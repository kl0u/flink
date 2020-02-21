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
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc.
 */
@Internal
public class EmbeddedClusterDescriptor implements ClusterDescriptor<String> {

	private static final String CLUSTER_DESCRIPTION = "The cluster descriptor for Application Mode execution. " +
			"This implies a cluster available ONLY to the Jobs of a single Application, or the jobGraphs executed " +
			"by the execute() calls of a single main() method.";

	private final Configuration configuration;

	private final DispatcherGateway dispatcherGateway;

	public EmbeddedClusterDescriptor(
			final Configuration configuration,
			final DispatcherGateway dispatcherGateway) {
		this.configuration = checkNotNull(configuration);
		this.dispatcherGateway = checkNotNull(dispatcherGateway);
	}

	@Override
	public String getClusterDescription() {
		return CLUSTER_DESCRIPTION;
	}

	@Override
	public ClusterClientProvider<String> retrieve(String clusterId) {
		return new EmbeddedClusterClientProvider(configuration, dispatcherGateway);
	}

	@Override
	public ClusterClientProvider<String> deploySessionCluster(ClusterSpecification clusterSpecification) {
		throw new UnsupportedOperationException("The EmbeddedClusterDescriptor assumes an already running cluster.");
	}

	@Override
	public ClusterClientProvider<String> deployApplicationCluster(ClusterSpecification clusterSpecification) {
		throw new UnsupportedOperationException("The EmbeddedClusterDescriptor assumes an already running cluster.");
	}

	@Override
	public ClusterClientProvider<String> deployJobCluster(ClusterSpecification clusterSpecification, JobGraph jobGraph, boolean detached) {
		throw new UnsupportedOperationException("The EmbeddedClusterDescriptor assumes an already running cluster.");
	}

	@Override
	public void killCluster(String clusterId) {
		throw new UnsupportedOperationException("Cannot terminate an embedded cluster.");
	}

	@Override
	public void close() {
		// do nothing
	}
}
