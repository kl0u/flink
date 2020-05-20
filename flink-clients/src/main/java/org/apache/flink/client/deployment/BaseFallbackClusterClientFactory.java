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

package org.apache.flink.client.deployment;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An abstract {@link FallbackClusterClientFactory} that throws a
 * {@link ClusterDeploymentException} when any method is called.
 */
@Internal
public abstract class BaseFallbackClusterClientFactory implements FallbackClusterClientFactory<Void> {

	private final String errorMessage;

	public BaseFallbackClusterClientFactory(final String errorMessage) {
		this.errorMessage = checkNotNull(errorMessage);
	}

	@Override
	public ClusterDescriptor<Void> createClusterDescriptor(Configuration configuration) throws ClusterDeploymentException {
		throw new ClusterDeploymentException(errorMessage);
	}

	@Nullable
	@Override
	public Void getClusterId(Configuration configuration) throws ClusterDeploymentException {
		throw new ClusterDeploymentException(errorMessage);
	}

	@Override
	public ClusterSpecification getClusterSpecification(Configuration configuration) throws ClusterDeploymentException {
		throw new ClusterDeploymentException(errorMessage);
	}
}
