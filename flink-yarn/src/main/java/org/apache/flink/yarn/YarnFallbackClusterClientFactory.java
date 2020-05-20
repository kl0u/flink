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

package org.apache.flink.yarn;

import org.apache.flink.annotation.Internal;
import org.apache.flink.client.deployment.BaseFallbackClusterClientFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link org.apache.flink.client.deployment.FallbackClusterClientFactory} for the
 * {@link YarnClusterClientFactory}.
 */
@Internal
public class YarnFallbackClusterClientFactory extends BaseFallbackClusterClientFactory {

	private static final String ERROR_MESSAGE = "A yarn-cluster can only be used with exported HADOOP_CLASSPATH." +
			"Please refer to the \"Deployment and Operations\" page of Flink's documentation.";

	public YarnFallbackClusterClientFactory() {
		super(ERROR_MESSAGE);
	}

	@Override
	public boolean isCompatibleWith(Configuration configuration) {
		checkNotNull(configuration);
		final String deploymentTarget = configuration.getString(DeploymentOptions.TARGET);
		return YarnDeploymentTarget.isValidYarnTarget(deploymentTarget);
	}
}
