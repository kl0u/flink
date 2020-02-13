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
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc.
 */
@Internal
public class EmbeddedClusterClientProvider implements ClusterClientProvider<String> {

	private final Configuration configuration;

	private final CompletableFuture<LeaderGatewayRetriever<DispatcherGateway>> dispatcherGatewayRetrieverFuture;

	public EmbeddedClusterClientProvider(
			final Configuration configuration,
			final CompletableFuture<LeaderGatewayRetriever<DispatcherGateway>> dispatcherGatewayRetrieverFuture) {
		this.configuration = checkNotNull(configuration);
		this.dispatcherGatewayRetrieverFuture = checkNotNull(dispatcherGatewayRetrieverFuture);
	}

	@Override
	public ClusterClient<String> getClusterClient() {
		return new EmbeddedClusterClient(configuration, dispatcherGatewayRetrieverFuture);
	}
}
