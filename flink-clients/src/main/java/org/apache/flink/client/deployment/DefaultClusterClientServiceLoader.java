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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A service provider for {@link ClusterClientFactory cluster client factories}.
 */
public class DefaultClusterClientServiceLoader implements ClusterClientServiceLoader {

	private static final Logger LOG = LoggerFactory.getLogger(DefaultClusterClientServiceLoader.class);

	private static final ServiceLoader<ClusterClientFactory> defaultLoader =
			ServiceLoader.load(ClusterClientFactory.class);

	private static final ServiceLoader<FallbackClusterClientFactory> fallbackLoader =
			ServiceLoader.load(FallbackClusterClientFactory.class);

	@Override
	public <ClusterID> ClusterClientFactory<ClusterID> getClusterClientFactory(final Configuration configuration) {
		checkNotNull(configuration);

		final List<ClusterClientFactory> compatibleFactories = loadCompatibleFactories(configuration);
		if (compatibleFactories.size() > 1) {
			throw new IllegalStateException(
					"Multiple compatible client factories found for:\n" +
							configurationToString(configuration) + ".");
		}

		if (!compatibleFactories.isEmpty()) {
			return (ClusterClientFactory<ClusterID>) compatibleFactories.get(0);
		}

		final List<FallbackClusterClientFactory> fallbackFactories = loadFallbackFactories(configuration);
		if (!fallbackFactories.isEmpty()) {
			return (ClusterClientFactory) fallbackFactories.get(0);
		}

		throw new IllegalArgumentException(
				"No compatible ClusterClientFactory found. The provided configuration was:\n" +
						configurationToString(configuration));
	}

	private List<ClusterClientFactory> loadCompatibleFactories(Configuration configuration) {
		return loadAllCompatibleFactories(defaultLoader, configuration);
	}

	private List<FallbackClusterClientFactory> loadFallbackFactories(Configuration configuration) {
		return loadAllCompatibleFactories(fallbackLoader, configuration);
	}

	private <T extends  ClusterClientFactory> List<T> loadAllCompatibleFactories(
			final ServiceLoader<T> serviceLoader,
			final Configuration configuration) {
		final List<T> compatibleFactories = new ArrayList<>();
		final Iterator<T> factories = serviceLoader.iterator();
		while (factories.hasNext()) {
			try {
				final T factory = factories.next();
				if (factory != null && factory.isCompatibleWith(configuration)) {
					compatibleFactories.add(factory);
				}
			} catch (ServiceConfigurationError e) {
				if (ExceptionUtils.findThrowable(e, NoClassDefFoundError.class).isPresent()) {
					LOG.info("Could not load factory due to missing dependencies.");
				} else {
					throw e;
				}
			}
		}
		return compatibleFactories;
	}

	private static String configurationToString(final Configuration configuration) {
		return configuration.toMap().entrySet().stream()
				.map(e -> e.getKey() + "=" + e.getValue())
				.collect(Collectors.joining("\n"));
	}
}
