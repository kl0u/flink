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

/**
 * This is a marker interface to be used when a {@link ClusterClientFactory} wants to register
 * a fallback factory that will be loaded when the original one fails to do so. Implementing such
 * a fallback {@link ClusterClientFactory} can allow for more descriptive/less generic error messages,
 * when a factory fails to be loaded.This is the case, for example, with the {@code YarnClusterClientFactory}
 * when the {@code HADOOP_CLASSPATH} has not been set.
 *
 * <p>For the implementations to be correct, the subclasses have to have the same
 * {@link #isCompatibleWith(Configuration)} implementation with the original {@link ClusterClientFactory} and
 * whenever possible, the original factory can simply extend the fallback one so that the two methods match.
 * In addition, the implementor has to make sure that the fallback can be loaded in any case, even when
 * the original fails.
 *
 * <p>For an implementation of the {@code FallbackClusterClientFactory} that throws a
 * {@link ClusterDeploymentException} with a specific message please refer to the {@link BaseFallbackClusterClientFactory}.
 * And for a full example, please refer to the {@code YarnClusterClientFactory} and its fallback
 * {@code YarnFallbackClusterClientFactory}.
 */
@Internal
public interface FallbackClusterClientFactory<ClusterID> extends ClusterClientFactory<ClusterID> {

}
