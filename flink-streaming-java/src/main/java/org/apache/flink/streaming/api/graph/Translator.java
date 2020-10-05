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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.dag.Transformation;

import java.util.Collection;
import java.util.List;

/**
 * Javadoc.
 */
public interface Translator<O, T extends Transformation<O>> {

	Collection<Integer> translate(
			final T transformation,
			final StreamGraph streamGraph,
			final Context translationContext);

	/**
	 * Javadoc.
	 */
	interface Context {

		boolean translateForBatch();

		ExecutionConfig getExecutionConfig();

		List<Collection<Integer>> getParentNodeIdsByParent();

		String getSlotSharingGroup();
	}
}
