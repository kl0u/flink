/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.IntegerTypeInfo;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.streaming.api.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.transformations.SourceTransformation;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Javadoc.
 */
public class StreamGraphGeneratorTranslatorTest {

	@Test
	public void testBoundedDetection() {
		final SourceTransformation<Integer> bounded =
				getSourceTransformation("Bounded Source", Boundedness.BOUNDED);
		assertEquals(Boundedness.BOUNDED, bounded.getBoundedness());

		final StreamGraph graph = generateStreamGraph(RuntimeExecutionMode.AUTOMATIC,
				bounded);
		assertEquals(GlobalDataExchangeMode.POINTWISE_EDGES_PIPELINED, graph.getGlobalDataExchangeMode());
		assertEquals(ScheduleMode.LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST, graph.getScheduleMode());
		assertFalse(graph.isAllVerticesInSameSlotSharingGroupByDefault());
	}

	private StreamGraph generateStreamGraph(
			final RuntimeExecutionMode initMode,
			final Transformation<?>... transformations) {

		final List<Transformation<?>> registeredTransformations = new ArrayList<>();
		Collections.addAll(registeredTransformations, transformations);

		StreamGraphGenerator streamGraphGenerator =
				new StreamGraphGenerator(
						registeredTransformations,
						new ExecutionConfig(),
						new CheckpointConfig());
		streamGraphGenerator.setRuntimeExecutionMode(initMode);
		return streamGraphGenerator.generate();
	}

	private SourceTransformation<Integer> getSourceTransformation(
			final String name,
			final Boundedness boundedness) {
		return new SourceTransformation<>(
				name,
				new SourceOperatorFactory<>(new MockSource(boundedness, 100), WatermarkStrategy.noWatermarks()),
				IntegerTypeInfo.of(Integer.class),
				1);
	}
}
