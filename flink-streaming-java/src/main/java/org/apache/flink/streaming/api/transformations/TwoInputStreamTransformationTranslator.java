/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.Translator;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc.
 */
@Internal
public class TwoInputStreamTransformationTranslator<IN1, IN2, OUT> implements Translator<OUT, TwoInputTransformation<IN1, IN2, OUT>> {

	@Override
	public Collection<Integer> translate(
			final TwoInputTransformation<IN1, IN2, OUT> transformation,
			final StreamGraph streamGraph,
			final Context translationContext) {
		checkNotNull(transformation);
		checkNotNull(streamGraph);
		checkNotNull(translationContext);

		final int transformationId = transformation.getId();
		final ExecutionConfig executionConfig = translationContext.getExecutionConfig();
		final String slotSharingGroup = translationContext.getSlotSharingGroup();

		streamGraph.addCoOperator(
				transformationId,
				slotSharingGroup,
				transformation.getCoLocationGroupKey(),
				transformation.getOperatorFactory(),
				transformation.getInputType1(),
				transformation.getInputType2(),
				transformation.getOutputType(),
				transformation.getName());

		if (transformation.getStateKeySelector1() != null || transformation.getStateKeySelector2() != null) {
			TypeSerializer<?> keySerializer = transformation.getStateKeyType().createSerializer(executionConfig);
			streamGraph.setTwoInputStateKey(
					transformationId,
					transformation.getStateKeySelector1(),
					transformation.getStateKeySelector2(),
					keySerializer);
		}

		int parallelism = transformation.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT
				? transformation.getParallelism()
				: executionConfig.getParallelism();

		streamGraph.setParallelism(transformationId, parallelism);
		streamGraph.setMaxParallelism(transformationId, transformation.getMaxParallelism());

		final List<Collection<Integer>> allInputIds = translationContext.getParentNodeIdsByParent();
		for (int i = 0; i < allInputIds.size(); i++) {
			final Collection<Integer> inputIds = allInputIds.get(i);
			for (Integer inputId: inputIds) {
				streamGraph.addEdge(inputId, transformationId, i + 1);
			}
		}
		return Collections.singleton(transformationId);
	}
}
