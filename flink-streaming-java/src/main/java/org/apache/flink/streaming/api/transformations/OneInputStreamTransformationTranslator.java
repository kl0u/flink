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

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc.
 */
@Internal
public class OneInputStreamTransformationTranslator<IN, OUT> implements Translator<OUT, OneInputTransformation<IN, OUT>> {

	@Override
	public Collection<Integer> translate(
			final OneInputTransformation<IN, OUT> transformation,
			final StreamGraph streamGraph,
			final Context translationContext) {
		checkNotNull(transformation);
		checkNotNull(translationContext);

		final int transformationId = transformation.getId();
		final ExecutionConfig executionConfig = translationContext.getExecutionConfig();
		final String slotSharingGroup = translationContext.getSlotSharingGroup();

		streamGraph.addOperator(
				transformationId,
				slotSharingGroup,
				transformation.getCoLocationGroupKey(),
				transformation.getOperatorFactory(),
				transformation.getInputType(),
				transformation.getOutputType(),
				transformation.getName());

		if (transformation.getStateKeySelector() != null) {
			final TypeSerializer<?> keySerializer = transformation.getStateKeyType().createSerializer(executionConfig);
			streamGraph.setOneInputStateKey(transformationId, transformation.getStateKeySelector(), keySerializer);
		}

		int parallelism = transformation.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT
				? transformation.getParallelism()
				: executionConfig.getParallelism();

		streamGraph.setParallelism(transformationId, parallelism);
		streamGraph.setMaxParallelism(transformationId, transformation.getMaxParallelism());

		for (Integer inputId: translationContext.getParentNodeIds()) {
			streamGraph.addEdge(inputId, transformationId, 0);
		}

		return Collections.singleton(transformationId);
	}
}
