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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.SideInputProcessFunction;
import org.apache.flink.streaming.api.operators.MultiInputStreamOperator;
import org.apache.flink.streaming.api.transformations.MultiInputTransformation;
import org.apache.flink.util.InputTag;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Javadoc.
 */
public class SideInputStream<T, O> {

	private final StreamExecutionEnvironment environment;

	private final DataStream<T> mainStream;

	private final Map<InputTag, SideInputInfo<?, ?, O>> inputStreamInfo = new HashMap<>();

	public SideInputStream(final DataStream<T> mainStream) {
		this.environment = Preconditions.checkNotNull(mainStream.getExecutionEnvironment());
		this.mainStream = Preconditions.checkNotNull(mainStream);
	}

	public StreamExecutionEnvironment getExecutionEnvironment() {
		return environment;
	}

	@PublicEvolving
	public <I> SideInputStream<T, O> withSideInput(
			final InputTag inputTag,
			final DataStream<I> sideInput,
			final SideInputProcessFunction<I, O> processFunction) {

		Preconditions.checkState(
				!inputStreamInfo.containsKey(inputTag),
				"Duplicate side input tag: " + inputTag);

		inputStreamInfo.put(inputTag, new SideInputInfo<>(sideInput, clean(processFunction)));
		return this;
	}

	@PublicEvolving
	public SingleOutputStreamOperator<O> process(
			final SideInputProcessFunction<T, O> processFunction,
			final TypeInformation<O> outputType) {

		// put the main input in the list of inputs
		withSideInput(InputTag.MAIN_INPUT_TAG, mainStream, processFunction);

		Preconditions.checkState(
				inputsAreCompatible(),
				"Inputs can be keyed or non-keyed, but the keyed one should have the same type of key."
		);

		final Map<InputTag, SideInputProcessFunction<?, O>> sideInputFunctions =
				getCleanProcessFunctionsPerSideInput();

		final MultiInputStreamOperator<O> operator = new MultiInputStreamOperator<>(sideInputFunctions);
		return transform(getOperationName(), outputType, operator);
	}

	// TODO: 9/1/18 write a test for this
	private boolean inputsAreCompatible() {
		TypeInformation<?> seenKeyTypeInfo = null;
		for (SideInputInfo<?, ?, ?> info : inputStreamInfo.values()) {
			final TypeInformation<?> currentKeyType = info.getKeyTypeInfo();
			if (seenKeyTypeInfo == null) {
				seenKeyTypeInfo = currentKeyType;
			} else if (currentKeyType != null && !Objects.equals(currentKeyType, seenKeyTypeInfo)) {
				return false;
			}
		}
		return true;
	}

	private SingleOutputStreamOperator<O> transform(
			final String operatorName,
			final TypeInformation<O> outTypeInfo,
			final MultiInputStreamOperator<O> operator) {

		// read the output type of the input Transform to coax out errors about MissingTypeInfo
		mainStream.getTransformation().getOutputType();

		final MultiInputTransformation<O> resultTransform =
				new MultiInputTransformation<>(
						operatorName,
						operator,
						inputStreamInfo,
						outTypeInfo,
						environment.getParallelism()
				);

		final SingleOutputStreamOperator<O> resultStream =
				new SingleOutputStreamOperator<>(environment, resultTransform);

		environment.addOperator(resultTransform);

		return resultStream;
	}

	private String getOperationName() {
		return inputStreamInfo.size() + "-Input-Processor";
	}

	private Map<InputTag, SideInputProcessFunction<?, O>> getCleanProcessFunctionsPerSideInput() {
		final Map<InputTag, SideInputProcessFunction<?, O>> cleanFunctions = new HashMap<>(inputStreamInfo.size());
		for (Map.Entry<InputTag, SideInputInfo<?, ?, O>> entry : inputStreamInfo.entrySet()) {
			cleanFunctions.put(entry.getKey(), entry.getValue().getFunction());
		}
		return cleanFunctions;
	}

	private <F> F clean(final F f) {
		return getExecutionEnvironment().clean(f);
	}
}
