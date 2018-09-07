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
import org.apache.flink.streaming.api.functions.KeyedSideInputProcessFunction;
import org.apache.flink.streaming.api.functions.NonKeyedSideInputProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedMultiInputStreamOperator;
import org.apache.flink.streaming.api.operators.MultiInputStreamOperator;
import org.apache.flink.streaming.api.operators.NonKeyedMultiInputStreamOperator;
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

	private final Map<InputTag, NonKeyedSideInputInfo<?, O>> nonKeyedInputInfo;

	private final Map<InputTag, KeyedSideInputInfo<?, ?, O>> keyedInputInfo;

	public SideInputStream(final DataStream<T> mainStream) {
		this.environment = Preconditions.checkNotNull(mainStream.getExecutionEnvironment());
		this.mainStream = Preconditions.checkNotNull(mainStream);

		this.nonKeyedInputInfo = new HashMap<>();
		this.keyedInputInfo = new HashMap<>();
	}

	public StreamExecutionEnvironment getExecutionEnvironment() {
		return environment;
	}

	@PublicEvolving
	public <I> SideInputStream<T, O> withSideInput(
			final InputTag inputTag,
			final DataStream<I> sideInput,
			final NonKeyedSideInputProcessFunction<I, O> processFunction) {

		checkInputTagUniqueness(inputTag);

		nonKeyedInputInfo.put(inputTag, new NonKeyedSideInputInfo<>(sideInput, clean(processFunction)));
		return this;
	}

	@PublicEvolving
	public <I, K> SideInputStream<T, O> withKeyedSideInput(
			final InputTag inputTag,
			final KeyedStream<I, K> sideInput,
			final KeyedSideInputProcessFunction<I, K, O> processFunction) {

		checkInputTagUniqueness(inputTag);

		keyedInputInfo.put(inputTag, new KeyedSideInputInfo<>(sideInput, clean(processFunction)));
		return this;
	}

	private void checkInputTagUniqueness(final InputTag tag) {
		Preconditions.checkArgument(
				!keyedInputInfo.containsKey(tag) && !nonKeyedInputInfo.containsKey(tag),
				"Duplicate side input tag: " + tag);
	}

	private void checkKeyCompatibility() {
		TypeInformation<?> lastSeenKeyTypeInfo = null;
		for (Map.Entry<InputTag, KeyedSideInputInfo<?, ?, O>> info : keyedInputInfo.entrySet()) {
			if (lastSeenKeyTypeInfo == null) {
				lastSeenKeyTypeInfo = info.getValue().getKeyTypeInfo();
			} else if (!Objects.equals(info.getValue().getKeyTypeInfo(), lastSeenKeyTypeInfo)) {
				throw new IllegalArgumentException(
						"Incompatible Keys Types detected:" +
								" combining keyed with non-keyed inputs is allowed," +
								" BUT all keyed inputs should have the same key type.");
			}
		}
	}

	@PublicEvolving
	public SingleOutputStreamOperator<O> process(
			final NonKeyedSideInputProcessFunction<T, O> processFunction,
			final TypeInformation<O> outputType) {

		// put the main input in the list of inputs
		withSideInput(InputTag.MAIN_INPUT_TAG, mainStream, processFunction);
		checkKeyCompatibility();

		final MultiInputStreamOperator<O> operator = getFunctionsPerKeyedSideInput().isEmpty()
				? new NonKeyedMultiInputStreamOperator<>(getFunctionsPerNonKeyedSideInput())
				: new KeyedMultiInputStreamOperator<>(
						getFunctionsPerNonKeyedSideInput(), getFunctionsPerKeyedSideInput());

		return transform(getOperationName(), outputType, operator);
	}

	@PublicEvolving
	public <K> SingleOutputStreamOperator<O> process(
			final KeyedSideInputProcessFunction<T, K, O> processFunction,
			final TypeInformation<O> outputType) {

		Preconditions.checkState(
				mainStream instanceof KeyedStream,
				"KeyedSideInputProcessFunction is only available to keyed streams.");

		// put the main input in the list of inputs
		withKeyedSideInput(InputTag.MAIN_INPUT_TAG, (KeyedStream) mainStream, processFunction);
		checkKeyCompatibility();

		final KeyedMultiInputStreamOperator<?, O> operator =
				new KeyedMultiInputStreamOperator<>(
						getFunctionsPerNonKeyedSideInput(),
						getFunctionsPerKeyedSideInput());

		return transform(getOperationName(), outputType, operator);
	}

	private Map<InputTag, NonKeyedSideInputProcessFunction<?, O>> getFunctionsPerNonKeyedSideInput() {
		final Map<InputTag, NonKeyedSideInputProcessFunction<?, O>> cleanFunctions = new HashMap<>(nonKeyedInputInfo.size());
		for (Map.Entry<InputTag, NonKeyedSideInputInfo<?, O>> entry : nonKeyedInputInfo.entrySet()) {
			cleanFunctions.put(entry.getKey(), entry.getValue().getFunction());
		}
		return cleanFunctions;
	}

	private <K> Map<InputTag, KeyedSideInputProcessFunction<?, K, O>> getFunctionsPerKeyedSideInput() {
		final Map<InputTag, KeyedSideInputProcessFunction<?, K, O>> cleanFunctions = new HashMap<>(keyedInputInfo.size());
		for (Map.Entry<InputTag, KeyedSideInputInfo<?, ?, O>> entry : keyedInputInfo.entrySet()) {
			cleanFunctions.put(entry.getKey(), (KeyedSideInputProcessFunction<?, K, O>) entry.getValue().getFunction());
		}
		return cleanFunctions;
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
						nonKeyedInputInfo,
						keyedInputInfo,
						outTypeInfo,
						environment.getParallelism()
				);

		final SingleOutputStreamOperator<O> resultStream =
				new SingleOutputStreamOperator<>(environment, resultTransform);

		environment.addOperator(resultTransform);

		return resultStream;
	}

	private String getOperationName() {
		return nonKeyedInputInfo.size() + "-Input-Processor";
	}

	private <F> F clean(final F f) {
		return getExecutionEnvironment().clean(f);
	}
}
