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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.streaming.api.operators.MultiInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.GeneralValveOutputHandler;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.InputTag;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

/**
 * Javadoc.
 */
public class InputConfig {

	private final TypeInformation<?> inputTypeInfo;

	@Nullable
	private final KeySelector<?, ?> keySelector;

	private final Collection<InputGate> inputGates;

	private final WatermarkGauge watermarkGauge;

	InputConfig(
			final TypeInformation<?> inputTypeInfo,
			@Nullable final KeySelector<?, ?> keySelector
	) {
		this.inputTypeInfo = Preconditions.checkNotNull(inputTypeInfo);
		this.keySelector = keySelector;

		this.inputGates = new ArrayList<>();
		this.watermarkGauge = new WatermarkGauge();
	}

	void addInputGate(final InputGate gate) {
		inputGates.add(gate);
	}

	public Collection<InputGate> getInputGates() {
		return inputGates;
	}

	public WatermarkGauge getWatermarkGauge() {
		return watermarkGauge;
	}

	public TypeInformation<?> getInputTypeInfo() {
		return inputTypeInfo;
	}

	public <OUT, OP extends StreamOperator<OUT>> GeneralValveOutputHandler.OperatorProxy createOperatorProxy(InputTag inputTag, OP operator) {

		if (operator instanceof OneInputStreamOperator) {
			final OneInputStreamOperator op = (OneInputStreamOperator) operator;
			if (Objects.equals(inputTag, InputTag.MAIN_INPUT_TAG)) {
				return new Proxy(op, keySelector);
			} else {
				throw new RuntimeException("Unknown Input \"" + inputTag + "\" in OneInputStreamOperator.");
			}
		}

		if (operator instanceof TwoInputStreamOperator) {
			final TwoInputStreamOperator op = (TwoInputStreamOperator) operator;
			if (Objects.equals(inputTag, InputTag.MAIN_INPUT_TAG)) {
				return new FirstOperatorProxy(op, keySelector);
			} else if (Objects.equals(inputTag, InputTag.LEGACY_SECOND_INPUT_TAG)) {
				return new SecondOperatorProxy(op, keySelector);
			} else {
				throw new RuntimeException("Unknown Input \"" + inputTag + "\" in TwoInputStreamOperator.");
			}
		}

		if (operator instanceof MultiInputStreamOperator) {
			final MultiInputStreamOperator<OUT> op = (MultiInputStreamOperator) operator;
			return new SingleInputOperatorProxy<>(op, inputTag, keySelector);
		}

		throw new RuntimeException("Unknown Operator Type \"" + operator.getClass().getName() + "\".");
	}

	// ---------------------------------- Methods for One and TwoInputStreamOperators ----------------------------------

	/**
	 * Javadoc.
	 */
	private static class Proxy implements GeneralValveOutputHandler.OperatorProxy {

		private final OneInputStreamOperator<?, ?> operator;

		private final KeySelector<?, ?> keySelector;

		Proxy(OneInputStreamOperator<?, ?> operator, @Nullable KeySelector<?, ?> keySelector) {
			this.operator = Preconditions.checkNotNull(operator);
			this.keySelector = keySelector;
		}

		@Override
		public void processLatencyMarker(LatencyMarker marker) throws Exception {
			operator.processLatencyMarker(marker);
		}

		@Override
		public void processElement(StreamRecord record) throws Exception {
			operator.setKeyContextElement(record, keySelector);
			operator.processElement(record);
		}

		@Override
		public void processWatermark(Watermark mark) throws Exception {
			operator.processWatermark(mark);
		}
	}

	/**
	 * Javadoc.
	 */
	private static class FirstOperatorProxy implements GeneralValveOutputHandler.OperatorProxy {

		private final TwoInputStreamOperator<?, ?, ?> operator;

		private final KeySelector<?, ?> keySelector;

		FirstOperatorProxy(TwoInputStreamOperator<?, ?, ?> operator, @Nullable KeySelector<?, ?> keySelector) {
			this.operator = Preconditions.checkNotNull(operator);
			this.keySelector = keySelector;
		}

		@Override
		public void processLatencyMarker(LatencyMarker marker) throws Exception {
			operator.processLatencyMarker1(marker);
		}

		@Override
		public void processElement(StreamRecord record) throws Exception {
			operator.setKeyContextElement(record, keySelector);
			operator.processElement1(record);
		}

		@Override
		public void processWatermark(Watermark mark) throws Exception {
			operator.processWatermark1(mark);
		}
	}

	/**
	 * Javadoc.
	 */
	private static class SecondOperatorProxy implements GeneralValveOutputHandler.OperatorProxy {

		private final TwoInputStreamOperator<?, ?, ?> operator;

		private final KeySelector<?, ?> keySelector;

		SecondOperatorProxy(TwoInputStreamOperator<?, ?, ?> operator, @Nullable KeySelector<?, ?> keySelector) {
			this.operator = Preconditions.checkNotNull(operator);
			this.keySelector = keySelector;
		}

		@Override
		public void processLatencyMarker(LatencyMarker marker) throws Exception {
			operator.processLatencyMarker2(marker);
		}

		@Override
		public void processElement(StreamRecord record) throws Exception {
			operator.setKeyContextElement(record, keySelector);
			operator.processElement2(record);
		}

		@Override
		public void processWatermark(Watermark mark) throws Exception {
			operator.processWatermark2(mark);
		}
	}
}
