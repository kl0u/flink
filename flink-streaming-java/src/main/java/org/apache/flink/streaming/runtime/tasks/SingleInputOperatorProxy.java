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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.AbstractOneInputOperator;
import org.apache.flink.streaming.api.operators.MultiInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.GeneralValveOutputHandler;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.InputTag;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

/**
 * Javadoc.
 */
public class SingleInputOperatorProxy<OUT> implements GeneralValveOutputHandler.OperatorProxy {

	private final MultiInputStreamOperator<OUT> naryOperator;

	private final InputTag inputTag;

	private AbstractOneInputOperator<?, OUT, ?> operator;

	@Nullable
	private final KeySelector<?, ?> keySelector;

	SingleInputOperatorProxy(
			final MultiInputStreamOperator<OUT> operator,
			final InputTag inputTag,
			@Nullable final KeySelector<?, ?> keySelector) {
		this.naryOperator = Preconditions.checkNotNull(operator);
		this.inputTag = Preconditions.checkNotNull(inputTag);
		this.keySelector = keySelector;

		// TODO: 8/29/18 there was a problem because op.open() is called
		// after task.init(). this is why we do not initiate the operator here

	}

	@Override
	public void processLatencyMarker(LatencyMarker marker) throws Exception {
		if (operator == null) {
			this.operator = naryOperator.getOperatorForInput(inputTag); // TODO: 8/29/18 just for testing
		}
		operator.processLatencyMarker(marker);
	}

	@Override
	public void processElement(StreamRecord record) throws Exception {
		if (operator == null) {
			this.operator = naryOperator.getOperatorForInput(inputTag); // TODO: 8/29/18 just for testing
		}
		// TODO: 9/16/18 this can move to the keyedoneinputoperator
		naryOperator.setKeyContextElement(record, keySelector);
		operator.processElement(record);
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		if (operator == null) {
			this.operator = naryOperator.getOperatorForInput(inputTag); // TODO: 8/29/18 just for testing
		}
		operator.processWatermark(mark);
	}
}
