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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.streaming.api.functions.NonKeyedSideInputProcessFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

/**
 * Javadoc.
 */
public class NonKeyedOneInputOperator<I, O> extends AbstractOneInputOperator<I, O, NonKeyedSideInputProcessFunction<I, O>> {

	private final ContextImpl context;

	public NonKeyedOneInputOperator(
			final AbstractStreamOperator<O> operator,
			final NonKeyedSideInputProcessFunction<I, O> function) {
		super(operator, function);
		this.context = new ContextImpl(this);
	}

	@Override
	public void processElement(StreamRecord<I> element) throws Exception {
		getCollector().setTimestamp(element);
		context.setElement(element);
		getFunction().processElement(element.getValue(), context, getCollector());
		context.setElement(null);
	}

	private class ContextImpl implements NonKeyedSideInputProcessFunction.Context {

		private final NonKeyedOneInputOperator<I, O> operator;

		private final ProcessingTimeService processingTimeService;

		private final Output<StreamRecord<O>> operatorOutput;

		@Nullable
		private StreamRecord<I> element;

		ContextImpl(final NonKeyedOneInputOperator<I, O> operator) {
			this.operator = Preconditions.checkNotNull(operator);
			this.operatorOutput = operator.getOutput();
			this.processingTimeService = operator.getOperator().getProcessingTimeService();
		}

		public void setElement(@Nullable StreamRecord<I> record) {
			this.element = record;
		}

		@Nullable
		@Override
		public Long timestamp() {
			Preconditions.checkState(element != null);
			return element.hasTimestamp() ? element.getTimestamp() : null;
		}

		@Override
		public <X> void output(OutputTag<X> outputTag, X value) {
			Preconditions.checkArgument(outputTag != null, "OutputTag must not be null.");
			operatorOutput.collect(outputTag, new StreamRecord<>(value, element.getTimestamp()));
		}

		@Override
		public long currentProcessingTime() {
			return processingTimeService.getCurrentProcessingTime();
		}

		@Override
		public long currentWatermark() {
			return operator.getCurrentWatermark();
		}
	}
}
