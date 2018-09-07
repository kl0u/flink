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

import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedSideInputProcessFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc.
 */
public class KeyedOneInputOperator<I, K, O> extends AbstractOneInputOperator<I, O, KeyedSideInputProcessFunction<I, K, O>> {

	private ContextImpl context;

	private OnTimerContextImpl timerContext;

	public KeyedOneInputOperator(
			final AbstractStreamOperator<O> operator,
			final KeyedSideInputProcessFunction<I, K, O> function) {
		super(operator, function);
	}

	@Override
	public void open(TimestampedCollector<O> collector, TimerService timerService) throws Exception {
		super.open(collector, timerService);
		this.context = new ContextImpl(this, getTimerService());
		this.timerContext = new OnTimerContextImpl(this, getTimerService());
	}

	@Override
	public void processElement(final StreamRecord<I> element) throws Exception {
		getCollector().setTimestamp(element);
		context.setElement(element);
		getFunction().processElement(element.getValue(), context, getCollector());
		context.setElement(null);
	}

	public void invokeUserFunction(
			final TimeDomain timeDomain,
			final InternalTimer<K, VoidNamespace> timer) throws Exception {
		timerContext.setTimerAndDomain(timer, timeDomain);
		getFunction().onTimer(timer.getTimestamp(), timerContext, getCollector());
		timerContext.setTimerAndDomain(null, null);
	}

	private class ContextImpl implements KeyedSideInputProcessFunction.Context<K> {

		private final KeyedOneInputOperator<I, K, O> operator;

		private final TimerService timerService;

		private final Output<StreamRecord<O>> operatorOutput;

		@Nullable
		private StreamRecord<I> element;

		ContextImpl(
				final KeyedOneInputOperator<I, K, O> operator,
				final TimerService timerService
		) {
			this.operator = Preconditions.checkNotNull(operator);
			this.operatorOutput = operator.getOutput();
			this.timerService = Preconditions.checkNotNull(timerService);
		}

		public void setElement(@Nullable StreamRecord<I> record) {
			this.element = record;
		}

		@Override
		@Nullable
		public Long timestamp() {
			Preconditions.checkState(element != null);
			return element.hasTimestamp() ? element.getTimestamp() : null;
		}

		@Override
		public TimerService timerService() {
			return timerService;
		}

		@Override
		public long currentProcessingTime() {
			return timerService.currentProcessingTime();
		}

		@Override
		public long currentWatermark() {
			return timerService.currentWatermark();
		}

		@Override
		@SuppressWarnings("unchecked")
		public K getCurrentKey() {
			return (K) operator.getOperator().getCurrentKey();
		}

		@Override
		public <X> void output(OutputTag<X> outputTag, X value) {
			Preconditions.checkArgument(outputTag != null, "OutputTag must not be null.");
			operatorOutput.collect(outputTag, new StreamRecord<>(value, element.getTimestamp()));
		}
	}

	private class OnTimerContextImpl implements KeyedSideInputProcessFunction.OnTimerContext<K> {

		private final Output<StreamRecord<O>> operatorOutput;

		private final TimerService timerService;

		@Nullable
		private TimeDomain timeDomain;

		@Nullable
		private InternalTimer<K, VoidNamespace> timer;

		OnTimerContextImpl(
				final KeyedOneInputOperator<I, K, O> operator,
				final TimerService timerService
		) {
			this.operatorOutput = Preconditions.checkNotNull(operator).getOutput();
			this.timerService = checkNotNull(timerService);
		}

		public void setTimerAndDomain(
				@Nullable final InternalTimer<K, VoidNamespace> timer,
				@Nullable final TimeDomain timeDomain) {
			this.timer = timer;
			this.timeDomain = timeDomain;
		}

		@Override
		public Long timestamp() {
			Preconditions.checkState(timer != null);
			return timer.getTimestamp();
		}

		@Override
		public TimerService timerService() {
			return timerService;
		}

		@Override
		public long currentProcessingTime() {
			return timerService.currentProcessingTime();
		}

		@Override
		public long currentWatermark() {
			return timerService.currentWatermark();
		}

		@Override
		public <X> void output(OutputTag<X> outputTag, X value) {
			Preconditions.checkArgument(outputTag != null, "OutputTag must not be null.");
			operatorOutput.collect(outputTag, new StreamRecord<>(value, timer.getTimestamp()));
		}

		@Override
		public TimeDomain timeDomain() {
			Preconditions.checkState(timeDomain != null);
			return timeDomain;
		}

		@Override
		public K getCurrentKey() {
			return timer.getKey();
		}
	}
}
