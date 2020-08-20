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

package org.apache.flink.streaming.api.operators.sink;

import org.apache.flink.api.connector.sink.InitContext;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.Writer;
import org.apache.flink.api.connector.sink.WriterOutput;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc.
 */
public class BatchSinkOperator<IN, Committable, WriterStateT>
		extends AbstractStreamOperator<Object>
		implements OneInputStreamOperator<IN, Object> {

	private final Sink<IN, Committable, WriterStateT> sink;

	private final OperatorEventGateway operatorEventGateway;

	private Writer<IN, Committable, WriterStateT> writer;

	private WriterContext<Committable> writerContext;

	private WriterOutput<Committable> writerOutput;

	private long lastWatermark;

	public BatchSinkOperator(
			final Sink<IN, Committable, WriterStateT> sink,
			final OperatorEventGateway operatorEventGateway,
			final ProcessingTimeService timeService) {
		this.sink = checkNotNull(sink);
		this.operatorEventGateway = checkNotNull(operatorEventGateway);
		this.processingTimeService = checkNotNull(timeService);
	}

	@Override
	public void open() throws Exception {
		super.open();

		writer = sink.createWriter(createInitContext());

		writerOutput = new OutputToCoordinator<>(
				operatorEventGateway,
				sink.getCommittableSerializer());

		writerContext = new WriterContext<>(
				processingTimeService,
				writerOutput);

		lastWatermark = Long.MIN_VALUE;
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		super.processWatermark(mark);
		lastWatermark = mark.getTimestamp();
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		writerContext.update(element.getTimestamp(), lastWatermark);
		writer.write(element.getValue(), writerContext, writerOutput);
	}

	@Override
	public void close() throws Exception {
		super.close();

		// TODO: 19.08.20 should we block here
		//  until the coordinator acks reception of messages?
		//  We can send a final MSG to the coordinator and wait for ack of that
		writer.flush(writerOutput);
	}

	private InitContext createInitContext() {
		return new InitContext() {
			@Override
			public int getSubtaskId() {
				return getRuntimeContext().getIndexOfThisSubtask();
			}

			@Override
			public int getAttemptID() {
				return getRuntimeContext().getAttemptNumber();
			}

			@Override
			public MetricGroup metricGroup() {
				return getMetricGroup();
			}
		};
	}
}
