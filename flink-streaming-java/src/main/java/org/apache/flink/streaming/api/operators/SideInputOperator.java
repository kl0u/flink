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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.streaming.api.functions.SideInputProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

/**
 * Javadoc.
 */
public class SideInputOperator<I, OUT> {

	// TODO: 8/29/18 could also put the keyselector here instead of the input processor

	private final MultiInputStreamOperator<OUT> operator;

	private final SideInputProcessFunction<I, OUT> function;

	private final Output<StreamRecord<OUT>> output;

	private TimestampedCollector<OUT> collector;

	SideInputOperator(
			final MultiInputStreamOperator<OUT> operator,
			final Output<StreamRecord<OUT>> output,
			final SideInputProcessFunction<I, OUT> function
	) {
		this.function = Preconditions.checkNotNull(function);
		this.operator = Preconditions.checkNotNull(operator);
		this.output = Preconditions.checkNotNull(output);
	}

	public void open() throws Exception {
		// TODO: 8/29/18 this is a Flatmap for now
		this.collector = new TimestampedCollector<>(output);
	}

	public StreamingRuntimeContext getRuntimeContext() {
		return operator.getRuntimeContext();
	}

	public void processElement(StreamRecord<I> element) throws Exception {
		collector.setTimestamp(element);
		function.process(element.getValue(), collector);
	}

	public void processWatermark(Watermark mark) throws Exception {
		operator.processWatermark(mark);
	}

	public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
		operator.processLatencyMarker(latencyMarker);
	}

	public void close() throws Exception {
		// TODO: 8/29/18 do nothing now
	}

}
