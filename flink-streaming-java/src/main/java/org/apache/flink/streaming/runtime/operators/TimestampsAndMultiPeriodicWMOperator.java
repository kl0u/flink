/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.api.java.functions.SelectiveWatermarkAssigner;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.TimestampAssigner;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * javadoc.
 */
public class TimestampsAndMultiPeriodicWMOperator<IN>
		extends AbstractUdfStreamOperator<IN, TimestampsAndMultiPeriodicWMOperator.MultiWmAssigner<IN>>
		implements OneInputStreamOperator<IN, IN>, ProcessingTimeCallback {

	private static final long serialVersionUID = 1L;

	private transient long watermarkInterval;

	private transient Map<String, Long> currentWatermarks;

	public TimestampsAndMultiPeriodicWMOperator(final List<SelectiveWatermarkAssigner<IN>> assigners) {
		super(Preconditions.checkNotNull(new MultiWmAssigner<>(assigners)));
		setChainingStrategy(ChainingStrategy.ALWAYS);
	}

	@Override
	public void open() throws Exception {
		super.open();

		// for each tag we monitor the
		this.currentWatermarks = new HashMap<>();
		for (String tag: userFunction.getTags()) {
			currentWatermarks.put(tag, Long.MIN_VALUE);
		}

		watermarkInterval = getExecutionConfig().getAutoWatermarkInterval();
		if (watermarkInterval > 0L) {
			long now = getProcessingTimeService().getCurrentProcessingTime();
			getProcessingTimeService().registerTimer(now + watermarkInterval, this);
		}
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		final long newTimestamp = userFunction.extractTimestamp(element.getValue(),
				element.hasTimestamp() ? element.getTimestamp() : Long.MIN_VALUE);
// TODO: 6/7/18 calls can be optimized
		output.collect(element.replace(element.getValue(), newTimestamp, userFunction.getTag(element.getValue())));
	}

	@Override
	public void onProcessingTime(long timestamp) throws Exception {
		// register next timer
		for (Watermark newWatermark: userFunction.getCurrentWatermarks()) {
			if (newWatermark != null && newWatermark.getTimestamp() > currentWatermarks.get(newWatermark.getTag())) {
				currentWatermarks.put(newWatermark.getTag(), newWatermark.getTimestamp());
				output.emitWatermark(newWatermark);
			}
		}

		long now = getProcessingTimeService().getCurrentProcessingTime();
		getProcessingTimeService().registerTimer(now + watermarkInterval, this);
	}

	/**
	 * Override the base implementation to completely ignore watermarks propagated from
	 * upstream (we rely only on the {@link AssignerWithPeriodicWatermarks} to emit
	 * watermarks from here).
	 */
	@Override
	public void processWatermark(Watermark mark) throws Exception {
		// if we receive a Long.MAX_VALUE watermark we forward it since it is used
		// to signal the end of input and to not block watermark progress downstream
		if (mark.getTimestamp() == Long.MAX_VALUE && currentWatermarks.get(mark.getTag()) != Long.MAX_VALUE) {
			currentWatermarks.put(mark.getTag(), Long.MAX_VALUE);
			output.emitWatermark(mark);
		}
	}

	@Override
	public void close() throws Exception {
		super.close();

		// emit a final watermark
		// TODO: 6/7/18 here should emit MAX_VALUE right? when periodic operator
		for (Watermark newWatermark: userFunction.getCurrentWatermarks()) {
			if (newWatermark != null && newWatermark.getTimestamp() > currentWatermarks.get(newWatermark.getTag())) {
				currentWatermarks.put(newWatermark.getTag(), newWatermark.getTimestamp());
				output.emitWatermark(newWatermark);
			}
		}
	}

	/**
	 * Javadoc.
	 */
	public static class MultiWmAssigner<T> implements TimestampAssigner<T> {

		private static final long serialVersionUID = 4998222228683986485L;

		private final Map<String, SelectiveWatermarkAssigner<T>> watermarkAssigners;

		public MultiWmAssigner(List<SelectiveWatermarkAssigner<T>> assigners) {
			this.watermarkAssigners = Collections.unmodifiableMap(
					Preconditions.checkNotNull(loadAssigners(assigners))
			);
		}

		private Map<String, SelectiveWatermarkAssigner<T>> loadAssigners(List<SelectiveWatermarkAssigner<T>> assigners) {
			Map<String, SelectiveWatermarkAssigner<T>> tmp = new HashMap<>(assigners.size());
			for (SelectiveWatermarkAssigner<T> assigner : assigners) {
				tmp.put(assigner.getId(), assigner);
			}
			return tmp;
		}

		public String getTag(T element) {
			for (SelectiveWatermarkAssigner<T> assigner: watermarkAssigners.values()) {
				if (assigner.select(element)) {
					return assigner.getId();
				}
			}
			// TODO: 6/7/18 we should have a default
			throw new FlinkRuntimeException("Uncategorized element " + element);
		}

		public Set<String> getTags() {
			return Collections.unmodifiableSet(watermarkAssigners.keySet());
		}

		// TODO: 6/7/18 can be a set when I fix the equals/hashcode of the Watermark
		public List<Watermark> getCurrentWatermarks() {
			final List<Watermark> watermarks = new ArrayList<>();
			for (SelectiveWatermarkAssigner<T> assigner: watermarkAssigners.values()) {
				Long timestamp = assigner.getCurrentWatermark();
				if (timestamp != null) {
					Watermark wm = new Watermark(timestamp);
					wm.setTag(assigner.getId());
					watermarks.add(wm);
				}
			}
			return watermarks;
		}

		@Override
		public long extractTimestamp(T element, long previousElementTimestamp) {
			for (SelectiveWatermarkAssigner<T> assigner: watermarkAssigners.values()) {
				if (assigner.select(element)) {
					return assigner.extractTimestamp(element, previousElementTimestamp);
				}
			}
			// TODO: 6/7/18 we should have a default
			throw new FlinkRuntimeException("Uncategorized element " + element);
		}
	}
}
