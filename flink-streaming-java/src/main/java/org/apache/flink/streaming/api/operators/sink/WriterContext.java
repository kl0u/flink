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

import org.apache.flink.api.connector.sink.Writer;
import org.apache.flink.api.connector.sink.WriterOutput;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc.
 */
class WriterContext<Committable> implements Writer.Context<Committable> {

	private final ProcessingTimeService timeService;

	private final WriterOutput<Committable> output;

	@Nullable
	private Long elementTimestamp;

	private long currentWatermark;

	private long currentProcessingTime;

	WriterContext(
			final ProcessingTimeService timeService,
			final WriterOutput<Committable> output) {
		this.timeService = checkNotNull(timeService);
		this.output = checkNotNull(output);

		this.elementTimestamp = null;
		this.currentWatermark = Long.MIN_VALUE;
		this.currentProcessingTime = Long.MIN_VALUE;
	}

	void update(@Nullable Long elementTimestamp, long watermark) {
		this.elementTimestamp = elementTimestamp;
		this.currentWatermark = watermark;
		this.currentProcessingTime = timeService.getCurrentProcessingTime();
	}

	@Override
	public long currentProcessingTime() {
		return currentProcessingTime;
	}

	@Override
	public long currentWatermark() {
		return currentWatermark;
	}

	@Override
	@Nullable
	public Long timestamp() {
		return elementTimestamp;
	}

	@Override
	public void registerCallback(Writer.TimerCallback<Committable> callback, long delay) {
		final long currentTime = timeService.getCurrentProcessingTime();
		this.timeService.registerTimer(
				currentTime + delay,
				new Callback<>(callback, this, output));
	}

	private static final class Callback<Committable> implements ProcessingTimeCallback {

		private final Writer.TimerCallback<Committable> callback;

		private final WriterContext<Committable> context;

		private final WriterOutput<Committable> output;

		public Callback(
				Writer.TimerCallback<Committable> callback,
				WriterContext<Committable> context,
				WriterOutput<Committable> output) {
			this.callback = callback;
			this.context = context;
			this.output = output;
		}

		@Override
		public void onProcessingTime(long timestamp) throws Exception {
			callback.apply(context, output);
		}
	}
}
