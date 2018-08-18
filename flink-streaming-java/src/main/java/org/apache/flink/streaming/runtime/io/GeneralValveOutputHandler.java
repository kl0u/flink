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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.util.Preconditions;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc.
 */
public class GeneralValveOutputHandler implements StatusWatermarkValve.ValveOutputHandler {

	private final OperatorProxy callWrapper;
	private final StreamStatusMaintainer streamStatusMaintainer;
	private final WatermarkGauge watermarkGauge;
	private final Object lock;

	private StatusWatermarkValve.ValveOutputHandler[] otherInputHandlers;

	private StreamStatus status;

	GeneralValveOutputHandler(
			final OperatorProxy callWrapper,
			final StreamStatusMaintainer streamStatusMaintainer,
			final WatermarkGauge watermarkGauge,
			final Object lock) {
		this.callWrapper = checkNotNull(callWrapper);
		this.streamStatusMaintainer = checkNotNull(streamStatusMaintainer);
		this.watermarkGauge = checkNotNull(watermarkGauge);
		this.lock = checkNotNull(lock);

		this.status = StreamStatus.ACTIVE;
	}

	public void addLocalInputHandlers(StatusWatermarkValve.ValveOutputHandler... inputHandlers) {
		Preconditions.checkState(otherInputHandlers == null);
		this.otherInputHandlers = checkNotNull(inputHandlers);
	}

	@Override
	public StreamStatus getStatus() {
		return status;
	}

	@Override
	public void handleWatermark(Watermark watermark) {
		try {
			synchronized (lock) {
				watermarkGauge.setCurrentWatermark(watermark.getTimestamp());
				callWrapper.processWatermark(watermark);
			}
		} catch (Exception e) {
			throw new RuntimeException("Exception occurred while processing valve output watermark: ", e);
		}
	}

	@Override
	public void handleStreamStatus(StreamStatus streamStatus) {
		try {
			synchronized (lock) {
				status = streamStatus;

				if (!Objects.equals(streamStatus, streamStatusMaintainer.getStreamStatus())) {
					if (streamStatus.isActive()) {
						streamStatusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);
						return;
					}

					if (otherInputHandlers != null) {
						for (StatusWatermarkValve.ValveOutputHandler handler : otherInputHandlers) {
							if (Objects.equals(handler.getStatus(), StreamStatus.ACTIVE)) {
								streamStatusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);
								return;
							}
						}
					}

					streamStatusMaintainer.toggleStreamStatus(StreamStatus.IDLE);
				}
			}
		} catch (Exception e) {
			throw new RuntimeException("Exception occurred while processing valve output stream status: ", e);
		}
	}

	/**
	 * Javadoc.
	 */
	public interface OperatorProxy {

		void processLatencyMarker(LatencyMarker marker) throws Exception;

		void processElement(StreamRecord record) throws Exception;

		void processWatermark(Watermark mark) throws Exception;
	}
}
