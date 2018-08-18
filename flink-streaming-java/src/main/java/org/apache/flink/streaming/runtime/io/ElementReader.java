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

import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Javadoc.
 */
public class ElementReader {

	private final GeneralValveOutputHandler.OperatorProxy operatorProxy;

	private final RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDeserializers;

	private final DeserializationDelegate<StreamElement> deserializationDelegate;

	private final StatusWatermarkValve statusWatermarkValve;

	private final int channelIdxOffset;

	private final int numInputChannels;

	// ------------------------- Runtime Fields -------------------------------

	private int currentChannel;

	@Nullable
	private RecordDeserializer<DeserializationDelegate<StreamElement>> currentRecordDeserializer;

	public ElementReader(
			final GeneralValveOutputHandler.OperatorProxy operatorProxy,
			final RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDeserializers,
			final DeserializationDelegate<StreamElement> deserializationDelegate,
			final StatusWatermarkValve.ValveOutputHandler outputValveHandler,
			final int numInputChannels,
			final int channelIdxOffset) {

		this.operatorProxy = Preconditions.checkNotNull(operatorProxy);
		this.recordDeserializers = Preconditions.checkNotNull(recordDeserializers);
		this.statusWatermarkValve = new StatusWatermarkValve(numInputChannels, Preconditions.checkNotNull(outputValveHandler));
		this.numInputChannels = numInputChannels;
		this.channelIdxOffset = channelIdxOffset;
		this.deserializationDelegate = Preconditions.checkNotNull(deserializationDelegate);

		this.currentRecordDeserializer = null;
		this.currentChannel = -1;
	}

	public int getNumInputChannels() {
		return numInputChannels;
	}

	public void processWatermark(final Watermark watermark) {
		statusWatermarkValve.inputWatermark(watermark, currentChannel);
	}

	public void processLatencyMarker(LatencyMarker marker) throws Exception {
		operatorProxy.processLatencyMarker(marker);
	}

	public void processElement(StreamRecord record) throws Exception {
		operatorProxy.processElement(record);
	}

	public void processStreamStatus(final StreamStatus status) {
		statusWatermarkValve.inputStreamStatus(status, currentChannel);
	}

	public boolean isReadyToReadElement() {
		return currentRecordDeserializer != null;
	}

	public void prepareToReadElement(final BufferOrEvent buffer) throws IOException {
		currentChannel = buffer.getChannelIndex() - channelIdxOffset;
		currentRecordDeserializer = recordDeserializers[currentChannel];
		currentRecordDeserializer.setNextBuffer(buffer.getBuffer());
	}

	@Nullable
	public StreamElement readElement() throws IOException {
		final RecordDeserializer.DeserializationResult result =
				currentRecordDeserializer.getNextRecord(deserializationDelegate);

		if (result.isBufferConsumed()) {
			currentRecordDeserializer.getCurrentBuffer().recycleBuffer();
			currentRecordDeserializer = null;
		}

		return result.isFullRecord() ? deserializationDelegate.getInstance() : null;
	}

	public void cleanup() {
		// clear the buffers first. this part should not ever fail
		for (RecordDeserializer<?> deserializer : recordDeserializers) {
			Buffer buffer = deserializer.getCurrentBuffer();
			if (buffer != null && !buffer.isRecycled()) {
				buffer.recycleBuffer();
			}
			deserializer.clear();
		}
	}
}
