/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.streamstatus;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc.
 */
public class StatusMultiWatermarkValve {

	// TODO: 6/7/18 it should know about the tags upfront? (NOT NECESSARILY BUT IT SEEMS BETTER)

	// TODO: 6/7/18 IDLENESS SHOULD BE TAGGED PER GROUP/TAG. AND THE CONTEXTS SHOULD BE AWARE OF TAGS, OR THE ASSIGNER OP should.

	/**
	 * Usages of {@code StatusWatermarkValve} should implement a {@code ValveOutputHandler}
	 * to handle watermark and stream status outputs from the valve.
	 */
	public interface ValveOutputHandler {
		void handleWatermark(Watermark watermark);

		void handleStreamStatus(StreamStatus streamStatus);
	}

	private final ValveOutputHandler outputHandler;

	// ------------------------------------------------------------------------
	//	Runtime state for watermark & stream status output determination
	// ------------------------------------------------------------------------

	private int numInputChannels;

	/**
	 * Array of current status of all input channels. Changes as watermarks & stream statuses are
	 * fed into the valve. todo MAYBE ALSO THE STATUS HAS TO BE TAGGED AS WELL
	 */
	private final Map<String, InputChannelStatus[]> taggedChannelStatuses;

	/** todo this should also be tagged The last watermark emitted from the valve. */
	private final Map<String, Long> lastOutputWatermarks;

	/** The last stream status emitted from the valve. */
	private StreamStatus lastOutputStreamStatus;

	/**
	 * Returns a new {@code StatusWatermarkValve}.
	 *
	 * @param numInputChannels the number of input channels that this valve will need to handle
	 * @param outputHandler the customized output handler for the valve
	 */
	public StatusMultiWatermarkValve(int numInputChannels, ValveOutputHandler outputHandler) {
		checkArgument(numInputChannels > 0);
		this.numInputChannels = numInputChannels;

		this.taggedChannelStatuses = new HashMap<>();
		this.lastOutputWatermarks = new HashMap<>();

		this.outputHandler = checkNotNull(outputHandler);

		this.lastOutputStreamStatus = StreamStatus.ACTIVE;
	}

	private InputChannelStatus[] getChannelStatusForTag(String tag) {
		InputChannelStatus[] channelStatus = taggedChannelStatuses.get(tag);
		if (channelStatus == null) {
			channelStatus = new InputChannelStatus[numInputChannels];
			for (int i = 0; i < numInputChannels; i++) {
				channelStatus[i] = new InputChannelStatus();
				channelStatus[i].watermark = Long.MIN_VALUE;
				channelStatus[i].streamStatus = StreamStatus.ACTIVE;
				channelStatus[i].isWatermarkAligned = true;
			}
			lastOutputWatermarks.put(tag, Long.MIN_VALUE);
			taggedChannelStatuses.put(tag, channelStatus);
		}
		return channelStatus;
	}

	/**
	 * Feed a {@link Watermark} into the valve. If the input triggers the valve to output a new Watermark,
	 * {@link StatusWatermarkValve.ValveOutputHandler#handleWatermark(Watermark)} will be called to process the new Watermark.
	 *
	 * @param watermark the watermark to feed to the valve
	 * @param channelIndex the index of the channel that the fed watermark belongs to (index starting from 0)
	 */
	public void inputWatermark(Watermark watermark, int channelIndex) {

		// get the status for the right tag todo the idleness is still not correctly handled
		InputChannelStatus[] channelStatuses = getChannelStatusForTag(watermark.getTag());

		// ignore the input watermark if its input channel, or all input channels are idle (i.e. overall the valve is idle).
		if (lastOutputStreamStatus.isActive() && channelStatuses[channelIndex].streamStatus.isActive()) {
			long watermarkMillis = watermark.getTimestamp();

			// if the input watermark's value is less than the last received watermark for its input channel, ignore it also.
			if (watermarkMillis > channelStatuses[channelIndex].watermark) {
				channelStatuses[channelIndex].watermark = watermarkMillis;

				// previously unaligned input channels are now aligned if its watermark has caught up
				if (!channelStatuses[channelIndex].isWatermarkAligned && watermarkMillis >= lastOutputWatermarks.get(watermark.getTag())) {
					channelStatuses[channelIndex].isWatermarkAligned = true;
				}

				// now, attempt to find a new min watermark across all aligned channels
				findAndOutputNewMinWatermarkAcrossAlignedChannels(watermark.getTag(), channelStatuses);
			}
		}
	}

	/**
	 * Feed a {@link StreamStatus} into the valve. This may trigger the valve to output either a new Stream Status,
	 * for which {@link StatusWatermarkValve.ValveOutputHandler#handleStreamStatus(StreamStatus)} will be called, or a new Watermark,
	 * for which {@link StatusWatermarkValve.ValveOutputHandler#handleWatermark(Watermark)} will be called.
	 *
	 * @param streamStatus the stream status to feed to the valve
	 * @param channelIndex the index of the channel that the fed stream status belongs to (index starting from 0)
	 */
	public void inputStreamStatus(StreamStatus streamStatus, int channelIndex) {

		// todo the idleness is still not correctly handled. Now we may have idle group, not channel

		for (Map.Entry<String, InputChannelStatus[]> entry: taggedChannelStatuses.entrySet()) {

			final String tag = entry.getKey();
			final InputChannelStatus[] channelStatuses = entry.getValue();

			// only account for stream status inputs that will result in a status change for the input channel
			if (streamStatus.isIdle() && channelStatuses[channelIndex].streamStatus.isActive()) {
				// handle active -> idle toggle for the input channel
				channelStatuses[channelIndex].streamStatus = StreamStatus.IDLE;

				// the channel is now idle, therefore not aligned
				channelStatuses[channelIndex].isWatermarkAligned = false;

				// if all input channels of the valve are now idle, we need to output an idle stream
				// status from the valve (this also marks the valve as idle)
				if (!InputChannelStatus.hasActiveChannels(channelStatuses)) {

					// now that all input channels are idle and no channels will continue to advance its watermark,
					// we should "flush" all watermarks across all channels; effectively, this means emitting
					// the max watermark across all channels as the new watermark. Also, since we already try to advance
					// the min watermark as channels individually become IDLE, here we only need to perform the flush
					// if the watermark of the last active channel that just became idle is the current min watermark.
					if (channelStatuses[channelIndex].watermark == lastOutputWatermarks.get(tag)) {
						findAndOutputMaxWatermarkAcrossAllChannels(tag);
					}

					lastOutputStreamStatus = StreamStatus.IDLE;
					outputHandler.handleStreamStatus(lastOutputStreamStatus);
				} else if (channelStatuses[channelIndex].watermark == lastOutputWatermarks.get(tag)) {
					// if the watermark of the channel that just became idle equals the last output
					// watermark (the previous overall min watermark), we may be able to find a new
					// min watermark from the remaining aligned channels
					findAndOutputNewMinWatermarkAcrossAlignedChannels(tag, channelStatuses);
				}
			} else if (streamStatus.isActive() && channelStatuses[channelIndex].streamStatus.isIdle()) {
				// handle idle -> active toggle for the input channel
				channelStatuses[channelIndex].streamStatus = StreamStatus.ACTIVE;

				// if the last watermark of the input channel, before it was marked idle, is still larger than
				// the overall last output watermark of the valve, then we can set the channel to be aligned already.
				if (channelStatuses[channelIndex].watermark >= lastOutputWatermarks.get(tag)) {
					channelStatuses[channelIndex].isWatermarkAligned = true;
				}

				// if the valve was previously marked to be idle, mark it as active and output an active stream
				// status because at least one of the input channels is now active
				if (lastOutputStreamStatus.isIdle()) {
					lastOutputStreamStatus = StreamStatus.ACTIVE;
					outputHandler.handleStreamStatus(lastOutputStreamStatus);
				}
			}
		}
	}

	private void findAndOutputNewMinWatermarkAcrossAlignedChannels(String tag, InputChannelStatus[] channelStatuses) {
		long newMinWatermark = Long.MAX_VALUE;
		boolean hasAlignedChannels = false;

		// determine new overall watermark by considering only watermark-aligned channels across all channels
		for (InputChannelStatus channelStatus : channelStatuses) {
			if (channelStatus.isWatermarkAligned) {
				hasAlignedChannels = true;
				newMinWatermark = Math.min(channelStatus.watermark, newMinWatermark);
			}
		}

		// we acknowledge and output the new overall watermark if it really is aggregated
		// from some remaining aligned channel, and is also larger than the last output watermark
		if (hasAlignedChannels && newMinWatermark > lastOutputWatermarks.get(tag)) {
			lastOutputWatermarks.put(tag, newMinWatermark);
			Watermark toEmit = new Watermark(newMinWatermark);
			toEmit.setTag(tag);
			outputHandler.handleWatermark(toEmit);
		}
	}

	private void findAndOutputMaxWatermarkAcrossAllChannels(String tag) {
		long maxWatermark = Long.MIN_VALUE;

		for (InputChannelStatus channelStatus : taggedChannelStatuses.get(tag)) {
			maxWatermark = Math.max(channelStatus.watermark, maxWatermark);
		}

		if (maxWatermark > lastOutputWatermarks.get(tag)) {
			lastOutputWatermarks.put(tag, maxWatermark);
			Watermark toEmit = new Watermark(maxWatermark);
			toEmit.setTag(tag);
			outputHandler.handleWatermark(toEmit);
		}
	}

	/**
	 * An {@code InputChannelStatus} keeps track of an input channel's last watermark, stream
	 * status, and whether or not the channel's current watermark is aligned with the overall
	 * watermark output from the valve.
	 *
	 * <p>There are 2 situations where a channel's watermark is not considered aligned:
	 * <ul>
	 *   <li>the current stream status of the channel is idle
	 *   <li>the stream status has resumed to be active, but the watermark of the channel hasn't
	 *   caught up to the last output watermark from the valve yet.
	 * </ul>
	 */
	@VisibleForTesting
	protected static class InputChannelStatus {
		protected long watermark;
		protected StreamStatus streamStatus;
		protected boolean isWatermarkAligned;

		/**
		 * Utility to check if at least one channel in a given array of input channels is active.
		 */
		private static boolean hasActiveChannels(InputChannelStatus[] channelStatuses) {
			for (InputChannelStatus status : channelStatuses) {
				if (status.streamStatus.isActive()) {
					return true;
				}
			}
			return false;
		}
	}

	@VisibleForTesting
	protected InputChannelStatus getInputChannelStatus(String tag, int channelIndex) {
		Preconditions.checkState(taggedChannelStatuses.get(tag) != null);
		Preconditions.checkArgument(
				channelIndex >= 0 && channelIndex < taggedChannelStatuses.get(tag).length,
				"Invalid channel index. Number of input channels: " + taggedChannelStatuses.get(tag).length);

		return taggedChannelStatuses.get(tag)[channelIndex];
	}
}
