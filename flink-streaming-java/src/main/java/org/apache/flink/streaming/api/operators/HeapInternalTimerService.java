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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link InternalTimerService} that stores timers on the Java heap.
 */
public class HeapInternalTimerService<K, N> implements InternalTimerService<N>, ProcessingTimeCallback {

	private final ProcessingTimeService processingTimeService;

	private final KeyContext keyContext;

	/**
	 * Processing time timers that are currently in-flight.
	 */
	private final Map<String, InternalTimerHeap<K, N>> processingTimeTimersQueue;

	/**
	 * Event time timers that are currently in-flight.
	 */
	private final Map<String, InternalTimerHeap<K, N>> eventTimeTimersQueue;

	/**
	 * Information concerning the local key-group range.
	 */
	private final KeyGroupRange localKeyGroupRange;

	private final int totalKeyGroups;

	private final int localKeyGroupRangeStartIdx;

	/**
	 * The local event time, as denoted by the last received
	 * {@link org.apache.flink.streaming.api.watermark.Watermark Watermark}.
	 */
	private long currentWatermark = Long.MIN_VALUE;

	/**
	 * The one and only Future (if any) registered to execute the
	 * next {@link Triggerable} action, when its (processing) time arrives.
	 * */
	private ScheduledFuture<?> nextTimer;

	// Variables to be set when the service is started.

	private TypeSerializer<String> tagSerializer = StringSerializer.INSTANCE;

	private TypeSerializer<K> keySerializer;

	private TypeSerializer<N> namespaceSerializer;

	private Triggerable<K, N> triggerTarget;

	private volatile boolean isInitialized;

	private TypeSerializer<String> tagDeserializer = StringSerializer.INSTANCE;

	private TypeSerializer<K> keyDeserializer;

	private TypeSerializer<N> namespaceDeserializer;

	/** The restored timers snapshot, if any. */
	private InternalTimersSnapshot<K, N> restoredTimersSnapshot;

	HeapInternalTimerService(
		int totalKeyGroups,
		KeyGroupRange localKeyGroupRange,
		KeyContext keyContext,
		ProcessingTimeService processingTimeService) {

		this.keyContext = checkNotNull(keyContext);
		this.processingTimeService = checkNotNull(processingTimeService);
		this.localKeyGroupRange = checkNotNull(localKeyGroupRange);

		Preconditions.checkArgument(totalKeyGroups > 0);
		this.totalKeyGroups = totalKeyGroups;

		// find the starting index of the local key-group range
		int startIdx = Integer.MAX_VALUE;
		for (Integer keyGroupIdx : localKeyGroupRange) {
			startIdx = Math.min(keyGroupIdx, startIdx);
		}
		this.localKeyGroupRangeStartIdx = startIdx;

		this.eventTimeTimersQueue = new HashMap<>();
		this.processingTimeTimersQueue = new HashMap<>();
	}

	private InternalTimerHeap<K, N> getEventTimeHeapForTag(String tag) {
		return eventTimeTimersQueue.computeIfAbsent(
				tag, x -> new InternalTimerHeap<>(128, localKeyGroupRange, totalKeyGroups));
	}

	private InternalTimerHeap<K, N> getProcessingTimeHeapForTag(String tag) {
		return processingTimeTimersQueue.computeIfAbsent(
				tag, x -> new InternalTimerHeap<>(128, localKeyGroupRange, totalKeyGroups));
	}

	/**
	 * Starts the local {@link HeapInternalTimerService} by:
	 * <ol>
	 *     <li>Setting the {@code keySerialized} and {@code namespaceSerializer} for the timers it will contain.</li>
	 *     <li>Setting the {@code triggerTarget} which contains the action to be performed when a timer fires.</li>
	 *     <li>Re-registering timers that were retrieved after recovering from a node failure, if any.</li>
	 * </ol>
	 * This method can be called multiple times, as long as it is called with the same serializers.
	 */
	public void startTimerService(
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			Triggerable<K, N> triggerTarget) {

		if (!isInitialized) {

			if (keySerializer == null || namespaceSerializer == null) {
				throw new IllegalArgumentException("The TimersService serializers cannot be null.");
			}

			if (this.keySerializer != null || this.namespaceSerializer != null || this.triggerTarget != null) {
				throw new IllegalStateException("The TimerService has already been initialized.");
			}

			// the following is the case where we restore
			if (restoredTimersSnapshot != null) {
				CompatibilityResult<K> keySerializerCompatibility = CompatibilityUtil.resolveCompatibilityResult(
					this.keyDeserializer,
					null,
					restoredTimersSnapshot.getKeySerializerConfigSnapshot(),
					keySerializer);

				CompatibilityResult<N> namespaceSerializerCompatibility = CompatibilityUtil.resolveCompatibilityResult(
					this.namespaceDeserializer,
					null,
					restoredTimersSnapshot.getNamespaceSerializerConfigSnapshot(),
					namespaceSerializer);

				if (keySerializerCompatibility.isRequiresMigration() || namespaceSerializerCompatibility.isRequiresMigration()) {
					throw new IllegalStateException("Tried to initialize restored TimerService " +
						"with incompatible serializers than those used to snapshot its state.");
				}
			}

			this.keySerializer = keySerializer;
			this.namespaceSerializer = namespaceSerializer;
			this.keyDeserializer = null;
			this.namespaceDeserializer = null;

			this.triggerTarget = Preconditions.checkNotNull(triggerTarget);

			// re-register the restored timers (if any)
			for (Map.Entry<String, InternalTimerHeap<K, N>> entry: processingTimeTimersQueue.entrySet()) {
				final InternalTimer<K, N> headTimer = entry.getValue().peek();
				if (headTimer != null) {
					nextTimer = processingTimeService.registerTimer(entry.getKey(), headTimer.getTimestamp(), this);
				}
			}
			this.isInitialized = true;
		} else {
			if (!(this.keySerializer.equals(keySerializer) && this.namespaceSerializer.equals(namespaceSerializer))) {
				throw new IllegalArgumentException("Already initialized Timer Service " +
					"tried to be initialized with different key and namespace serializers.");
			}
		}
	}

	@Override
	public long currentProcessingTime() {
		return processingTimeService.getCurrentProcessingTime();
	}

	@Override
	public long currentWatermark() {
		return currentWatermark;
	}

	@Override
	public void registerProcessingTimeTimer(N namespace, long time) {
		registerProcessingTimeTimer(Watermark.DEFAULT_TAG, namespace, time);
	}

	@Override
	public void registerProcessingTimeTimer(String tag, N namespace, long time) {
		InternalTimerHeap<K, N> timerHeap = getProcessingTimeHeapForTag(tag);

		InternalTimer<K, N> oldHead = timerHeap.peek();
		if (timerHeap.scheduleTimer(time, (K) keyContext.getCurrentKey(), namespace)) {
			long nextTriggerTime = oldHead != null ? oldHead.getTimestamp() : Long.MAX_VALUE;
			// check if we need to re-schedule our timer to earlier
			if (time < nextTriggerTime) {
				if (nextTimer != null) {
					nextTimer.cancel(false);
				}
				// TODO: 6/8/18 should I pass the tag here?
				nextTimer = processingTimeService.registerTimer(tag, time, this);
			}
		}
	}

	@Override
	public void registerEventTimeTimer(N namespace, long time) {
		registerEventTimeTimer(Watermark.DEFAULT_TAG, namespace, time);
	}

	@Override
	public void registerEventTimeTimer(String tag, N namespace, long time) {
		getEventTimeHeapForTag(tag).scheduleTimer(time, (K) keyContext.getCurrentKey(), namespace);
	}

	@Override
	public void deleteProcessingTimeTimer(N namespace, long time) {
		deleteProcessingTimeTimer(Watermark.DEFAULT_TAG, namespace, time);
	}

	@Override
	public void deleteProcessingTimeTimer(String tag, N namespace, long time) {
		getProcessingTimeHeapForTag(tag).stopTimer(time, (K) keyContext.getCurrentKey(), namespace);
	}

	@Override
	public void deleteEventTimeTimer(N namespace, long time) {
		deleteEventTimeTimer(Watermark.DEFAULT_TAG, namespace, time);
	}

	@Override
	public void deleteEventTimeTimer(String tag, N namespace, long time) {
		getEventTimeHeapForTag(tag).stopTimer(time, (K) keyContext.getCurrentKey(), namespace);
	}

	@Override
	public void onProcessingTime(long time) throws Exception {
		onProcessingTime(time, Watermark.DEFAULT_TAG);
	}

	@Override
	public void onProcessingTime(long time, String tag) throws Exception {
		// null out the timer in case the Triggerable calls registerProcessingTimeTimer()
		// inside the callback.
		nextTimer = null;

		InternalTimer<K, N> timer;

		InternalTimerHeap<K, N> timersForTag = getProcessingTimeHeapForTag(tag);
		while ((timer = timersForTag.peek()) != null && timer.getTimestamp() <= time) {
			timersForTag.poll();
			keyContext.setCurrentKey(timer.getKey());
			triggerTarget.onProcessingTime(timer, tag);
		}

		if (timer != null) {
			if (nextTimer == null) {
				nextTimer = processingTimeService.registerTimer(tag, timer.getTimestamp(), this);
			}
		}
	}

	public void advanceWatermark(Watermark watermark) throws Exception {
		System.out.println("WATERMARK: " + watermark);
		final String tag = watermark.getTag();

		currentWatermark = watermark.getTimestamp();

		InternalTimer<K, N> timer;

		InternalTimerHeap<K, N> timersForTag = getEventTimeHeapForTag(tag);
		while ((timer = timersForTag.peek()) != null && timer.getTimestamp() <= currentWatermark) {
			timersForTag.poll();
			keyContext.setCurrentKey(timer.getKey());
			triggerTarget.onEventTime(timer, tag);
		}
	}

	private Map<String, Set<InternalTimer<K, N>>> getEventTimeTimersForKeyGroup(int keyGroupIdx) {
		Map<String, Set<InternalTimer<K, N>>> timersForKeyGroup = new HashMap<>(eventTimeTimersQueue.size());
		for (Map.Entry<String, InternalTimerHeap<K, N>> entry: eventTimeTimersQueue.entrySet()) {
			timersForKeyGroup.put(entry.getKey(), entry.getValue().getTimersForKeyGroup(keyGroupIdx));
		}
		return timersForKeyGroup;
	}

	private Map<String, Set<InternalTimer<K, N>>> getProcessingTimeTimersForKeyGroup(int keyGroupIdx) {
		Map<String, Set<InternalTimer<K, N>>> timersForKeyGroup = new HashMap<>(eventTimeTimersQueue.size());
		for (Map.Entry<String, InternalTimerHeap<K, N>> entry: processingTimeTimersQueue.entrySet()) {
			timersForKeyGroup.put(entry.getKey(), entry.getValue().getTimersForKeyGroup(keyGroupIdx));
		}
		return timersForKeyGroup;
	}

	/**
	 * Snapshots the timers (both processing and event time ones) for a given {@code keyGroupIdx}.
	 *
	 * @param keyGroupIdx the id of the key-group to be put in the snapshot.
	 * @return a snapshot containing the timers for the given key-group, and the serializers for them
	 */
	public InternalTimersSnapshot<K, N> snapshotTimersForKeyGroup(int keyGroupIdx) {
		return new InternalTimersSnapshot<>(
				keySerializer,
				keySerializer.snapshotConfiguration(),
				namespaceSerializer,
				namespaceSerializer.snapshotConfiguration(),
				getEventTimeTimersForKeyGroup(keyGroupIdx),
				getProcessingTimeTimersForKeyGroup(keyGroupIdx));
	}

	/**
	 * Restore the timers (both processing and event time ones) for a given {@code keyGroupIdx}.
	 *
	 * @param restoredSnapshot the restored snapshot containing the key-group's timers,
	 *                       and the serializers that were used to write them
	 * @param keyGroupIdx the id of the key-group to be put in the snapshot.
	 */
	@SuppressWarnings("unchecked")
	public void restoreTimersForKeyGroup(InternalTimersSnapshot<?, ?> restoredSnapshot, int keyGroupIdx) {
		this.restoredTimersSnapshot = (InternalTimersSnapshot<K, N>) restoredSnapshot;

		if (areSnapshotSerializersIncompatible(restoredSnapshot)) {
			throw new IllegalArgumentException("Tried to restore timers " +
				"for the same service with different serializers.");
		}

		this.keyDeserializer = restoredTimersSnapshot.getKeySerializer();
		this.namespaceDeserializer = restoredTimersSnapshot.getNamespaceSerializer();

		checkArgument(localKeyGroupRange.contains(keyGroupIdx),
			"Key Group " + keyGroupIdx + " does not belong to the local range.");

		// restore the event time timers
		for (Map.Entry<String, Set<InternalTimer<K, N>>> entry: restoredTimersSnapshot.getEventTimeTimers().entrySet()) {
			getEventTimeHeapForTag(entry.getKey()).bulkAddRestoredTimers(entry.getValue());
		}

		// restore the processing time timers
		for (Map.Entry<String, Set<InternalTimer<K, N>>> entry: restoredTimersSnapshot.getProcessingTimeTimers().entrySet()) {
			getProcessingTimeHeapForTag(entry.getKey()).bulkAddRestoredTimers(entry.getValue());
		}
	}

	@VisibleForTesting
	public int numProcessingTimeTimers() {
		return this.processingTimeTimersQueue.size();
	}

	@VisibleForTesting
	public int numEventTimeTimers() {
		return this.eventTimeTimersQueue.size();
	}

	@VisibleForTesting
	public int numProcessingTimeTimers(N namespace) {
		int count = 0;
		for (InternalTimerHeap<K, N> heap: processingTimeTimersQueue.values()) {
			for (InternalTimer<K, N> timer : heap) {
				if (timer.getNamespace().equals(namespace)) {
					count++;
				}
			}
		}
		return count;
	}

	@VisibleForTesting
	public int numEventTimeTimers(N namespace) {
		int count = 0;
		for (InternalTimerHeap<K, N> heap: eventTimeTimersQueue.values()) {
			for (InternalTimer<K, N> timer : heap) {
				if (timer.getNamespace().equals(namespace)) {
					count++;
				}
			}
		}
		return count;
	}

	@VisibleForTesting
	int getLocalKeyGroupRangeStartIdx() {
		return this.localKeyGroupRangeStartIdx;
	}

	@VisibleForTesting
	List<Set<InternalTimer<K, N>>> getEventTimeTimersPerKeyGroup() {
		List<Set<InternalTimer<K, N>>> res = new ArrayList<>();
		for (InternalTimerHeap<K, N> heap: eventTimeTimersQueue.values()) {
			res.addAll(heap.getTimersByKeyGroup());
		}
		return res;
	}

	@VisibleForTesting
	List<Set<InternalTimer<K, N>>> getProcessingTimeTimersPerKeyGroup() {
		List<Set<InternalTimer<K, N>>> res = new ArrayList<>();
		for (InternalTimerHeap<K, N> heap: processingTimeTimersQueue.values()) {
			res.addAll(heap.getTimersByKeyGroup());
		}
		return res;
	}

	private boolean areSnapshotSerializersIncompatible(InternalTimersSnapshot<?, ?> restoredSnapshot) {
		return (this.keyDeserializer != null && !this.keyDeserializer.equals(restoredSnapshot.getKeySerializer())) ||
			(this.namespaceDeserializer != null && !this.namespaceDeserializer.equals(restoredSnapshot.getNamespaceSerializer()));
	}
}
