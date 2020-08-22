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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink.Writer;
import org.apache.flink.api.connector.sink.WriterOutput;
import org.apache.flink.core.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc.
 */
@Internal
public class FileWriterTracker<IN, BucketID> implements
		Writer<IN, InProgressFileWriter.PendingFileRecoverable, FileWriterState<BucketID>, Long>,
		Writer.TimerCallback<InProgressFileWriter.PendingFileRecoverable> {

	private static final Logger LOG = LoggerFactory.getLogger(FileWriterTracker.class);

	// ------------------------ configuration fields --------------------------

	private final Path basePath;

	private final BucketAssigner<IN, BucketID> bucketAssigner;

	private final BucketWriter<IN, BucketID> bucketWriter;

	private final RollingPolicy<IN, BucketID> rollingPolicy;

	private final long bucketCheckInterval;

	// --------------------------- runtime fields -----------------------------

	private final int subtaskId;

	private final int attemptId;

	// TODO: 22.08.20 do we ever clean??? check the Buckets
	private final Map<BucketID, FileWriter<BucketID, IN>> activeBuckets;

	private final Buckets.BucketerContext bucketerContext;

	private final OutputFileConfig outputFileConfig;

	private long maxPartCounter;

	FileWriterTracker(
			final int subtaskId,
			final int attemptId,
			final Path basePath,
			final BucketAssigner<IN, BucketID> bucketAssigner,
			final BucketWriter<IN, BucketID> bucketWriter,
			final RollingPolicy<IN, BucketID> rollingPolicy,
			final OutputFileConfig outputFileConfig,
			final long bucketCheckInterval) {
		this.subtaskId = subtaskId;
		this.attemptId = attemptId;
		this.maxPartCounter = 0L;

		this.basePath = checkNotNull(basePath);
		this.bucketAssigner = checkNotNull(bucketAssigner);
		this.bucketWriter = checkNotNull(bucketWriter);
		this.rollingPolicy = checkNotNull(rollingPolicy);
		this.outputFileConfig = checkNotNull(outputFileConfig);
		this.bucketCheckInterval = bucketCheckInterval;

		this.activeBuckets = new HashMap<>();
		this.bucketerContext = new Buckets.BucketerContext();
	}

	@Override
	public void init(
			final List<FileWriterState<BucketID>> subtaskState,
			final List<Long> sharedState,
			final WriterOutput<InProgressFileWriter.PendingFileRecoverable> output) throws Exception {
		checkNotNull(subtaskState);
		checkNotNull(sharedState);
		checkNotNull(output);

		this.maxPartCounter = getMaxCounter(sharedState);
		LOG.info("Subtask {} initializing its state (max part counter={}).", subtaskId, maxPartCounter);

		for (Map.Entry<BucketID, List<FileWriterState<BucketID>>> entry : groupByBucket(subtaskState).entrySet()) {
			getOrCreateBucketForBucketId(entry.getKey(), entry.getValue(), output);
		}
	}

	private long getMaxCounter(final List<Long> restoredCounters) {
		long maxCounter = 0;
		for (long counter : restoredCounters) {
			maxCounter = Math.max(maxCounter, counter);
		}
		return maxCounter;
	}

	private Map<BucketID, List<FileWriterState<BucketID>>> groupByBucket(final List<FileWriterState<BucketID>> states) {
		final Map<BucketID, List<FileWriterState<BucketID>>> statesByKey = new HashMap<>();
		for (FileWriterState<BucketID> state : states) {
			final List<FileWriterState<BucketID>> stateForKey = statesByKey
					.computeIfAbsent(state.getBucketID(), k -> new ArrayList<>());
			stateForKey.add(state);
		}
		return statesByKey;
	}

	@Override
	public void write(
			final IN element,
			final Context<InProgressFileWriter.PendingFileRecoverable> ctx,
			final WriterOutput<InProgressFileWriter.PendingFileRecoverable> output) throws Exception {
		bucketerContext.update(
				ctx.timestamp(),
				ctx.currentWatermark(),
				ctx.currentProcessingTime());

		final BucketID bucketId = bucketAssigner.getBucketId(element, bucketerContext);
		final FileWriter<BucketID, IN> bucket = getOrCreateBucketForBucketId(
				bucketId, Collections.emptyList(), output);

		final long unstagedPartCounter = bucket.write(element, ctx, output);

		// we update the global max counter here because as buckets become inactive and
		// get removed from the list of active buckets, at the time when we want to create
		// another part file for the bucket, if we start from 0 we may overwrite previous parts.

		this.maxPartCounter = Math.max(maxPartCounter, unstagedPartCounter);
		ctx.registerCallback(this, bucketCheckInterval);
	}

	private FileWriter<BucketID, IN> getOrCreateBucketForBucketId(
			final BucketID bucketId,
			final List<FileWriterState<BucketID>> initStates,
			final WriterOutput<InProgressFileWriter.PendingFileRecoverable> output) throws IOException {
		FileWriter<BucketID, IN> bucket = activeBuckets.get(bucketId);
		if (bucket == null) {
			final Path bucketPath = assembleBucketPath(bucketId);
			bucket = new FileWriter<>(
					subtaskId,
					attemptId,
					bucketId,
					bucketPath,
					maxPartCounter,
					bucketWriter,
					rollingPolicy,
					outputFileConfig);
			bucket.init(initStates, output);
			activeBuckets.put(bucketId, bucket);
		}
		return bucket;
	}

	private Path assembleBucketPath(BucketID bucketId) {
		final String child = bucketId.toString();
		return "".equals(child) ? basePath : new Path(basePath, child);
	}

	@Override
	public Long snapshotSharedState() {
		return maxPartCounter;
	}

	@Override
	public List<FileWriterState<BucketID>> snapshotState(final WriterOutput<InProgressFileWriter.PendingFileRecoverable> output) throws Exception {
		final List<FileWriterState<BucketID>> states = new ArrayList<>();
		if (activeBuckets.isEmpty()) {
			return states;
		}

		for (FileWriter<BucketID, IN> writer : activeBuckets.values()) {
			final FileWriterState<BucketID> state = writer.getSubtaskState(output);
			states.add(state);
		}
		return states;
	}

	@Override
	public void flush(WriterOutput<InProgressFileWriter.PendingFileRecoverable> output) throws Exception {
		if (activeBuckets.isEmpty()) {
			return;
		}

		for (FileWriter<BucketID, IN> writer : activeBuckets.values()) {
			writer.finalize(output);
		}
	}

	@Override
	public void apply(
			final Context<InProgressFileWriter.PendingFileRecoverable> ctx,
			final WriterOutput<InProgressFileWriter.PendingFileRecoverable> output) throws IOException {
		for (FileWriter<BucketID, IN> bucket : activeBuckets.values()) {
			bucket.apply(ctx, output);
		}
	}
}
