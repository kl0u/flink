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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A bucket is the directory organization of the output of the {@link StreamingFileSink}.
 *
 * <p>For each incoming element in the {@code StreamingFileSink}, the user-specified
 * {@link BucketAssigner} is queried to see in which bucket this element should be written to.
 */
@Internal
public class Bucket<IN, BucketID> {

	private static final Logger LOG = LoggerFactory.getLogger(Bucket.class);

	private final BucketID bucketId;

	private final Path bucketPath;

	private final int subtaskIndex;

	private final PartFileWriter.PartFileFactory<IN, BucketID> partFileFactory;

	private final RollingPolicy<IN, BucketID> rollingPolicy;

	private final NavigableMap<Long, PartFileWriter.InProgressFileRecoverable> inProgressFileRecoverablesPerCheckpoint;

	private final NavigableMap<Long, List<PartFileWriter.PendingFileRecoverable>> pendingFileRecoverablesPerCheckpoint;

	private final OutputFileConfig outputFileConfig;

	private long partCounter;

	@Nullable
	private PartFileWriter<IN, BucketID> inProgressPart;

	private List<PartFileWriter.PendingFileRecoverable> pendingPartsForCurrentCheckpoint;

	/**
	 * Constructor to create a new empty bucket.
	 */
	private Bucket(
			final int subtaskIndex,
			final BucketID bucketId,
			final Path bucketPath,
			final long initialPartCounter,
			final PartFileWriter.PartFileFactory<IN, BucketID> partFileFactory,
			final RollingPolicy<IN, BucketID> rollingPolicy,
			final OutputFileConfig outputFileConfig) {
		this.subtaskIndex = subtaskIndex;
		this.bucketId = checkNotNull(bucketId);
		this.bucketPath = checkNotNull(bucketPath);
		this.partCounter = initialPartCounter;
		this.partFileFactory = checkNotNull(partFileFactory);
		this.rollingPolicy = checkNotNull(rollingPolicy);

		this.pendingPartsForCurrentCheckpoint = new ArrayList<>();
		this.pendingFileRecoverablesPerCheckpoint = new TreeMap<>();
		this.inProgressFileRecoverablesPerCheckpoint = new TreeMap<>();

		this.outputFileConfig = checkNotNull(outputFileConfig);
	}

	/**
	 * Constructor to restore a bucket from checkpointed state.
	 */
	private Bucket(
			final int subtaskIndex,
			final long initialPartCounter,
			final PartFileWriter.PartFileFactory<IN, BucketID> partFileFactory,
			final RollingPolicy<IN, BucketID> rollingPolicy,
			final BucketState<BucketID> bucketState,
			final OutputFileConfig outputFileConfig) throws IOException {

		this(
				subtaskIndex,
				bucketState.getBucketId(),
				bucketState.getBucketPath(),
				initialPartCounter,
				partFileFactory,
				rollingPolicy,
				outputFileConfig);

		restoreInProgressFile(bucketState);
		commitRecoveredPendingFiles(bucketState);
	}

	private void restoreInProgressFile(final BucketState<BucketID> state) throws IOException {
		if (!state.hasInProgressResumableFile()) {
			return;
		}

		// we try to resume the previous in-progress file
		final PartFileWriter.InProgressFileRecoverable inProgressFileRecoverable = state.getInProgressFileRecoverable();

		if (partFileFactory.supportsResume()) {
			inProgressPart = partFileFactory.resumeFrom(
					bucketId, inProgressFileRecoverable, state.getInProgressFileCreationTime());
		} else {
			// if the writer does not support resume, then we close the
			// in-progress part and commit it, as done in the case of pending files.
			partFileFactory.recoverPendingFile(inProgressFileRecoverable).commitAfterRecovery();
		}
	}

	private void commitRecoveredPendingFiles(final BucketState<BucketID> state) throws IOException {

		// we commit pending files for checkpoints that precess the last successful one, from which we are recovering
		for (List<PartFileWriter.PendingFileRecoverable> pendingFileRecoverables: state.getPendingFileRecoverables().values()) {
			for (PartFileWriter.PendingFileRecoverable pendingFileRecoverable: pendingFileRecoverables) {
				partFileFactory.recoverPendingFile(pendingFileRecoverable).commitAfterRecovery();
			}
		}
	}

	public BucketID getBucketId() {
		return bucketId;
	}

	public Path getBucketPath() {
		return bucketPath;
	}

	public long getPartCounter() {
		return partCounter;
	}

	boolean isActive() {
		return inProgressPart != null || !pendingPartsForCurrentCheckpoint.isEmpty() || !pendingFileRecoverablesPerCheckpoint.isEmpty();
	}

	void merge(final Bucket<IN, BucketID> bucket) throws IOException {
		checkNotNull(bucket);
		checkState(Objects.equals(bucket.bucketPath, bucketPath));

		// There should be no pending files in the "to-merge" states.
		// The reason is that:
		// 1) the pendingPartsForCurrentCheckpoint is emptied whenever we take a Recoverable (see prepareBucketForCheckpointing()).
		//    So a Recoverable, including the one we are recovering from, will never contain such files.
		// 2) the files in pendingFileRecoverablesPerCheckpoint are committed upon recovery (see commitRecoveredPendingFiles()).

		checkState(bucket.pendingPartsForCurrentCheckpoint.isEmpty());
		checkState(bucket.pendingFileRecoverablesPerCheckpoint.isEmpty());

		PartFileWriter.PendingFileRecoverable pendingFileRecoverable = bucket.closePartFile();
		if (pendingFileRecoverable != null) {
			pendingPartsForCurrentCheckpoint.add(pendingFileRecoverable);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Subtask {} merging buckets for bucket id={}", subtaskIndex, bucketId);
		}
	}

	void write(IN element, long currentTime) throws IOException {
		if (inProgressPart == null || rollingPolicy.shouldRollOnEvent(inProgressPart, element)) {

			if (LOG.isDebugEnabled()) {
				LOG.debug("Subtask {} closing in-progress part file for bucket id={} due to element {}.",
						subtaskIndex, bucketId, element);
			}

			rollPartFile(currentTime);
		}
		inProgressPart.write(element, currentTime);
	}

	private void rollPartFile(final long currentTime) throws IOException {
		closePartFile();

		final Path partFilePath = assembleNewPartPath();
		inProgressPart = partFileFactory.openNew(bucketId, partFilePath, currentTime);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Subtask {} opening new part file \"{}\" for bucket id={}.",
					subtaskIndex, partFilePath.getName(), bucketId);
		}

		partCounter++;
	}

	private Path assembleNewPartPath() {
		return new Path(bucketPath, outputFileConfig.getPartPrefix() + '-' + subtaskIndex + '-' + partCounter + outputFileConfig.getPartSuffix());
	}

	private PartFileWriter.PendingFileRecoverable closePartFile() throws IOException {
		PartFileWriter.PendingFileRecoverable pendingFileRecoverable = null;
		if (inProgressPart != null) {
			pendingFileRecoverable = inProgressPart.closeForCommit();
			pendingPartsForCurrentCheckpoint.add(pendingFileRecoverable);
			inProgressPart = null;
		}
		return pendingFileRecoverable;
	}

	void disposePartFile() {
		if (inProgressPart != null) {
			inProgressPart.dispose();
		}
	}

	BucketState<BucketID> onReceptionOfCheckpoint(long checkpointId) throws IOException {
		prepareBucketForCheckpointing(checkpointId);

		PartFileWriter.InProgressFileRecoverable inProgressFileRecoverable = null;
		long inProgressFileCreationTime = Long.MAX_VALUE;

		if (inProgressPart != null) {
			inProgressFileRecoverable = inProgressPart.persist();
			inProgressFileCreationTime = inProgressPart.getCreationTime();

			// TODO: 12.05.20 here there was a note about an optimization for the memory consumption of the map.
			this.inProgressFileRecoverablesPerCheckpoint.put(checkpointId, inProgressFileRecoverable);
		}

		return new BucketState<>(bucketId, bucketPath, inProgressFileCreationTime, inProgressFileRecoverable, pendingFileRecoverablesPerCheckpoint);
	}

	private void prepareBucketForCheckpointing(long checkpointId) throws IOException {
		if (inProgressPart != null && rollingPolicy.shouldRollOnCheckpoint(inProgressPart)) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Subtask {} closing in-progress part file for bucket id={} on checkpoint.", subtaskIndex, bucketId);
			}
			closePartFile();
		}

		if (!pendingPartsForCurrentCheckpoint.isEmpty()) {
			pendingFileRecoverablesPerCheckpoint.put(checkpointId, pendingPartsForCurrentCheckpoint);
			pendingPartsForCurrentCheckpoint = new ArrayList<>();
		}
	}

	void onSuccessfulCompletionOfCheckpoint(long checkpointId) throws IOException {
		checkNotNull(partFileFactory);

		Iterator<Map.Entry<Long, List<PartFileWriter.PendingFileRecoverable>>> it =
				pendingFileRecoverablesPerCheckpoint.headMap(checkpointId, true)
						.entrySet().iterator();

		while (it.hasNext()) {
			Map.Entry<Long, List<PartFileWriter.PendingFileRecoverable>> entry = it.next();

			for (PartFileWriter.PendingFileRecoverable pendingFileRecoverable : entry.getValue()) {
				partFileFactory.recoverPendingFile(pendingFileRecoverable).commit();
			}
			it.remove();
		}

		cleanupInProgressFileRecoverables(checkpointId);
	}

	private void cleanupInProgressFileRecoverables(long checkpointId) throws IOException {
		Iterator<Map.Entry<Long, PartFileWriter.InProgressFileRecoverable>> it =
				inProgressFileRecoverablesPerCheckpoint.headMap(checkpointId, false)
						.entrySet().iterator();

		while (it.hasNext()) {
			final PartFileWriter.InProgressFileRecoverable inProgressFileRecoverable = it.next().getValue();

			// this check is redundant, as we only put entries in the inProgressFileRecoverablesPerCheckpoint map
			// list when the requiresCleanupOfInProgressFileRecoverableState() returns true, but having it makes
			// the code more readable.

			if (partFileFactory.requiresCleanupOfInProgressFileRecoverableState()) {
				final boolean successfullyDeleted = partFileFactory.cleanupInProgressFileRecoverable(inProgressFileRecoverable);

				if (LOG.isDebugEnabled() && successfullyDeleted) {
					LOG.debug("Subtask {} successfully deleted incomplete part for bucket id={}.", subtaskIndex, bucketId);
				}
			}
			it.remove();
		}
	}

	void onProcessingTime(long timestamp) throws IOException {
		if (inProgressPart != null && rollingPolicy.shouldRollOnProcessingTime(inProgressPart, timestamp)) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Subtask {} closing in-progress part file for bucket id={} due to processing time rolling policy " +
						"(in-progress file created @ {}, last updated @ {} and current time is {}).",
						subtaskIndex, bucketId, inProgressPart.getCreationTime(), inProgressPart.getLastUpdateTime(), timestamp);
			}
			closePartFile();
		}
	}

	// --------------------------- Testing Methods -----------------------------

	@VisibleForTesting
	Map<Long, List<PartFileWriter.PendingFileRecoverable>> getPendingFileRecoverablesPerCheckpoint() {
		return pendingFileRecoverablesPerCheckpoint;
	}

	@Nullable
	@VisibleForTesting
	PartFileWriter<IN, BucketID> getInProgressPart() {
		return inProgressPart;
	}

	@VisibleForTesting
	List<PartFileWriter.PendingFileRecoverable> getPendingPartsForCurrentCheckpoint() {
		return pendingPartsForCurrentCheckpoint;
	}

	// --------------------------- Static Factory Methods -----------------------------

	/**
	 * Creates a new empty {@code Bucket}.
	 * @param subtaskIndex the index of the subtask creating the bucket.
	 * @param bucketId the identifier of the bucket, as returned by the {@link BucketAssigner}.
	 * @param bucketPath the path to where the part files for the bucket will be written to.
	 * @param initialPartCounter the initial counter for the part files of the bucket.
	 * @param partFileFactory the {@link PartFileWriter.PartFileFactory} the factory creating part file writers.
	 * @param <IN> the type of input elements to the sink.
	 * @param <BucketID> the type of the identifier of the bucket, as returned by the {@link BucketAssigner}
	 * @param outputFileConfig the part file configuration.
	 * @return The new Bucket.
	 */
	static <IN, BucketID> Bucket<IN, BucketID> getNew(
			final int subtaskIndex,
			final BucketID bucketId,
			final Path bucketPath,
			final long initialPartCounter,
			final PartFileWriter.PartFileFactory<IN, BucketID> partFileFactory,
			final RollingPolicy<IN, BucketID> rollingPolicy,
			final OutputFileConfig outputFileConfig) {
		return new Bucket<>(subtaskIndex, bucketId, bucketPath, initialPartCounter, partFileFactory, rollingPolicy, outputFileConfig);
	}

	/**
	 * Restores a {@code Bucket} from the state included in the provided {@link BucketState}.
	 * @param subtaskIndex the index of the subtask creating the bucket.
	 * @param initialPartCounter the initial counter for the part files of the bucket.
	 * @param partFileFactory the {@link PartFileWriter.PartFileFactory} the factory creating part file writers.
	 * @param bucketState the initial state of the restored bucket.
	 * @param <IN> the type of input elements to the sink.
	 * @param <BucketID> the type of the identifier of the bucket, as returned by the {@link BucketAssigner}
	 * @param outputFileConfig the part file configuration.
	 * @return The restored Bucket.
	 */
	static <IN, BucketID> Bucket<IN, BucketID> restore(
			final int subtaskIndex,
			final long initialPartCounter,
			final PartFileWriter.PartFileFactory<IN, BucketID> partFileFactory,
			final RollingPolicy<IN, BucketID> rollingPolicy,
			final BucketState<BucketID> bucketState,
			final OutputFileConfig outputFileConfig) throws IOException {
		return new Bucket<>(subtaskIndex, initialPartCounter, partFileFactory, rollingPolicy, bucketState, outputFileConfig);
	}
}
