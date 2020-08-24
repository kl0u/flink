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

import org.apache.flink.api.connector.sink.Writer;
import org.apache.flink.api.connector.sink.WriterOutput;
import org.apache.flink.core.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Javadoc.
 */
public class FileWriter<BucketID, IN> {

	private static final Logger LOG = LoggerFactory.getLogger(FileWriter.class);

	private final BucketID bucketId;

	private final Path bucketPath;

	private final int subtaskIndex;

	private final BucketWriter<IN, BucketID> bucketWriter;

	private final RollingPolicy<IN, BucketID> rollingPolicy;

	private final OutputFileConfig outputFileConfig;

	private final int attemptId; // TODO: 24.08.20 this may become the epoch. 

	private boolean initialized;

	private long partCounter;

	@Nullable
	private InProgressFileWriter<IN, BucketID> inProgressPart;

	FileWriter(
			final int subtaskIndex,
			final int attemptId,
			final BucketID bucketId,
			final Path bucketPath,
			final long initialPartCounter,
			final BucketWriter<IN, BucketID> bucketWriter,
			final RollingPolicy<IN, BucketID> rollingPolicy,
			final OutputFileConfig outputFileConfig) {
		this.subtaskIndex = subtaskIndex;
		this.bucketId = checkNotNull(bucketId);
		this.bucketPath = checkNotNull(bucketPath);
		this.attemptId = attemptId;
		this.partCounter = initialPartCounter;
		this.bucketWriter = checkNotNull(bucketWriter);
		this.rollingPolicy = checkNotNull(rollingPolicy);
		this.outputFileConfig = checkNotNull(outputFileConfig);
		this.initialized = false;
	}

	public BucketID getBucketId() {
		return bucketId;
	}

	boolean isActive() {
		return initialized && inProgressPart != null;
	}

	void init(
			final List<FileWriterState<BucketID>> writerStates,
			final WriterOutput<InProgressFileWriter.PendingFileRecoverable> output) throws IOException {
		checkState(writerStates != null && !initialized);

		for (FileWriterState<BucketID> state : writerStates) {
			if (!state.hasInProgressFileRecoverable()) {
				return;
			}

			final InProgressFileWriter.InProgressFileRecoverable recoverable =
					state.getInProgressFileRecoverable();

			if (bucketWriter.getProperties().supportsResume() && inProgressPart == null) {
				inProgressPart = bucketWriter.resumeInProgressFileFrom(
						bucketId, recoverable, state.getInProgressFileCreationTime());
			} else {
				// TODO: 13.08.20 test this
				final InProgressFileWriter.PendingFileRecoverable rec = bucketWriter
						.resumeInProgressFileFrom(bucketId, recoverable, state.getInProgressFileCreationTime())
						.closeForCommit();
				output.sendToCommit(rec);
			}
		}
		this.initialized = true;
	}

	long write(
			final IN element,
			final Writer.Context<InProgressFileWriter.PendingFileRecoverable> context,
			final WriterOutput<InProgressFileWriter.PendingFileRecoverable> output) throws IOException {
		checkState(initialized);

		final long currentProcessingTime = context.currentProcessingTime();
		if (inProgressPart == null || rollingPolicy.shouldRollOnEvent(inProgressPart, element)) {

			if (LOG.isDebugEnabled()) {
				LOG.debug("Subtask {} closing in-progress part file for bucket id={} due to element {}.",
						subtaskIndex, bucketId, element);
			}

			final InProgressFileWriter.PendingFileRecoverable recoverable =
					rollPartFile(currentProcessingTime);

			if (recoverable != null) {
				output.sendToCommit(recoverable);
			}
		}
		inProgressPart.write(element, currentProcessingTime);
		return partCounter;
	}

	void apply(
			final Writer.Context<InProgressFileWriter.PendingFileRecoverable> ctx,
			final WriterOutput<InProgressFileWriter.PendingFileRecoverable> output) throws IOException {
		checkState(initialized);

		if (inProgressPart != null && rollingPolicy.shouldRollOnProcessingTime(inProgressPart, ctx.currentProcessingTime())) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Subtask {} closing in-progress part file for bucket id={} due to processing time rolling policy " +
								"(in-progress file created @ {}, last updated @ {} and current time is {}).",
						subtaskIndex, bucketId, inProgressPart.getCreationTime(), inProgressPart.getLastUpdateTime(), ctx.currentProcessingTime());
			}
			output.sendToCommit(closePartFile());
		}
	}

	FileWriterState<BucketID> getSubtaskState(final WriterOutput<InProgressFileWriter.PendingFileRecoverable> output) throws IOException {
		checkState(initialized);

		final InProgressFileWriter.PendingFileRecoverable committable = prepareBucketForCheckpointing();
		if (committable != null) {
			output.sendToCommit(committable);
		}

		InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable = null;
		long inProgressFileCreationTime = Long.MAX_VALUE;

		if (inProgressPart != null) {
			inProgressFileRecoverable = inProgressPart.persist();
			inProgressFileCreationTime = inProgressPart.getCreationTime();
		}

		return new FileWriterState<>(bucketId, bucketPath, inProgressFileCreationTime, inProgressFileRecoverable);
	}

	void flush(final WriterOutput<InProgressFileWriter.PendingFileRecoverable> output) throws Exception {
		checkState(initialized);

		final InProgressFileWriter.PendingFileRecoverable committable = closePartFile();
		if (committable != null) {
			output.sendToCommit(committable);
		}
	}

	private InProgressFileWriter.PendingFileRecoverable rollPartFile(final long currentTime) throws IOException {
		final InProgressFileWriter.PendingFileRecoverable recoverable = closePartFile();

		final Path partFilePath = assembleNewPartPath();
		inProgressPart = bucketWriter.openNewInProgressFile(bucketId, partFilePath, currentTime);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Subtask {} opening new part file \"{}\" for bucket id={}.",
					subtaskIndex, partFilePath.getName(), bucketId);
		}

		partCounter++;
		return recoverable;
	}

	private Path assembleNewPartPath() {
		return new Path(bucketPath, outputFileConfig.getPartPrefix() + '-' + subtaskIndex + '-' + partCounter + outputFileConfig.getPartSuffix());
	}

	private InProgressFileWriter.PendingFileRecoverable closePartFile() throws IOException {
		if (inProgressPart == null) {
			return null;
		}
		final InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable = inProgressPart.closeForCommit();
		inProgressPart = null;
		return pendingFileRecoverable;
	}

	private InProgressFileWriter.PendingFileRecoverable prepareBucketForCheckpointing() throws IOException {
		if (inProgressPart != null && rollingPolicy.shouldRollOnCheckpoint(inProgressPart)) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Subtask {} closing in-progress part file for bucket id={} on checkpoint.", subtaskIndex, bucketId);
			}
			return closePartFile();
		}
		return null;
	}
}
