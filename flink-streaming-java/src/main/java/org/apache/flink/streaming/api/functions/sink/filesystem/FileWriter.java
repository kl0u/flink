package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedLongSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class FileWriter<BucketID, IN> implements Writer<BucketID, IN, InProgressFileWriter.PendingFileRecoverable, FileWriterState<BucketID>, Long> {

	private static final Logger LOG = LoggerFactory.getLogger(FileWriter.class);

	private final BucketID bucketId;

	private final Path bucketPath;

	private final int subtaskIndex;

	private final BucketWriter<IN, BucketID> bucketWriter;

	private final RollingPolicy<IN, BucketID> rollingPolicy;

	private final OutputFileConfig outputFileConfig;

	private final SimpleVersionedLongSerializer sharedStateSerializer;

	private final FileWriterStateSerializer<BucketID> stateSerializer;

	private boolean initialized;
	
	private long partCounter;

	@Nullable
	private InProgressFileWriter<IN, BucketID> inProgressPart;

	FileWriter(
			final int subtaskIndex,
			final BucketID bucketId,
			final SimpleVersionedSerializer<BucketID> bucketIdSerializer,
			final Path basePath,
			final BucketWriter<IN, BucketID> bucketWriter,
			final RollingPolicy<IN, BucketID> rollingPolicy,
			final OutputFileConfig outputFileConfig) {
		this.subtaskIndex = subtaskIndex;
		this.bucketId = checkNotNull(bucketId);
		this.bucketPath = assembleBucketPath(basePath, bucketId);
		this.bucketWriter = checkNotNull(bucketWriter);
		this.rollingPolicy = checkNotNull(rollingPolicy);
		this.outputFileConfig = checkNotNull(outputFileConfig);

		this.sharedStateSerializer = new SimpleVersionedLongSerializer();
		this.stateSerializer = new FileWriterStateSerializer<>(
				bucketWriter.getProperties().getInProgressFileRecoverableSerializer(),
				bucketIdSerializer);
		this.initialized = false;
	}

	private Path assembleBucketPath(final Path basePath, final BucketID bucketId) {
		checkNotNull(basePath);
		final String child = bucketId.toString();
		return "".equals(child) ? basePath : new Path(basePath, child);
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

	@Override
	public void initState(
			final FileWriterState<BucketID> subtaskState,
			final List<Long> commonState,
			final WriterOutput<InProgressFileWriter.PendingFileRecoverable> output) throws IOException {
		checkState(!initialized);

		for (Long counter : commonState) {
			this.partCounter = Math.max(partCounter, counter);
		}
		// TODO: 07.08.20 also create EMPTY state 
		if (!subtaskState.hasInProgressFileRecoverable()) {
			return;
		}

		final InProgressFileWriter.InProgressFileRecoverable recoverable =
				subtaskState.getInProgressFileRecoverable();

		if (bucketWriter.getProperties().supportsResume()) {
			inProgressPart = bucketWriter.resumeInProgressFileFrom(
					bucketId, recoverable, subtaskState.getInProgressFileCreationTime());
		} else {
			// if the writer does not support resume, we close and commit the in-progress part.
			output.collect(recoverable);
		}
		this.initialized = true; // TODO: 07.08.20 put this flag everywhere 
	}

	@Override
	public boolean isActive() {
		return initialized && inProgressPart != null;
	}

	@Override
	public void combine(
			final Writer<BucketID, IN, InProgressFileWriter.PendingFileRecoverable, FileWriterState<BucketID>, Long> writer,
			final WriterOutput<InProgressFileWriter.PendingFileRecoverable> output) throws Exception {
		checkState(initialized && writer instanceof FileWriter);
		safeCombine((FileWriter<BucketID, IN>) writer, output);
	}

	private void safeCombine(
			final FileWriter<BucketID, IN> writer,
			final WriterOutput<InProgressFileWriter.PendingFileRecoverable> output) throws Exception {
		checkNotNull(output);
		checkNotNull(writer);
		checkState(Objects.equals(writer.bucketPath, bucketPath));

		final InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable = writer.closePartFile();
		if (pendingFileRecoverable != null) {
			output.collect(pendingFileRecoverable);
			LOG.debug("Subtask {} merging buckets for bucket id={}", subtaskIndex, bucketId);
		}
	}

	@Override
	public void write(IN element, Context context, WriterOutput<InProgressFileWriter.PendingFileRecoverable> output) throws IOException {
		checkState(initialized);

		final long currentProcessingTime = context.currentProcessingTime();
		if (inProgressPart == null || rollingPolicy.shouldRollOnEvent(inProgressPart, element)) {

			if (LOG.isDebugEnabled()) {
				LOG.debug("Subtask {} closing in-progress part file for bucket id={} due to element {}.",
						subtaskIndex, bucketId, element);
			}

			final InProgressFileWriter.PendingFileRecoverable recoverable = rollPartFile(currentProcessingTime);
			output.collect(recoverable);
		}
		inProgressPart.write(element, currentProcessingTime);
	}

	@Override
	public FileWriterState<BucketID> getSubtaskState(WriterOutput<InProgressFileWriter.PendingFileRecoverable> output) throws IOException {
		checkState(initialized);

		final InProgressFileWriter.PendingFileRecoverable committable = prepareBucketForCheckpointing();
		output.collect(committable);

		InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable = null;
		long inProgressFileCreationTime = Long.MAX_VALUE;

		if (inProgressPart != null) {
			inProgressFileRecoverable = inProgressPart.persist();
			inProgressFileCreationTime = inProgressPart.getCreationTime();
		}

		return new FileWriterState<>(bucketId, bucketPath, inProgressFileCreationTime, inProgressFileRecoverable);
	}

	@Override
	public Long getCommonState() {
		checkState(initialized);
		return partCounter;
	}

	@Override
	public void onPeriodicCheck(long currentTime, WriterOutput<InProgressFileWriter.PendingFileRecoverable> output) throws IOException {
		checkState(initialized);

		if (inProgressPart != null && rollingPolicy.shouldRollOnProcessingTime(inProgressPart, currentTime)) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Subtask {} closing in-progress part file for bucket id={} due to processing time rolling policy " +
								"(in-progress file created @ {}, last updated @ {} and current time is {}).",
						subtaskIndex, bucketId, inProgressPart.getCreationTime(), inProgressPart.getLastUpdateTime(), currentTime);
			}
			output.collect(closePartFile());
		}
	}

	@Override
	public SimpleVersionedSerializer<FileWriterState<BucketID>> getSubtaskStateSerializer() {
		return this.stateSerializer;
	}

	@Override
	public SimpleVersionedSerializer<Long> getCommonStateSerializer() {
		return this.sharedStateSerializer;
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
