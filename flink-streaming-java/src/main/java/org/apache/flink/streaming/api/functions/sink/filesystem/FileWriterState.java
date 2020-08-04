package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

public class FileWriterState<BucketID> extends WriterState<BucketID> {

	private final Path bucketPath;

	private final long inProgressFileCreationTime;

	@Nullable
	private final InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable;

	FileWriterState(
			final BucketID bucketId,
			final Path bucketPath,
			final long inProgressFileCreationTime,
			@Nullable final InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable) {
		super(bucketId);
		this.bucketPath = Preconditions.checkNotNull(bucketPath);
		this.inProgressFileCreationTime = inProgressFileCreationTime;
		this.inProgressFileRecoverable = inProgressFileRecoverable;
	}

	Path getBucketPath() {
		return bucketPath;
	}

	long getInProgressFileCreationTime() {
		return inProgressFileCreationTime;
	}

	boolean hasInProgressFileRecoverable() {
		return inProgressFileRecoverable != null;
	}

	@Nullable
	InProgressFileWriter.InProgressFileRecoverable getInProgressFileRecoverable() {
		return inProgressFileRecoverable;
	}

	@Override
	public String toString() {
		final StringBuilder strBuilder = new StringBuilder();
		strBuilder
				.append("BucketState for bucketId=").append(getWriterID())
				.append(" and bucketPath=").append(bucketPath);

		if (hasInProgressFileRecoverable()) {
			strBuilder.append(", has open part file created @ ").append(inProgressFileCreationTime);
		}

		return strBuilder.toString();
	}
}
