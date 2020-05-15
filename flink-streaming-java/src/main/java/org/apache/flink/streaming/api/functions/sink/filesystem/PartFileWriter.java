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
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/**
 * The {@link Bucket} uses the {@link PartFileWriter} to write element to a part file.
 */
@Internal
public interface PartFileWriter<IN, BucketID> extends PartFileInfo<BucketID> {

	/**
	 * Write a element to the part file.
	 * @param element the element to be written
	 * @param currentTime the writing time
	 * @throws IOException
	 */
	void write(final IN element, final long currentTime) throws IOException;

	/**
	 * @return the state of the current part file.
	 * @throws IOException
	 */
	InProgressFileRecoverable persist() throws IOException;


	/**
	 * @return The state of the pending part file. {@link Bucket} uses this to commit the pending file.
	 * @throws IOException
	 */
	PendingFileRecoverable closeForCommit() throws IOException;

	/**
	 * TODO:: why there is a dispose method for the writer?
	 */
	void dispose();

	// ------------------------------------------------------------------------

	/**
	 * An interface for factories that create the different {@link PartFileWriter writers}.
	 */
	interface PartFileFactory<IN, BucketID> {

		/**
		 * Used to create a new {@link PartFileWriter}.
		 * @param bucketID the id of the bucket this writer is writing to.
		 * @param path the path this writer will write to.
		 * @param creationTime the creation time of the file.
		 * @return the new {@link PartFileWriter}
		 */
		PartFileWriter<IN, BucketID> openNewInProgressFile(
			final BucketID bucketID,
			final Path path,
			final long creationTime) throws IOException;

		/**
		 * Used to resume a {@link PartFileWriter} from a {@link InProgressFileRecoverable}.
		 * @param bucketID the id of the bucket this writer is writing to.
		 * @param inProgressFileSnapshot the state of the part file.
		 * @param creationTime the creation time of the file.
		 * @return the resumed {@link PartFileWriter}
		 */
		PartFileWriter<IN, BucketID> resumeInProgressFileFrom(
			final BucketID bucketID,
			final InProgressFileRecoverable inProgressFileSnapshot,
			final long creationTime) throws IOException;

		WriterProperties getProperties();

		/**
		 * todo we have PendingFileRecoverable and InProgressFileRecoverable but only one method for both????
		 * WHY DO WE NEED BOTH???
		 *
		 * Recovers a pending file for finalizing and committing.
		 * @param pendingFileRecoverable The handle with the recovery information.
		 * @return A pending file
		 */
		PendingFile recoverPendingFile(final PendingFileRecoverable pendingFileRecoverable) throws IOException;

		/**
		 * Frees up any resources that were previously occupied in order to be able to
		 * recover from a (potential) failure.
		 *
		 * <p><b>NOTE:</b> This operation should not throw an exception.
		 *
		 * @param inProgressFileRecoverable the {@link InProgressFileRecoverable} whose state we want to clean-up.
		 * @return {@code true} if the resources were successfully freed, {@code false} otherwise
		 * (e.g. the writer does not require clean-up or the file to be deleted was not there for any reason
		 * - already deleted or never created).
		 */
		boolean cleanupInProgressFileRecoverable(final InProgressFileRecoverable inProgressFileRecoverable) throws IOException;
	}

	 /**
	  * todo why do we need both the below.
	 * A handle can be used to recover in-progress file..
	 */
	interface InProgressFileRecoverable extends PendingFileRecoverable {}


	/**
	 * The handle can be used to recover pending file.
	 */
	interface PendingFileRecoverable {}

	/**
	 * This represents the file that can not write any data to.
	 */
	interface PendingFile {
		/**
		 * Commits the pending file, making it visible. The file will contain the exact data
		 * as when the pending file was created.
		 *
		 * @throws IOException Thrown if committing fails.
		 */
		void commit() throws IOException;

		/**
		 * Commits the pending file, making it visible. The file will contain the exact data
		 * as when the pending file was created.
		 *
		 * <p>This method tolerates situations where the file was already committed and
		 * will not raise an exception in that case. This is important for idempotent
		 * commit retries as they need to happen after recovery.
		 *
		 * @throws IOException Thrown if committing fails.
		 */
		void commitAfterRecovery() throws IOException;

		/**
		 * TODO why is this needed? A pending file can  only be committed either normally or after recovery
		 *
		 * Gets a recoverable object to recover the pending file The recovered pending file
		 * will commit the file with the exact same data as this pending file would commit
		 * it.
		 */
		PendingFileRecoverable getRecoverable();
	}
}
