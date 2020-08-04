package org.apache.flink.streaming.api.functions.sink.filesystem;

import java.io.IOException;

public interface CommitFileRecoverable {

	/**
	 * Recovers a pending file for finalizing and committing.
	 * @param pendingFileRecoverable The handle with the recovery information.
	 * @return A pending file
	 * @throws IOException Thrown if recovering a pending file fails.
	 */
	PendingFile recoverPendingFile(final InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable) throws IOException;

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
	}
}
