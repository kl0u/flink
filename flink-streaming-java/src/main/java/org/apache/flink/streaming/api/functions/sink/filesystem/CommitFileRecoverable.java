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

import java.io.IOException;

/**
 * Javadoc.
 */
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
