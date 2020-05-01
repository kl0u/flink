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

package org.apache.flink.formats.hive;

import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.PathBasedBulkWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.PathBasedPartFileWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 *
 */
public class HadoopPathBasedPartFileWriter<IN, BucketID> extends PathBasedPartFileWriter<IN, BucketID> {
	private final HadoopFileCommitter fileCommitter;

	public HadoopPathBasedPartFileWriter(
		final BucketID bucketID,
		PathBasedBulkWriter<IN> writer,
		HadoopFileCommitter fileCommitter,
		long createTime) {

		super(bucketID, writer, createTime);

		this.fileCommitter = fileCommitter;
	}

	@Override
	public PendingFileRecoverable closeForCommit() throws IOException {
		writer.flush();
		writer.finish();
		fileCommitter.preCommit();
		return new HadoopPathBasedPendingFile(fileCommitter).getRecoverable();
	}

	@Override
	public void dispose() {
		writer.dispose();
	}

	static class HadoopPathBasedPendingFile implements PendingFile {
		private final HadoopFileCommitter fileCommitter;

		public HadoopPathBasedPendingFile(HadoopFileCommitter fileCommitter) {
			this.fileCommitter = fileCommitter;
		}

		@Override
		public void commit() throws IOException {
			fileCommitter.commit();
		}

		@Override
		public void commitAfterRecovery() throws IOException {
			fileCommitter.commitAfterRecovery();
		}

		@Override
		public PendingFileRecoverable getRecoverable() {
			return new PathBasedPendingFileRecoverable(
				new org.apache.flink.core.fs.Path(
					fileCommitter.getPath().toString()));
		}
	}

	public static class Factory<IN, BucketID> extends PathBasedPartFileWriter.Factory<IN, BucketID> {
		private final Configuration configuration;

		private final HadoopPathBasedBulkWriterFactory<IN> bulkWriterFactory;

		private final HadoopFileCommitterFactory fileCommitterFactory;

		public Factory(
			Configuration configuration,
			HadoopPathBasedBulkWriterFactory<IN> bulkWriterFactory,
			HadoopFileCommitterFactory fileCommitterFactory) {

			this.configuration = configuration;
			this.bulkWriterFactory = bulkWriterFactory;
			this.fileCommitterFactory = fileCommitterFactory;
		}

		@Override
		public PartFileWriter<IN, BucketID> openNew(BucketID bucketID, org.apache.flink.core.fs.Path flinkPath, long creationTime) throws IOException {
			Path path = new Path(flinkPath.toUri());
			HadoopFileCommitter fileCommitter = fileCommitterFactory.create(configuration, path);

			Path inProgressFilePath = fileCommitter.getInProgressFilePath();
			PathBasedBulkWriter<IN> writer = bulkWriterFactory.create(path, inProgressFilePath);
			return new HadoopPathBasedPartFileWriter<>(bucketID, writer, fileCommitter, creationTime);
		}

		@Override
		public PendingFile recoverPendingFile(PendingFileRecoverable pendingFileRecoverable) throws IOException {
			if (!(pendingFileRecoverable instanceof PathBasedPendingFileRecoverable)) {
				throw new UnsupportedOperationException("Only PathBasedPendingFileRecoverable is supported.");
			}

			Path path = new Path(((PathBasedPendingFileRecoverable) pendingFileRecoverable).getPath().toString());
			return new HadoopPathBasedPendingFile(fileCommitterFactory.create(configuration, path));
		}
	}
}
