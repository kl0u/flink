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

import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 *
 */
public abstract class PathBasedPartFileWriter<IN, BucketID> implements PartFileWriter<IN, BucketID> {
	private final BucketID bucketID;

	private final long creationTime;

	protected final PathBasedBulkWriter<IN> writer;

	private long lastUpdateTime;

	public PathBasedPartFileWriter(
		final BucketID bucketID,
		PathBasedBulkWriter<IN> writer,
		long createTime) {

		this.bucketID = bucketID;
		this.writer = writer;

		this.creationTime = createTime;
		this.lastUpdateTime = createTime;
	}

	@Override
	public void write(IN element, long currentTime) throws IOException {
		writer.addElement(element);
		markWrite(currentTime);
	}

	@Override
	public InProgressFileRecoverable persist() throws IOException {
		throw new UnsupportedOperationException("The hadoop path based writers do not support persisting");
	}

	@Override
	public void dispose() {
		writer.dispose();
	}

	@Override
	public BucketID getBucketId() {
		return bucketID;
	}

	@Override
	public long getCreationTime() {
		return creationTime;
	}

	@Override
	public long getSize() throws IOException {
		throw new UnsupportedOperationException("Size of the path-based writer is unknown");
	}

	@Override
	public long getLastUpdateTime() {
		return lastUpdateTime;
	}

	void markWrite(long now) {
		this.lastUpdateTime = now;
	}

	public static class PathBasedPendingFileRecoverable implements PendingFileRecoverable {
		private final Path path;

		public PathBasedPendingFileRecoverable(Path path) {
			this.path = path;
		}

		public Path getPath() {
			return path;
		}
	}

	public static class PathBasedPendingFileRecoverableSerializer implements SimpleVersionedSerializer<PathBasedPendingFileRecoverable> {
		static final PathBasedPendingFileRecoverableSerializer INSTANCE = new PathBasedPendingFileRecoverableSerializer();

		private static final Charset CHARSET = StandardCharsets.UTF_8;

		private static final int MAGIC_NUMBER = 0x2c853c90;

		@Override
		public int getVersion() {
			return 1;
		}

		@Override
		public byte[] serialize(PathBasedPendingFileRecoverable pendingFileRecoverable) throws IOException {
			byte[] pathBytes = pendingFileRecoverable.getPath().toString().getBytes(CHARSET);

			byte[] targetBytes = new byte[8 + pathBytes.length];
			ByteBuffer bb = ByteBuffer.wrap(targetBytes).order(ByteOrder.LITTLE_ENDIAN);
			bb.putInt(MAGIC_NUMBER);
			bb.putInt(pathBytes.length);
			bb.put(pathBytes);

			return targetBytes;
		}

		@Override
		public PathBasedPendingFileRecoverable deserialize(int version, byte[] serialized) throws IOException {
			switch (version) {
				case 1:
					return deserializeV1(serialized);
				default:
					throw new IOException("Unrecognized version or corrupt state: " + version);
			}
		}

		private PathBasedPendingFileRecoverable deserializeV1(byte[] serialized) throws IOException {
			final ByteBuffer bb = ByteBuffer.wrap(serialized).order(ByteOrder.LITTLE_ENDIAN);

			if (bb.getInt() != MAGIC_NUMBER) {
				throw new IOException("Corrupt data: Unexpected magic number.");
			}

			byte[] pathBytes = new byte[bb.getInt()];
			bb.get(pathBytes);
			String targetPath = new String(pathBytes, CHARSET);

			return new PathBasedPendingFileRecoverable(new Path(targetPath));
		}
	}

	public abstract static class Factory<IN, BucketID> implements PartFileFactory<IN, BucketID> {

		@Override
		public PartFileWriter<IN, BucketID> resumeFrom(BucketID bucketID, InProgressFileRecoverable inProgressFileSnapshot, long creationTime) throws IOException {
			throw new UnsupportedOperationException("Resume is not supported");
		}

		@Override
		public boolean requiresCleanupOfInProgressFileRecoverableState() {
			return false;
		}

		@Override
		public boolean cleanupInProgressFileRecoverable(InProgressFileRecoverable inProgressFileRecoverable) throws IOException {
			return false;
		}

		@Override
		public SimpleVersionedSerializer<? extends PendingFileRecoverable> getPendingFileRecoverableSerializer() {
			return PathBasedPendingFileRecoverableSerializer.INSTANCE;
		}

		@Override
		public SimpleVersionedSerializer<? extends InProgressFileRecoverable> getInProgressFileRecoverableSerializer() {
			return new SimpleVersionedSerializer<InProgressFileRecoverable>() {
				@Override
				public int getVersion() {
					return 0;
				}

				@Override
				public byte[] serialize(InProgressFileRecoverable obj) throws IOException {
					return new byte[0];
				}

				@Override
				public InProgressFileRecoverable deserialize(int version, byte[] serialized) throws IOException {
					return null;
				}
			};
		}

		@Override
		public boolean supportsResume() {
			return true;
		}
	}
}
