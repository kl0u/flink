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

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * Javadoc.
 */
public class FileWriterStateSerializer<WriterID> implements SimpleVersionedSerializer<FileWriterState<WriterID>> {

	private static final int MAGIC_NUMBER = 0x1e764b79;

	private final SimpleVersionedSerializer<InProgressFileWriter.InProgressFileRecoverable> inProgressFileRecoverableSerializer;

	private final SimpleVersionedSerializer<WriterID> bucketIdSerializer;

	FileWriterStateSerializer(
			final SimpleVersionedSerializer<WriterID> bucketIdSerializer,
			final SimpleVersionedSerializer<InProgressFileWriter.InProgressFileRecoverable> inProgressFileRecoverableSerializer) {
		this.bucketIdSerializer = Preconditions.checkNotNull(bucketIdSerializer);
		this.inProgressFileRecoverableSerializer = Preconditions.checkNotNull(inProgressFileRecoverableSerializer);
	}

	@Override
	public int getVersion() {
		return 0;
	}

	@Override
	public byte[] serialize(FileWriterState<WriterID> state) throws IOException {
		DataOutputSerializer out = new DataOutputSerializer(256);
		out.writeInt(MAGIC_NUMBER);
		serializeV2(state, out);
		return out.getCopyOfBuffer();
	}

	@Override
	public FileWriterState<WriterID> deserialize(int version, byte[] serialized) throws IOException {
		final DataInputDeserializer in = new DataInputDeserializer(serialized);

		switch (version) {
			case 1:
				validateMagicNumber(in);
				return deserializeV1(in);
			case 2:
				validateMagicNumber(in);
				return deserializeV2(in);
			default:
				throw new IOException("Unrecognized version or corrupt state: " + version);
		}
	}

	private void serializeV2(FileWriterState<WriterID> state, DataOutputView dataOutputView) throws IOException {
		SimpleVersionedSerialization.writeVersionAndSerialize(bucketIdSerializer, state.getBucketID(), dataOutputView);
		dataOutputView.writeUTF(state.getBucketPath().toString());
		dataOutputView.writeLong(state.getInProgressFileCreationTime());

		// put the current open part file
		if (state.hasInProgressFileRecoverable()) {
			final InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable = state.getInProgressFileRecoverable();
			dataOutputView.writeBoolean(true);
			SimpleVersionedSerialization.writeVersionAndSerialize(inProgressFileRecoverableSerializer, inProgressFileRecoverable, dataOutputView);
		} else {
			dataOutputView.writeBoolean(false);
		}
	}

	private FileWriterState<WriterID> deserializeV1(DataInputView in) throws IOException {
		final SimpleVersionedSerializer<RecoverableWriter.ResumeRecoverable> resumableSerializer = getResumableSerializer();

		final WriterID bucketId = SimpleVersionedSerialization.readVersionAndDeSerialize(bucketIdSerializer, in);
		final String bucketPathStr = in.readUTF();
		final long creationTime = in.readLong();

		InProgressFileWriter.InProgressFileRecoverable current = null;
		if (in.readBoolean()) {
			current = new OutputStreamBasedPartFileWriter.OutputStreamBasedInProgressFileRecoverable(
					SimpleVersionedSerialization.readVersionAndDeSerialize(resumableSerializer, in));
		}

		return new FileWriterState<>(
				bucketId,
				new Path(bucketPathStr),
				creationTime,
				current);
	}

	private FileWriterState<WriterID> deserializeV2(DataInputView dataInputView) throws IOException {
		final WriterID bucketId = SimpleVersionedSerialization.readVersionAndDeSerialize(bucketIdSerializer, dataInputView);
		final String bucketPathStr = dataInputView.readUTF();
		final long creationTime = dataInputView.readLong();

		// then get the current resumable stream
		InProgressFileWriter.InProgressFileRecoverable current = null;
		if (dataInputView.readBoolean()) {
			current = SimpleVersionedSerialization.readVersionAndDeSerialize(inProgressFileRecoverableSerializer, dataInputView);
		}

		return new FileWriterState<>(bucketId, new Path(bucketPathStr), creationTime, current);
	}

	private SimpleVersionedSerializer<RecoverableWriter.ResumeRecoverable> getResumableSerializer() {
		final OutputStreamBasedPartFileWriter.OutputStreamBasedInProgressFileRecoverableSerializer
				outputStreamBasedInProgressFileRecoverableSerializer =
				(OutputStreamBasedPartFileWriter.OutputStreamBasedInProgressFileRecoverableSerializer) inProgressFileRecoverableSerializer;
		return outputStreamBasedInProgressFileRecoverableSerializer.getResumeSerializer();
	}

	private static void validateMagicNumber(DataInputView in) throws IOException {
		final int magicNumber = in.readInt();
		if (magicNumber != MAGIC_NUMBER) {
			throw new IOException(String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
		}
	}
}
