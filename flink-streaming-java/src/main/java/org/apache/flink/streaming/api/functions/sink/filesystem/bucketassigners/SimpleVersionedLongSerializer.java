package org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class SimpleVersionedLongSerializer implements SimpleVersionedSerializer<Long> {

	@Override
	public int getVersion() {
		return 0;
	}

	@Override
	public byte[] serialize(Long value) {
		checkNotNull(value);
		final byte[] targetBytes = new byte[Long.BYTES];

		final ByteBuffer bb = ByteBuffer.wrap(targetBytes).order(ByteOrder.LITTLE_ENDIAN);
		bb.putLong(value);
		return targetBytes;
	}

	@Override
	public Long deserialize(int version, byte[] serialized) throws IOException {
		switch (version) {
			case 0:
				return deserializeV0(serialized);
			default:
				throw new IOException("Unrecognized version or corrupt state: " + version);
		}
	}

	private Long deserializeV0(byte[] serialized) {
		final ByteBuffer bb = ByteBuffer.wrap(serialized).order(ByteOrder.LITTLE_ENDIAN);
		return bb.getLong();
	}
}
