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

package org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc.
 */
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
