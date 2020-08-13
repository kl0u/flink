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

package org.apache.flink.streaming.api.operators.sink;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc.
 */
public class SinkOperatorEvent<Committable> implements OperatorEvent {

	private static final long serialVersionUID = 1L;

	private final int serializerVersion;

	private final List<byte[]> committables;

	public SinkOperatorEvent(
			final List<Committable> committables,
			final SimpleVersionedSerializer<Committable> serializer) throws IOException {
		checkNotNull(committables);
		checkNotNull(serializer);

		this.serializerVersion = serializer.getVersion();

		this.committables = new ArrayList<>(committables.size());
		for (Committable committable : committables) {
			this.committables.add(serializer.serialize(committable));
		}
	}

	public List<Committable> getCommittables(final SimpleVersionedSerializer<Committable> serializer) throws IOException {
		checkNotNull(serializer);

		final List<Committable> result = new ArrayList<>(committables.size());
		for (byte[] serializedCommittable : committables) {
			result.add(serializer.deserialize(serializerVersion, serializedCommittable));
		}
		return result;
	}

	@Override
	public String toString() {
		return String.format("SinkOperatorEvent[%s]", committables);
	}
}
