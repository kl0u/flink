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

import org.apache.flink.api.connector.sink.WriterOutput;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;

import java.io.IOException;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc.
 */
public class OutputToCoordinator<Committable> implements WriterOutput<Committable> {

	private final OperatorEventGateway coordinator;

	private final SimpleVersionedSerializer<Committable> serializer;

	public OutputToCoordinator(
			final OperatorEventGateway coordinator,
			final SimpleVersionedSerializer<Committable> serializer) {
		this.coordinator = checkNotNull(coordinator);
		this.serializer = checkNotNull(serializer);
	}

	@Override
	public void sendToCommit(final Committable committable) throws IOException {
		if (committable == null) {
			return;
		}

		coordinator.sendEventToCoordinator(
				new SinkOperatorEvent<>(
						Collections.singletonList(committable),
						serializer));
	}
}
