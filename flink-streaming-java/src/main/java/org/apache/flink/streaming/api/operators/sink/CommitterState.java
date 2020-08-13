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

import org.apache.flink.util.function.ConsumerWithException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * Javadoc.
 */
public class CommitterState<S> {

	private final NavigableMap<Long, List<S>> committablesPerCheckpoint;

	public CommitterState() {
		this.committablesPerCheckpoint = new TreeMap<>();
	}

	public void put(long checkpointId, S committable) {
		final List<S> registered = committablesPerCheckpoint
				.computeIfAbsent(checkpointId, k -> new ArrayList<>());
		registered.add(committable);
	}

	public void put(long checkpointId, List<S> committables) {
		final List<S> registered = committablesPerCheckpoint.get(checkpointId);
		if (registered == null) {
			committablesPerCheckpoint.put(checkpointId, committables);
		} else {
			registered.addAll(committables);
		}
	}

	Set<Map.Entry<Long, List<S>>> entrySet() {
		return committablesPerCheckpoint.entrySet();
	}

	public void consumeUpTo(long checkpointId, ConsumerWithException<S, Exception> consumer) throws Exception {
		final Iterator<Map.Entry<Long, List<S>>> it = committablesPerCheckpoint
				.headMap(checkpointId, true)
				.entrySet()
				.iterator();

		while (it.hasNext()) {
			final Map.Entry<Long, List<S>> entry = it.next();
			for (S pendingFileRecoverable : entry.getValue()) {
				consumer.accept(pendingFileRecoverable);
			}
			it.remove();
		}
	}

	public void merge(CommitterState<S> committerState) {
		for (Map.Entry<Long, List<S>> entry : committerState.committablesPerCheckpoint.entrySet()) {
			final long checkpointId = entry.getKey();
			final List<S> committables = entry.getValue();
			put(checkpointId, committables);
		}
	}
}
