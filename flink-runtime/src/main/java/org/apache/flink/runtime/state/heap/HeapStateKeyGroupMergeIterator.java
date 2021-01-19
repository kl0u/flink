/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupStateIterator;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateSnapshot;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc.
 */
@Internal
public class HeapStateKeyGroupMergeIterator implements KeyGroupStateIterator {

	private final KeyGroupRange keyGroupRange;
	private final Map<StateUID, Integer> stateNamesToId;
	private final Map<StateUID, StateSnapshot> cowStateStableSnapshots;

	private boolean isValid;
	private boolean newKeyGroup;
	private boolean newKVState;

	private int currentStateUIDindex;
	private final List<StateUID> statesToSavepoint;

	private int currentKeyGroupIndex;

	public HeapStateKeyGroupMergeIterator(
			final KeyGroupRange keyGroupRange,
			final Map<StateUID, Integer> stateNamesToId,
			final Map<StateUID, StateSnapshot> cowStateStableSnapshots) { // TODO: 14.01.21 I can reorganize the iterators as soon as I have sth working
		this.keyGroupRange = checkNotNull(keyGroupRange);
		this.stateNamesToId = checkNotNull(stateNamesToId);
		this.cowStateStableSnapshots = checkNotNull(cowStateStableSnapshots);

		this.isValid = !this.cowStateStableSnapshots.isEmpty();
		this.newKeyGroup = true;
		this.newKVState = true;

		this.statesToSavepoint = new ArrayList<>(cowStateStableSnapshots.keySet());
		this.currentKeyGroupIndex = 0;
		this.currentStateUIDindex = 0;
		this.currentState = statesToSavepoint.get(currentStateUIDindex);

		// TODO: 15.01.21 also set initial key and stuff here.
		final int keyGroupId = keyGroup();
		this.currentState = this.statesToSavepoint.get(currentStateUIDindex);
		this.currentStateIterator = this.cowStateStableSnapshots
				.get(currentState)
				.getKeyGroupWriter()
				.getIterator(keyGroupId);
	}

	@Override
	public boolean isValid() {
		return isValid;
	}

	@Override
	public boolean isNewKeyValueState() {
		return this.newKVState;
	}

	@Override
	public boolean isNewKeyGroup() {
		return this.newKeyGroup;
	}

	@Override
	public int keyGroup() {
		return keyGroupRange.getKeyGroupId(currentKeyGroupIndex);
	}

	@Override
	public int kvStateId() {
		return stateNamesToId.get(currentState);
	}

	private StateUID currentState;
	private StateSnapshot.StateKeyGroupWriter currentKeyGroupWriter;
	private Iterator<StateEntry> currentStateIterator;

	@Override
	public void next() {
		newKeyGroup = false;
		newKVState = false;

		if (currentStateIterator.hasNext()) {
			// set the values we need to set
		} else {
			c
		}
	}

	@Override
	public byte[] key() {
		return new byte[0];
	}

	@Override
	public byte[] value() {
		return new byte[0];
	}

	@Override
	public void close() {

	}
}
