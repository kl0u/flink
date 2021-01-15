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
import org.apache.flink.runtime.state.StateSnapshot;

import java.util.Map;
import java.util.Set;

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

	private final Set<StateUID> statesToSavepoint;
//	private Iterator<Map.Entry<StateUID, StateSnapshot>> keyGroupIterator;

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

		this.currentKeyGroupIndex = 0;
		this.statesToSavepoint = cowStateStableSnapshots.keySet();
//		this.keyGroupIterator = cowStateStableSnapshots.entrySet().iterator(); // TODO: 14.01.21 think when to re-initialize this.
	}

//	for (int keyGroupPos = 0;   keyGroupPos < keyGroupRange.getNumberOfKeyGroups(); ++keyGroupPos)
//	{
//		int keyGroupId = keyGroupRange.getKeyGroupId(keyGroupPos);
//		keyGroupRangeOffsets[keyGroupPos] = localStream.getPos();
//		outView.writeInt(keyGroupId);
//
//		for (Map.Entry<StateUID, StateSnapshot> stateSnapshot :
//				cowStateStableSnapshots.entrySet()) {
//
//			StateSnapshot.StateKeyGroupWriter partitionedSnapshot =
//					stateSnapshot.getValue().getKeyGroupWriter();
//			try (OutputStream kgCompressionOut =
//						 keyGroupCompressionDecorator.decorateWithCompression(
//								 localStream)) {
//				DataOutputViewStreamWrapper kgCompressionView =
//						new DataOutputViewStreamWrapper(kgCompressionOut);
//				kgCompressionView.writeShort(
//						stateNamesToId.get(stateSnapshot.getKey()));
//				partitionedSnapshot.writeStateInKeyGroup(
//						kgCompressionView, keyGroupId);
//			} // this will just close the outer compression stream
//		}
//	}

//	newKeyGroup = false;
//	newKVState = false;
//
//	final RocksIteratorWrapper rocksIterator = currentSubIterator.getIterator();
//		rocksIterator.next();
//
//	byte[] oldKey = currentSubIterator.getCurrentKey();
//		if (rocksIterator.isValid()) {
//
//		currentSubIterator.setCurrentKey(rocksIterator.key());
//
//		if (isDifferentKeyGroup(oldKey, currentSubIterator.getCurrentKey())) {
//			heap.offer(currentSubIterator);
//			currentSubIterator = heap.remove();
//			newKVState = currentSubIterator.getIterator() != rocksIterator;
//			detectNewKeyGroup(oldKey);
//		}
//	} else {
//		IOUtils.closeQuietly(rocksIterator);
//
//		if (heap.isEmpty()) {
//			currentSubIterator = null;
//			valid = false;
//		} else {
//			currentSubIterator = heap.remove();
//			newKVState = true;
//			detectNewKeyGroup(oldKey);
//		}
//	}

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
		return stateNamesToId.get(); // stateSnapshot.getKey()
	}


	private StateUID currentState;
	private StateSnapshot.StateKeyGroupWriter currentKeyGroupWriter;

	@Override
	public void next() {
		newKeyGroup = false;
		newKVState = false;

		final int currentKeyGroup = keyGroup();
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
