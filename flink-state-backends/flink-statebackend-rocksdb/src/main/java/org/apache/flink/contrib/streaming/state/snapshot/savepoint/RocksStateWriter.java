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

package org.apache.flink.contrib.streaming.state.snapshot.savepoint;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksIteratorWrapper;
import org.apache.flink.contrib.streaming.state.iterator.RocksStatesPerKeyGroupMergeIterator;
import org.apache.flink.runtime.state.KeyGroupStateIterator;
import org.apache.flink.util.IOUtils;

import org.rocksdb.ReadOptions;
import org.rocksdb.Snapshot;

import java.util.List;
import java.util.function.Function;

/**
 * Javadoc.
 */
@Internal
public class RocksStateWriter implements AutoCloseable {

	private final int keyGroupPrefixBytes;

	private final List<Tuple2<RocksIteratorWrapper, Integer>> kvStateIterators;

	private final ReadOptions readOptions;

	public RocksStateWriter(
			final Snapshot snapshot,
			final int keyGroupPrefixBytes,
			final Function<ReadOptions, List<Tuple2<RocksIteratorWrapper, Integer>>> kvStateIteratorsProvider) { // TODO: 12.01.21 for this we can pass a supplier-with-options
		this.keyGroupPrefixBytes = keyGroupPrefixBytes;

		this.readOptions = new ReadOptions();
		this.readOptions.setSnapshot(snapshot);
		this.kvStateIterators = kvStateIteratorsProvider.apply(readOptions);
	}

	public KeyGroupStateIterator getStateIterator() {
		return new RocksStatesPerKeyGroupMergeIterator(kvStateIterators, keyGroupPrefixBytes);
	}

	@Override
	public void close() {
		if (kvStateIterators != null) {
			for (Tuple2<RocksIteratorWrapper, Integer> kvStateIterator : kvStateIterators) {
				IOUtils.closeQuietly(kvStateIterator.f0);
			}
		}

		IOUtils.closeQuietly(readOptions);
	}
}
