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
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksIteratorWrapper;
import org.apache.flink.contrib.streaming.state.iterator.RocksTransformingIteratorWrapper;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.ResourceGuard;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc.
 */
@Internal
public class RocksResources {

	/**
	 * RocksDB instance from the backend.
	 */
	private final RocksDB db;

	/**
	 * Resource guard for the RocksDB instance.
	 */
	private final ResourceGuard rocksDBResourceGuard;

	/**
	 * Key/Value state meta info from the backend.
	 */
	private final LinkedHashMap<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo>
			kvStateInformation;

	private final int keyGroupPrefixBytes;

	private List<MetaData> metaData;

	private ResourceGuard.Lease dbLease;

	private List<StateMetaInfoSnapshot> metadataSnapshots;

	private Snapshot snapshot;

	public RocksResources(
			final RocksDB db,
			final ResourceGuard rocksDBResourceGuard,
			final LinkedHashMap<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo>
					kvStateInformation,
			final int keyGroupPrefixBytes) {
		this.db = checkNotNull(db);
		this.rocksDBResourceGuard = checkNotNull(rocksDBResourceGuard);
		this.kvStateInformation = checkNotNull(kvStateInformation);
		this.keyGroupPrefixBytes = keyGroupPrefixBytes;
	}

	public void init() throws IOException {
		this.metadataSnapshots = getMetadataSnapshotsHelper();
		this.metaData = fillMetaData(new ArrayList<>(kvStateInformation.values()));
		this.dbLease = rocksDBResourceGuard.acquireResource();
		this.snapshot = db.getSnapshot();
	}

	public List<StateMetaInfoSnapshot> getMetadataSnapshots() {
		return metadataSnapshots;
	}

	public int statesToSavepoint() {
		return kvStateInformation.size();
	}

	public void cleanup() {
		if (snapshot != null) {
			db.releaseSnapshot(snapshot);
			IOUtils.closeQuietly(snapshot);
			IOUtils.closeQuietly(dbLease);
		}
		snapshot = null;
	}

	public RocksStateWriter getStateWriter() {
		return new RocksStateWriter(snapshot, keyGroupPrefixBytes, this::getKVStateIterators);
	}

	private List<StateMetaInfoSnapshot> getMetadataSnapshotsHelper() {
		final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots =
				new ArrayList<>(kvStateInformation.size());
		for (RocksDBKeyedStateBackend.RocksDbKvStateInfo stateInfo : kvStateInformation.values()) {
			stateMetaInfoSnapshots.add(stateInfo.metaInfo.snapshot());
		}
		return stateMetaInfoSnapshots;
	}

	private List<MetaData> fillMetaData(
			List<RocksDBKeyedStateBackend.RocksDbKvStateInfo> metaDataCopy) {
		List<MetaData> metaData = new ArrayList<>(metaDataCopy.size());
		for (RocksDBKeyedStateBackend.RocksDbKvStateInfo rocksDbKvStateInfo : metaDataCopy) {
			StateSnapshotTransformer<byte[]> stateSnapshotTransformer = null;
			if (rocksDbKvStateInfo.metaInfo instanceof RegisteredKeyValueStateBackendMetaInfo) {
				stateSnapshotTransformer =
						((RegisteredKeyValueStateBackendMetaInfo<?, ?>) rocksDbKvStateInfo.metaInfo)
								.getStateSnapshotTransformFactory()
								.createForSerializedState()
								.orElse(null);
			}
			metaData.add(new MetaData(rocksDbKvStateInfo, stateSnapshotTransformer));
		}
		return metaData;
	}

	private List<Tuple2<RocksIteratorWrapper, Integer>> getKVStateIterators(
			final ReadOptions readOptions) {
		final List<Tuple2<RocksIteratorWrapper, Integer>> kvStateIterators =
				new ArrayList<>(metaData.size());
		int kvStateId = 0;

		for (MetaData metaDataEntry : metaData) {
			RocksIteratorWrapper rocksIteratorWrapper =
					getRocksIterator(
							db,
							metaDataEntry.rocksDbKvStateInfo.columnFamilyHandle,
							metaDataEntry.stateSnapshotTransformer,
							readOptions);
			kvStateIterators.add(
					Tuple2.of(rocksIteratorWrapper, kvStateId)); // TODO: 05.01.21 we fill it here
			++kvStateId;
		}
		return kvStateIterators;
	}

	private static RocksIteratorWrapper getRocksIterator(
			RocksDB db,
			ColumnFamilyHandle columnFamilyHandle,
			StateSnapshotTransformer<byte[]> stateSnapshotTransformer,
			ReadOptions readOptions) {
		RocksIterator rocksIterator = db.newIterator(columnFamilyHandle, readOptions);
		return stateSnapshotTransformer == null
				? new RocksIteratorWrapper(rocksIterator)
				: new RocksTransformingIteratorWrapper(rocksIterator, stateSnapshotTransformer);
	}

	static class MetaData {
		final RocksDBKeyedStateBackend.RocksDbKvStateInfo rocksDbKvStateInfo;
		final StateSnapshotTransformer<byte[]> stateSnapshotTransformer;

		private MetaData(
				RocksDBKeyedStateBackend.RocksDbKvStateInfo rocksDbKvStateInfo,
				StateSnapshotTransformer<byte[]> stateSnapshotTransformer) {

			this.rocksDbKvStateInfo = rocksDbKvStateInfo;
			this.stateSnapshotTransformer = stateSnapshotTransformer;
		}
	}
}
