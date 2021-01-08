package org.apache.flink.contrib.streaming.state.snapshot.savepoint;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksIteratorWrapper;
import org.apache.flink.contrib.streaming.state.iterator.RocksStatesPerKeyGroupMergeIterator;
import org.apache.flink.contrib.streaming.state.iterator.RocksTransformingIteratorWrapper;
import org.apache.flink.runtime.state.KeyGroupStateIterator;
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

public class RocksResources {

	/** RocksDB instance from the backend. */
	private final RocksDB db;

	/** Resource guard for the RocksDB instance. */
	private final ResourceGuard rocksDBResourceGuard;

	/** Key/Value state meta info from the backend. */
	private final LinkedHashMap<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStateInformation;

	private final int keyGroupPrefixBytes;

	private ResourceGuard.Lease dbLease;

	private Snapshot snapshot;

	public RocksResources(
			final RocksDB db,
			final ResourceGuard rocksDBResourceGuard,
			final LinkedHashMap<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStateInformation,
			final int keyGroupPrefixBytes) {
		this.db = checkNotNull(db);
		this.rocksDBResourceGuard = checkNotNull(rocksDBResourceGuard);
		this.kvStateInformation = checkNotNull(kvStateInformation);
		this.keyGroupPrefixBytes = keyGroupPrefixBytes;
	}

	public int statesToSavepoint() {
		return kvStateInformation.size();
	}

	public List<RocksDBKeyedStateBackend.RocksDbKvStateInfo> getKvStateInfoCopies() {
		return new ArrayList<>(kvStateInformation.values());
	}

	public Snapshot getSnapshot() throws IOException {
		if (snapshot == null) {
			dbLease = rocksDBResourceGuard.acquireResource();
			snapshot = db.getSnapshot();
		}
		return snapshot;
	}

	public void cleanup() {
		if (snapshot != null) {
			db.releaseSnapshot(snapshot);
			IOUtils.closeQuietly(snapshot);
			IOUtils.closeQuietly(dbLease);
		}
		snapshot = null;
	}

	public List<StateMetaInfoSnapshot> getMetadataSnapshots() {
		final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = new ArrayList<>(kvStateInformation.size());
		for (RocksDBKeyedStateBackend.RocksDbKvStateInfo stateInfo : kvStateInformation.values()) {
			stateMetaInfoSnapshots.add(stateInfo.metaInfo.snapshot());
		}
		return stateMetaInfoSnapshots;
	}

	public KeyGroupStateIterator getStateIterator() {
		return new RocksStatesPerKeyGroupMergeIterator(
				kvStateIterators, keyGroupPrefixBytes);
	}

	public List<Tuple2<RocksIteratorWrapper, Integer>> getKVStateIterators(final ReadOptions readOptions) {
		final List<Tuple2<RocksIteratorWrapper, Integer>> kvStateIterators =
				new ArrayList<>(metaData.size());
		int kvStateId = 0;

		for (RocksSavepointStrategyNew.MetaData metaDataEntry : metaData) {
			RocksIteratorWrapper rocksIteratorWrapper =
					getRocksIterator(
							db,
							metaDataEntry.rocksDbKvStateInfo.columnFamilyHandle,
							metaDataEntry.stateSnapshotTransformer,
							readOptions);
			kvStateIterators.add(Tuple2.of(rocksIteratorWrapper, kvStateId)); // TODO: 05.01.21 we fill it here
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
}
