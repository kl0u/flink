package org.apache.flink.contrib.streaming.state.snapshot.savepoint;

import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.ResourceGuard;

import org.rocksdb.RocksDB;
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

	private Snapshot snapshot;
	public RocksResources(
			final RocksDB db,
			final ResourceGuard rocksDBResourceGuard,
			final LinkedHashMap<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStateInformation) {
		this.db = checkNotNull(db);
		this.rocksDBResourceGuard = checkNotNull(rocksDBResourceGuard);
		this.kvStateInformation = checkNotNull(kvStateInformation);
	}

	public int statesToSavepoint() {
		return kvStateInformation.size();
	}

	public List<RocksDBKeyedStateBackend.RocksDbKvStateInfo> getKvStateInfoCopies() {
		return new ArrayList<>(kvStateInformation.values());
	}

	public Snapshot getSnapshot() {
		snapshot = db.getSnapshot();
		return snapshot;
	}

	public void releaseSnapshot() {
		if (snapshot != null) {
			db.releaseSnapshot(snapshot);
		}
	}

	public ResourceGuard.Lease acquireResource() throws IOException {
		return rocksDBResourceGuard.acquireResource();
	}

	public List<StateMetaInfoSnapshot> getMetadataSnapshots() {
		final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = new ArrayList<>(kvStateInformation.size());
		for (RocksDBKeyedStateBackend.RocksDbKvStateInfo stateInfo : kvStateInformation.values()) {
			stateMetaInfoSnapshots.add(stateInfo.metaInfo.snapshot());
		}
		return stateMetaInfoSnapshots;
	}
}
