package org.apache.flink.contrib.streaming.state.snapshot;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksIteratorWrapper;
import org.apache.flink.contrib.streaming.state.iterator.RocksStatesPerKeyGroupMergeIterator;
import org.apache.flink.contrib.streaming.state.iterator.RocksTransformingIteratorWrapper;
import org.apache.flink.runtime.state.KeyGroupStateIterator;
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

public class RocksSavepointResources implements AutoCloseable {

    private final RocksDB db;

    private final ResourceGuard.Lease dbLease;

    private final Snapshot snapshot;

    private final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;

    private final List<MetaData> metaData;

    private ReadOptions readOptions;

    private List<Tuple2<RocksIteratorWrapper, Integer>> kvStateIterators;

    public RocksSavepointResources(
            final RocksDB db,
            final ResourceGuard rocksDBResourceGuard,
            final LinkedHashMap<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo>
                    kvStateInformation)
            throws IOException {

        this.db = checkNotNull(db);
        this.dbLease = checkNotNull(rocksDBResourceGuard).acquireResource();
        this.snapshot = db.getSnapshot();

        this.stateMetaInfoSnapshots = new ArrayList<>(kvStateInformation.size());

        final List<RocksDBKeyedStateBackend.RocksDbKvStateInfo> metaDataCopy =
                new ArrayList<>(kvStateInformation.size());
        for (RocksDBKeyedStateBackend.RocksDbKvStateInfo stateInfo : kvStateInformation.values()) {
            stateMetaInfoSnapshots.add(stateInfo.metaInfo.snapshot());
            metaDataCopy.add(stateInfo);
        }
        this.metaData = fillMetaData(metaDataCopy);
    }

    public List<StateMetaInfoSnapshot> getStateMetaInfoSnapshots() {
        return stateMetaInfoSnapshots;
    }

    /**
     * This should always be called within a {@code try-with-resources} block so that resources that
     * are allocated here are removed by the {@link #close()}.
     */
    public KeyGroupStateIterator getStateIterator() {
        readOptions = new ReadOptions();
        readOptions.setSnapshot(snapshot);
        kvStateIterators = getStateIterators(readOptions);
        return new RocksStatesPerKeyGroupMergeIterator(kvStateIterators, keyGroupPrefixBytes);
    }

    private List<Tuple2<RocksIteratorWrapper, Integer>> getStateIterators(
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
            kvStateIterators.add(Tuple2.of(rocksIteratorWrapper, kvStateId));
            ++kvStateId;
        }
        return kvStateIterators;
    }

    @Override
    public void close() {
        if (kvStateIterators != null) {
            for (Tuple2<RocksIteratorWrapper, Integer> kvStateIterator : kvStateIterators) {
                IOUtils.closeQuietly(kvStateIterator.f0);
            }
        }

        if (readOptions != null) {
            IOUtils.closeQuietly(readOptions);
        }
    }

    // TODO: 07.01.21 checked
    public void cleanup() {
        db.releaseSnapshot(snapshot);
        IOUtils.closeQuietly(snapshot);
        IOUtils.closeQuietly(dbLease);
    }

    private static List<MetaData> fillMetaData(
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

    private static class MetaData {
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
