package org.apache.flink.contrib.streaming.state.snapshot;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksIteratorWrapper;
import org.apache.flink.contrib.streaming.state.iterator.RocksStatesPerKeyGroupMergeIterator;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.AsyncSnapshotCallable;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupStateIterator;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SavepointStrategy;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.ResourceGuard;
import org.apache.flink.util.function.SupplierWithException;

import org.rocksdb.ReadOptions;
import org.rocksdb.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.RunnableFuture;

import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.END_OF_KEY_GROUP_MARK;
import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.hasMetaDataFollowsFlag;
import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.setMetaDataFollowsFlagInKey;
import static org.apache.flink.util.Preconditions.checkNotNull;

// TODO: 05.01.21 the state handle may change in order to differentiate between savepoint and
// checkpoint.
public class RocksSavepointStrategy<K>
        implements SavepointStrategy<SnapshotResult<KeyedStateHandle>>, CheckpointListener {

    private static final Logger LOG = LoggerFactory.getLogger(RocksSavepointStrategy.class);

    private final TypeSerializer<K> keySerializer;

    private final KeyGroupRange keyGroupRange;

    private final StreamCompressionDecorator keyGroupCompressionDecorator;

    private final RocksSavepointResources resources;

    public RocksSavepointStrategy(
            final KeyGroupRange keyGroupRange,
            final TypeSerializer<K> keySerializer,
            final RocksSavepointResources resources,
            final StreamCompressionDecorator keyGroupCompressionDecorator) {
        this.keySerializer = checkNotNull(keySerializer);
        this.keyGroupRange = checkNotNull(keyGroupRange);
        this.keyGroupCompressionDecorator = checkNotNull(keyGroupCompressionDecorator);
        this.resources = checkNotNull(resources);
    }

    @Nonnull
    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> takeSavepoint(
            final long checkpointId,
            final long timestamp,
            @Nonnull final CheckpointStreamFactory streamFactory,
            @Nonnull final CheckpointOptions checkpointOptions)
            throws Exception {

        if (kvStateInformation.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Asynchronous RocksDB snapshot performed on empty keyed state at {}. Returning null.",
                        timestamp);
            }
            return DoneFuture.of(SnapshotResult.empty());
        } else {
            return doSnapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
        }
    }

    private RunnableFuture<SnapshotResult<KeyedStateHandle>> doSnapshot(
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory primaryStreamFactory,
            @Nonnull CheckpointOptions checkpointOptions)
            throws Exception {

        final SupplierWithException<CheckpointStreamWithResultProvider, Exception>
                checkpointStreamSupplier =
                        () ->
                                CheckpointStreamWithResultProvider.createSimpleStream(
                                        CheckpointedStateScope.EXCLUSIVE, primaryStreamFactory);

        final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots =
                new ArrayList<>(kvStateInformation.size());
        final List<RocksDBKeyedStateBackend.RocksDbKvStateInfo> metaDataCopy =
                new ArrayList<>(kvStateInformation.size());

        for (RocksDBKeyedStateBackend.RocksDbKvStateInfo stateInfo : kvStateInformation.values()) {
            // snapshot meta info
            stateMetaInfoSnapshots.add(stateInfo.metaInfo.snapshot());
            metaDataCopy.add(stateInfo);
        }

        final ResourceGuard.Lease lease = rocksDBResourceGuard.acquireResource();
        final Snapshot snapshot = db.getSnapshot();

        final SnapshotAsynchronousPartCallable asyncSnapshotCallable =
                new SnapshotAsynchronousPartCallable(
                        checkpointStreamSupplier,
                        lease,
                        snapshot,
                        stateMetaInfoSnapshots,
                        metaDataCopy,
                        primaryStreamFactory.toString());

        return asyncSnapshotCallable.toAsyncSnapshotFutureTask(cancelStreamRegistry);
    }

    @Override
    public void notifyCheckpointComplete(final long checkpointId) throws Exception {
        // do nothing for now.
    }

    @Override
    public void notifyCheckpointAborted(final long checkpointId) throws Exception {
        // do nothing for now.
    }

    private class SnapshotAsynchronousPartCallable
            extends AsyncSnapshotCallable<SnapshotResult<KeyedStateHandle>> {

        /** Supplier for the stream into which we write the snapshot. */
        private final SupplierWithException<CheckpointStreamWithResultProvider, Exception>
                checkpointStreamSupplier;

        private final String logPathString;

        private final RocksSavepointResources rocksSavepointResources;

        SnapshotAsynchronousPartCallable(
                SupplierWithException<CheckpointStreamWithResultProvider, Exception>
                        checkpointStreamSupplier,
                RocksSavepointResources resources,
                String logPathString) {

            this.checkpointStreamSupplier = checkpointStreamSupplier;
            this.logPathString = logPathString;
            this.rocksSavepointResources = checkNotNull(resources);
        }

        @Override
        protected SnapshotResult<KeyedStateHandle> callInternal() throws Exception {
            final KeyGroupRangeOffsets keyGroupRangeOffsets =
                    new KeyGroupRangeOffsets(keyGroupRange);
            final CheckpointStreamWithResultProvider checkpointStreamWithResultProvider =
                    checkpointStreamSupplier.get();

            snapshotCloseableRegistry.registerCloseable(checkpointStreamWithResultProvider);
            writeSnapshotToOutputStream(checkpointStreamWithResultProvider, keyGroupRangeOffsets);

            if (snapshotCloseableRegistry.unregisterCloseable(checkpointStreamWithResultProvider)) {
                return CheckpointStreamWithResultProvider.toKeyedStateHandleSnapshotResult(
                        checkpointStreamWithResultProvider.closeAndFinalizeCheckpointStreamResult(),
                        keyGroupRangeOffsets);
            } else {
                throw new IOException("Stream is already unregistered/closed.");
            }
        }

        @Override
        protected void cleanupProvidedResources() {
            resources.cleanup();
        }

        @Override
        protected void logAsyncSnapshotComplete(long startTime) {
            logAsyncCompleted(logPathString, startTime);
        }

        private void writeSnapshotToOutputStreamNew(
                final CheckpointStreamWithResultProvider checkpointStreamWithResultProvider,
                final KeyGroupRangeOffsets keyGroupRangeOffsets)
                throws IOException, InterruptedException {

            final DataOutputView outputView =
                    new DataOutputViewStreamWrapper(
                            checkpointStreamWithResultProvider.getCheckpointOutputStream());
        }

        private void writeSnapshotToOutputStream(
                final CheckpointStreamWithResultProvider checkpointStreamWithResultProvider,
                final KeyGroupRangeOffsets keyGroupRangeOffsets)
                throws IOException, InterruptedException {

            final DataOutputView outputView =
                    new DataOutputViewStreamWrapper(
                            checkpointStreamWithResultProvider.getCheckpointOutputStream());
            final ReadOptions readOptions = new ReadOptions();
            List<Tuple2<RocksIteratorWrapper, Integer>> kvStateIterators = null;
            try {
                writeKVStateMetaData(outputView);

                // TODO: 05.01.21 this can move to being an argument in the constructor...
                readOptions.setSnapshot(snapshot);
                kvStateIterators = getStateIterators(readOptions);
                writeKVStateData(
                        kvStateIterators, checkpointStreamWithResultProvider, keyGroupRangeOffsets);
            } finally {

                if (kvStateIterators != null) {
                    for (Tuple2<RocksIteratorWrapper, Integer> kvStateIterator : kvStateIterators) {
                        IOUtils.closeQuietly(kvStateIterator.f0);
                    }
                }

                IOUtils.closeQuietly(readOptions);
            }
        }

        private void writeKVStateMetaData(final DataOutputView outputView) throws IOException {
            KeyedBackendSerializationProxy<K> serializationProxy =
                    new KeyedBackendSerializationProxy<>(

                            // TODO: this code assumes that writing a serializer is threadsafe, we
                            // should support tocget a serialized form already at state registration
                            // time in the future

                            keySerializer,
                            rocksSavepointResources.getStateMetaInfoSnapshots(),
                            !Objects.equals(
                                    UncompressedStreamCompressionDecorator.INSTANCE,
                                    keyGroupCompressionDecorator));

            serializationProxy.write(outputView);
        }

        private List<Tuple2<RocksIteratorWrapper, Integer>> getStateIterators(
                ReadOptions readOptions) {
            final List<Tuple2<RocksIteratorWrapper, Integer>> kvStateIterators =
                    new ArrayList<>(metaData.size());

            int kvStateId = 0;

            for (RocksFullSnapshotStrategy.MetaData metaDataEntry : metaData) {
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

        private void writeKVStateData(
                final List<Tuple2<RocksIteratorWrapper, Integer>> kvStateIterators,
                final CheckpointStreamWithResultProvider checkpointStreamWithResultProvider,
                final KeyGroupRangeOffsets keyGroupRangeOffsets)
                throws IOException, InterruptedException {

            byte[] previousKey = null;
            byte[] previousValue = null;
            DataOutputView kgOutView = null;
            OutputStream kgOutStream = null;
            CheckpointStreamFactory.CheckpointStateOutputStream checkpointOutputStream =
                    checkpointStreamWithResultProvider.getCheckpointOutputStream();

            try {
                // Here we transfer ownership of RocksIterators to the
                // RocksStatesPerKeyGroupMergeIterator
                try (KeyGroupStateIterator mergeIterator =
                        new RocksStatesPerKeyGroupMergeIterator(
                                kvStateIterators, keyGroupPrefixBytes)) {

                    // preamble: setup with first key-group as our lookahead
                    if (mergeIterator.isValid()) {
                        // begin first key-group by recording the offset
                        keyGroupRangeOffsets.setKeyGroupOffset(
                                mergeIterator.keyGroup(), checkpointOutputStream.getPos());
                        // write the k/v-state id as metadata
                        kgOutStream =
                                keyGroupCompressionDecorator.decorateWithCompression(
                                        checkpointOutputStream);
                        kgOutView = new DataOutputViewStreamWrapper(kgOutStream);
                        // TODO this could be aware of keyGroupPrefixBytes and write only one byte
                        // if possible
                        kgOutView.writeShort(mergeIterator.kvStateId());
                        previousKey = mergeIterator.key();
                        previousValue = mergeIterator.value();
                        mergeIterator.next();
                    }

                    // main loop: write k/v pairs ordered by (key-group, kv-state), thereby tracking
                    // key-group offsets.
                    while (mergeIterator.isValid()) {

                        assert (!hasMetaDataFollowsFlag(previousKey));

                        // set signal in first key byte that meta data will follow in the stream
                        // after this k/v pair
                        if (mergeIterator.isNewKeyGroup() || mergeIterator.isNewKeyValueState()) {

                            // be cooperative and check for interruption from time to time in the
                            // hot loop
                            checkInterrupted();

                            setMetaDataFollowsFlagInKey(previousKey);
                        }

                        writeKeyValuePair(previousKey, previousValue, kgOutView);

                        // write meta data if we have to
                        if (mergeIterator.isNewKeyGroup()) {
                            // TODO this could be aware of keyGroupPrefixBytes and write only one
                            // byte if possible
                            kgOutView.writeShort(END_OF_KEY_GROUP_MARK);
                            // this will just close the outer stream
                            kgOutStream.close();
                            // begin new key-group
                            keyGroupRangeOffsets.setKeyGroupOffset(
                                    mergeIterator.keyGroup(), checkpointOutputStream.getPos());
                            // write the kev-state
                            // TODO this could be aware of keyGroupPrefixBytes and write only one
                            // byte if possible
                            kgOutStream =
                                    keyGroupCompressionDecorator.decorateWithCompression(
                                            checkpointOutputStream);
                            kgOutView = new DataOutputViewStreamWrapper(kgOutStream);
                            kgOutView.writeShort(mergeIterator.kvStateId());
                        } else if (mergeIterator.isNewKeyValueState()) {
                            // write the k/v-state
                            // TODO this could be aware of keyGroupPrefixBytes and write only one
                            // byte if possible
                            kgOutView.writeShort(mergeIterator.kvStateId());
                        }

                        // request next k/v pair
                        previousKey = mergeIterator.key();
                        previousValue = mergeIterator.value();
                        mergeIterator.next();
                    }
                }

                // epilogue: write last key-group
                if (previousKey != null) {
                    assert (!hasMetaDataFollowsFlag(previousKey));
                    setMetaDataFollowsFlagInKey(previousKey);
                    writeKeyValuePair(previousKey, previousValue, kgOutView);
                    // TODO this could be aware of keyGroupPrefixBytes and write only one byte if
                    // possible
                    kgOutView.writeShort(END_OF_KEY_GROUP_MARK);
                    // this will just close the outer stream
                    kgOutStream.close();
                    kgOutStream = null;
                }

            } finally {
                // this will just close the outer stream
                IOUtils.closeQuietly(kgOutStream);
            }
        }

        private void writeKeyValuePair(byte[] key, byte[] value, DataOutputView out)
                throws IOException {
            BytePrimitiveArraySerializer.INSTANCE.serialize(key, out);
            BytePrimitiveArraySerializer.INSTANCE.serialize(value, out);
        }

        private void checkInterrupted() throws InterruptedException {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("RocksDB snapshot interrupted.");
            }
        }
    }
}
