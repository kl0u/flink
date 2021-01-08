package org.apache.flink.contrib.streaming.state.snapshot.savepoint;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksIteratorWrapper;
import org.apache.flink.contrib.streaming.state.iterator.RocksStatesPerKeyGroupMergeIterator;
import org.apache.flink.contrib.streaming.state.iterator.RocksTransformingIteratorWrapper;
import org.apache.flink.contrib.streaming.state.snapshot.RocksDBSnapshotStrategyBase;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.AbstractSnapshotStrategy;
import org.apache.flink.runtime.state.AsyncSnapshotCallable;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.ResourceGuard;
import org.apache.flink.util.function.SupplierWithException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class RocksSavepointStrategyNew<K> extends AbstractSnapshotStrategy<KeyedStateHandle> implements CheckpointListener {

	private static final String DESCRIPTION = "Asynchronous full RocksDB snapshot";

	/** This decorator is used to apply compression per key-group for the written snapshot data. */
	private final StreamCompressionDecorator keyGroupCompressionDecorator;

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBSnapshotStrategyBase.class);

	/** The key serializer of the backend. */
	private final TypeSerializer<K> keySerializer;

	/** The key-group range for the task. */
	private final KeyGroupRange keyGroupRange;

	/** Number of bytes in the key-group prefix. */
	private final int keyGroupPrefixBytes;

	/** The configuration for local recovery. */
	private final LocalRecoveryConfig localRecoveryConfig;

	/** A {@link CloseableRegistry} that will be closed when the task is cancelled. */
	private final CloseableRegistry cancelStreamRegistry;

	private final RocksResources resources;

	public RocksSavepointStrategyNew(
			RocksResources resources,
			TypeSerializer<K> keySerializer,
			KeyGroupRange keyGroupRange,
			int keyGroupPrefixBytes,
			LocalRecoveryConfig localRecoveryConfig,
			CloseableRegistry cancelStreamRegistry,
			StreamCompressionDecorator keyGroupCompressionDecorator) {
		super(DESCRIPTION);
		this.resources = checkNotNull(resources);
		this.keySerializer = checkNotNull(keySerializer);
		this.keyGroupRange = checkNotNull(keyGroupRange);
		this.keyGroupPrefixBytes = keyGroupPrefixBytes;
		this.localRecoveryConfig = checkNotNull(localRecoveryConfig);
		this.cancelStreamRegistry = checkNotNull(cancelStreamRegistry);
		this.keyGroupCompressionDecorator = checkNotNull(keyGroupCompressionDecorator);
	}

	@Override
	public final RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
			long checkpointId,
			long timestamp,
			CheckpointStreamFactory streamFactory,
			CheckpointOptions checkpointOptions)
			throws Exception {

		if (resources.statesToSavepoint() == 0) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(
						"Asynchronous RocksDB snapshot performed on empty keyed state at {}. Returning null.",
						timestamp);
			}
			return DoneFuture.of(SnapshotResult.empty());
		} else {
			return doSnapshot(streamFactory);
		}
	}

	private RunnableFuture<SnapshotResult<KeyedStateHandle>> doSnapshot(CheckpointStreamFactory primaryStreamFactory) throws Exception {
		final SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier = () ->
				CheckpointStreamWithResultProvider.createSimpleStream(CheckpointedStateScope.EXCLUSIVE, primaryStreamFactory);

		// TODO: 07.01.21 maybe optimize to iterate once over the lists
		final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = resources.getMetadataSnapshots();

		final ResourceGuard.Lease lease = resources.acquireResource();
		final Snapshot snapshot = resources.getSnapshot();

		final List<RocksDBKeyedStateBackend.RocksDbKvStateInfo> metaDataCopy = resources.getKvStateInfoCopies();
		final List<MetaData> metaData = fillMetaData(metaDataCopy);

		final SnapshotAsynchronousPartCallable asyncSnapshotCallable =
				new SnapshotAsynchronousPartCallable(
						checkpointStreamSupplier,
						lease,
						snapshot,
						stateMetaInfoSnapshots,
						metaData,
						primaryStreamFactory.toString());

		return asyncSnapshotCallable.toAsyncSnapshotFutureTask(cancelStreamRegistry);
	}

	private static List<MetaData> fillMetaData(List<RocksDBKeyedStateBackend.RocksDbKvStateInfo> metaDataCopy) {
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

	@Override
	public void notifyCheckpointComplete(long checkpointId) {
		// nothing to do.
	}

	@Override
	public void notifyCheckpointAborted(long checkpointId) {
		// nothing to do.
	}

	/** Encapsulates the process to perform a full snapshot of a RocksDBKeyedStateBackend. */
	@VisibleForTesting
	private class SnapshotAsynchronousPartCallable
			extends AsyncSnapshotCallable<SnapshotResult<KeyedStateHandle>> {

		/** Supplier for the stream into which we write the snapshot. */
		private final SupplierWithException<CheckpointStreamWithResultProvider, Exception>
				checkpointStreamSupplier;

		/** This lease protects the native RocksDB resources. */
		private final ResourceGuard.Lease dbLease;

		/** RocksDB snapshot. */
		private final Snapshot snapshot;

		private List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;

		private final List<MetaData> metaData;

		private final String logPathString;

		SnapshotAsynchronousPartCallable(
				SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier,
				ResourceGuard.Lease dbLease,
				Snapshot snapshot,
				List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
				List<MetaData> metaData,
				String logPathString) {
			this.checkpointStreamSupplier = checkpointStreamSupplier;
			this.dbLease = dbLease;
			this.snapshot = snapshot;
			this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
			this.metaData = checkNotNull(metaData);
			this.logPathString = logPathString;
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
			resources.releaseSnapshot();
			IOUtils.closeQuietly(snapshot);
			IOUtils.closeQuietly(dbLease);
		}

		@Override
		protected void logAsyncSnapshotComplete(long startTime) {
			logAsyncCompleted(logPathString, startTime);
		}

		private void writeSnapshotToOutputStream(
				CheckpointStreamWithResultProvider checkpointStreamWithResultProvider,
				KeyGroupRangeOffsets keyGroupRangeOffsets) throws IOException, InterruptedException {

			final DataOutputView outputView =
					new DataOutputViewStreamWrapper(
							checkpointStreamWithResultProvider.getCheckpointOutputStream());
			final ReadOptions readOptions = new ReadOptions();

			List<Tuple2<RocksIteratorWrapper, Integer>> kvStateIterators = null;
			try {
				readOptions.setSnapshot(snapshot);

				kvStateIterators = getKVStateIterators(readOptions);
				writeKVStateMetaData(outputView);
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

		private List<Tuple2<RocksIteratorWrapper, Integer>> getKVStateIterators(final ReadOptions readOptions) {
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
				kvStateIterators.add(Tuple2.of(rocksIteratorWrapper, kvStateId)); // TODO: 05.01.21 we fill it here
				++kvStateId;
			}
			return kvStateIterators;
		}

		private void writeKVStateMetaData(final DataOutputView outputView) throws IOException {
			KeyedBackendSerializationProxy<K> serializationProxy =
					new KeyedBackendSerializationProxy<>(
							// TODO: this code assumes that writing a serializer is threadsafe, we
							// should support to
							// get a serialized form already at state registration time in the
							// future
							keySerializer,
							stateMetaInfoSnapshots,
							!Objects.equals(
									UncompressedStreamCompressionDecorator.INSTANCE,
									keyGroupCompressionDecorator));

			serializationProxy.write(outputView);
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
				try (RocksStatesPerKeyGroupMergeIterator mergeIterator =
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

	@SuppressWarnings("unchecked")
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
