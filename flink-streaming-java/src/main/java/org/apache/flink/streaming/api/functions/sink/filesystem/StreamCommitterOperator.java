package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class StreamCommitterOperator<IN extends Committable>
		extends AbstractStreamOperator<Void>
		implements OneInputStreamOperator<IN, Void> {

	private static final ListStateDescriptor<byte[]> COMMITTER_STATE_DESC =
			new ListStateDescriptor<>("committer-state", BytePrimitiveArraySerializer.INSTANCE);

	private final Committer<IN> committer;

	// TODO: 10.08.20 is this serializable???
	private final CommitterStateSerializer<IN> committableSerializer;

	private List<IN> committablesForCurrentCheckpoint;

	private ListState<byte[]> serializedCommitterState;

	private transient CommitterState<IN> committerState;

	public StreamCommitterOperator(
			final Committer<IN> committer,
			final SimpleVersionedSerializer<IN> committableSerializer) {
		this.committer = checkNotNull(committer);
		this.committableSerializer = new CommitterStateSerializer<>(committableSerializer);
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
		this.serializedCommitterState = context
				.getOperatorStateStore()
				.getListState(COMMITTER_STATE_DESC);
		this.committerState = initializeCommitterState(context);
	}

	private CommitterState<IN> initializeCommitterState(StateInitializationContext context) throws Exception {
		final CommitterState<IN> committerState = new CommitterState<>();
		if (context.isRestored()) {
			for (byte[] state : serializedCommitterState.get()) {
				final CommitterState<IN> restoredState = SimpleVersionedSerialization
						.readVersionAndDeSerialize(committableSerializer, state);
				committerState.merge(restoredState);
			}
		}
		return committerState;
	}

	@Override
	public void open() throws Exception {
		super.open();
		this.committablesForCurrentCheckpoint = new ArrayList<>();
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		committablesForCurrentCheckpoint.add(element.getValue());
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		super.snapshotState(context);
		prepareCommitterState(context);
		snapshotCommitterState();
	}

	private void prepareCommitterState(StateSnapshotContext context) {
		final long checkpointId = context.getCheckpointId();
		this.committerState.put(checkpointId, committablesForCurrentCheckpoint);
		this.committablesForCurrentCheckpoint = new ArrayList<>();
	}

	private void snapshotCommitterState() throws Exception {
		this.serializedCommitterState.clear();
		final byte[] serializedState = SimpleVersionedSerialization
				.writeVersionAndSerialize(committableSerializer, committerState);
		this.serializedCommitterState.add(serializedState);
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		super.notifyCheckpointComplete(checkpointId);
		committerState.consumeUpTo(checkpointId, committer::commit);
	}
}
