package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.util.LinkedOptionalMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

public class CommitterState<S extends Committable> {

	private final NavigableMap<Long, List<S>> committablesPerCheckpoint;

	public CommitterState() {
		this.committablesPerCheckpoint = new TreeMap<>();
	}

	public void put(long checkpointId, S committable) {
		final List<S> registered = committablesPerCheckpoint
				.computeIfAbsent(checkpointId, k -> new ArrayList<>());
		registered.add(committable);
	}

	public void put(long checkpointId, List<S> committables) {
		final List<S> registered = committablesPerCheckpoint.get(checkpointId);
		if (registered == null) {
			committablesPerCheckpoint.put(checkpointId, committables);
		} else {
			registered.addAll(committables);
		}
	}

	Set<Map.Entry<Long, List<S>>> entrySet() {
		return committablesPerCheckpoint.entrySet();
	}

	public void consumeUpTo(long checkpointId, ConsumerWithException<S, IOException> consumer) throws IOException {
		final Iterator<Map.Entry<Long, List<S>>> it = committablesPerCheckpoint
				.headMap(checkpointId, true)
				.entrySet()
				.iterator();

		while (it.hasNext()) {
			final Map.Entry<Long, List<S>> entry = it.next();
			for (S pendingFileRecoverable : entry.getValue()) {
				consumer.accept(pendingFileRecoverable);
			}
			it.remove();
		}
	}

	public void merge(CommitterState<S> committerState) {
		for (Map.Entry<Long, List<S>> entry : committerState.committablesPerCheckpoint.entrySet()) {
			final long checkpointId = entry.getKey();
			final List<S> committables = entry.getValue();
			put(checkpointId, committables);
		}
	}
}
