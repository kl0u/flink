/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.functions.WithGracefulShutdown;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.core.testutils.MultiShotLatch;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A test verifying the termination process
 * (synchronous checkpoint and task termination) at the {@link SourceStreamTask}.
 */
public class SourceTaskTerminationTest {

	private static OneShotLatch ready;
	private static MultiShotLatch runLoopStart;
	private static MultiShotLatch runLoopEnd;

	private static AtomicReference<Throwable> error;

	@Before
	public void initialize() {
		ready = new OneShotLatch();
		runLoopStart = new MultiShotLatch();
		runLoopEnd = new MultiShotLatch();
		error = new AtomicReference<>();

		error.set(null);
	}

	@After
	public void validate() {
		validateNoExceptionsWereThrown();
	}

	@Test
	public void terminateShouldBlockDuringCheckpointingAndEmitMaxWatermark() throws Exception {
		stopWithSavepointStreamTaskTestHelper(true);
	}

	@Test
	public void suspendShouldBlockDuringCheckpointingAndNotEmitMaxWatermark() throws Exception {
		stopWithSavepointStreamTaskTestHelper(false);
	}

	private void stopWithSavepointStreamTaskTestHelper(final boolean expectMaxWatermark) throws Exception {
		final long syncSavepointId = 34L;

		final StreamTaskTestHarness<Long> srcTaskTestHarness = getSourceStreamTaskTestHarness(new LockStepSourceWithOneWmPerElement());
		final Thread executionThread = srcTaskTestHarness.invoke();
		final StreamTask<Long, ?> srcTask = srcTaskTestHarness.getTask();

		ready.await();

		// step by step let the source thread emit elements
		emitAndVerifyWatermarkAndElement(srcTaskTestHarness, 1L);
		emitAndVerifyWatermarkAndElement(srcTaskTestHarness, 2L);

		emitAndVerifyCheckpoint(srcTaskTestHarness, srcTask, 31L);

		emitAndVerifyWatermarkAndElement(srcTaskTestHarness, 3L);

		final Thread syncSavepointThread = triggerSynchronousSavepointFromDifferentThread(srcTask, expectMaxWatermark, syncSavepointId);

		final SynchronousSavepointLatch syncSavepointFuture = waitForSyncSavepointFutureToBeSet(srcTask);

		if (expectMaxWatermark) {
			// if we are in TERMINATE mode, we expect the source task
			// to emit MAX_WM before the SYNC_SAVEPOINT barrier.
			verifyWatermark(srcTaskTestHarness.getOutput(), Watermark.MAX_WATERMARK);
		}

		verifyCheckpointBarrier(srcTaskTestHarness.getOutput(), syncSavepointId);

		assertFalse(syncSavepointFuture.isCompleted());
		assertTrue(syncSavepointFuture.isWaiting());

		srcTask.notifyCheckpointComplete(syncSavepointId);
		assertTrue(syncSavepointFuture.isCompleted());

		syncSavepointThread.join();
		executionThread.join();
	}

	private static class LockStepSourceWithOneWmPerElement extends RichSourceFunction<Long> {

		private volatile boolean isRunning;

		@Override
		public void run(SourceContext<Long> ctx) throws Exception {
			long element = 1L;
			isRunning = true;

			ready.trigger();

			while (isRunning) {
				runLoopStart.await();
				ctx.emitWatermark(new Watermark(element));
				ctx.collect(element++);
				runLoopEnd.trigger();
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
			runLoopStart.trigger();
		}
	}

	// ------------------------------------		UDF Lifecycle Monitoring Tests	------------------------------------

	private final List<String> suspendOrderedFunctionCalls =
			Collections.unmodifiableList(Arrays.asList(
					"UDF::SNAPSHOT",
					"UDF::CHECKPOINT-NOTIFICATION",
					"UDF::CANCEL",
					"UDF::CLOSE"));

	private final List<String> drainOrderedFunctionCalls =
			Collections.unmodifiableList(Arrays.asList(
					"UDF::PREPARE-SHUTDOWN",
					"UDF::SNAPSHOT",
					"UDF::CHECKPOINT-NOTIFICATION",
					"UDF::CANCEL",
					"UDF::SHUTDOWN",
					"UDF::CLOSE"));

	private static final List<String> actualMethodCalls = new ArrayList<>();

	@Test
	public void suspendShouldNotCallGracefulShutdownMethods() throws Exception {
		stopWithSavepointUDFLifecycleMonitoringHelper(false);
		assertEquals(suspendOrderedFunctionCalls, actualMethodCalls);
	}

	@Test
	public void drainShouldCallGracefulShutdownMethods() throws Exception {
		stopWithSavepointUDFLifecycleMonitoringHelper(true);
		assertEquals(drainOrderedFunctionCalls, actualMethodCalls);
	}

	private void stopWithSavepointUDFLifecycleMonitoringHelper(final boolean expectMaxWatermark) throws Exception {
		actualMethodCalls.clear();

		final long syncSavepointId = 34L;

		final LifecycleMonitoringFunction srcFunction = new LifecycleMonitoringFunction();
		final StreamTaskTestHarness<Long> srcTaskTestHarness = getSourceStreamTaskTestHarness(srcFunction);
		final Thread executionThread = srcTaskTestHarness.invoke();
		final StreamTask<Long, ?> srcTask = srcTaskTestHarness.getTask();

		ready.await();

		final Thread syncSavepointThread = triggerSynchronousSavepointFromDifferentThread(srcTask, expectMaxWatermark, syncSavepointId);

		final SynchronousSavepointLatch syncSavepointFuture = waitForSyncSavepointFutureToBeSet(srcTask);

		if (expectMaxWatermark) {
			// if we are in TERMINATE mode, we expect the source task
			// to emit MAX_WM before the SYNC_SAVEPOINT barrier.
			verifyWatermark(srcTaskTestHarness.getOutput(), Watermark.MAX_WATERMARK);
		}

		verifyCheckpointBarrier(srcTaskTestHarness.getOutput(), syncSavepointId);

		assertFalse(syncSavepointFuture.isCompleted());
		assertTrue(syncSavepointFuture.isWaiting());

		srcTask.notifyCheckpointComplete(syncSavepointId);
		assertTrue(syncSavepointFuture.isCompleted());

		syncSavepointThread.join();
		executionThread.join();
	}

	private static class LifecycleMonitoringFunction extends RichSourceFunction<Long> implements WithGracefulShutdown, CheckpointedFunction, CheckpointListener {

		private volatile boolean isRunning;

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {

		}

		@Override
		public void run(SourceContext<Long> ctx) throws Exception {
			isRunning = true;
			ready.trigger();

			while (isRunning) {
				Thread.sleep(10L);
			}
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			actualMethodCalls.add("UDF::SNAPSHOT");
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			actualMethodCalls.add("UDF::CHECKPOINT-NOTIFICATION");
		}

		@Override
		public void prepareToShutdown() throws Exception {
			actualMethodCalls.add("UDF::PREPARE-SHUTDOWN");
		}

		@Override
		public void shutdown() throws Exception {
			actualMethodCalls.add("UDF::SHUTDOWN");
		}

		@Override
		public void close() throws Exception {
			super.close();
			actualMethodCalls.add("UDF::CLOSE");
			isRunning = false;
		}

		@Override
		public void cancel() {
			actualMethodCalls.add("UDF::CANCEL");
			isRunning = false;
		}
	}

	// ------------------------------------		Utilities	------------------------------------

	private void validateNoExceptionsWereThrown() {
		if (error.get() != null && !(error.get() instanceof CancelTaskException)) {
			fail(error.get().getMessage());
		}
	}

	private Thread triggerSynchronousSavepointFromDifferentThread(
			final StreamTask<Long, ?> task,
			final boolean advanceToEndOfEventTime,
			final long syncSavepointId) {
		final Thread checkpointingThread = new Thread(() -> {
			try {
				task.triggerCheckpoint(
						new CheckpointMetaData(syncSavepointId, 900),
						new CheckpointOptions(CheckpointType.SYNC_SAVEPOINT, CheckpointStorageLocationReference.getDefault()),
						advanceToEndOfEventTime);
			} catch (Exception e) {
				error.set(e);
			}
		});
		checkpointingThread.start();

		return checkpointingThread;

	}

	private void emitAndVerifyWatermarkAndElement(
			final StreamTaskTestHarness<Long> srcTaskTestHarness,
			final long expectedElement) throws InterruptedException {

		runLoopStart.trigger();
		verifyWatermark(srcTaskTestHarness.getOutput(), new Watermark(expectedElement));
		verifyNextElement(srcTaskTestHarness.getOutput(), expectedElement);
		runLoopEnd.await();
	}

	private void emitAndVerifyCheckpoint(
			final StreamTaskTestHarness<Long> srcTaskTestHarness,
			final StreamTask<Long, ?> srcTask,
			final long checkpointId) throws Exception {

		srcTask.triggerCheckpoint(
				new CheckpointMetaData(checkpointId, 900),
				CheckpointOptions.forCheckpointWithDefaultLocation(),
				false);
		verifyCheckpointBarrier(srcTaskTestHarness.getOutput(), checkpointId);
	}

	private StreamTaskTestHarness<Long> getSourceStreamTaskTestHarness(@Nonnull final RichSourceFunction<Long> source) {
		final StreamTaskTestHarness<Long> testHarness = new StreamTaskTestHarness<>(
				SourceStreamTask::new,
				BasicTypeInfo.LONG_TYPE_INFO);

		testHarness.setupOutputForSingletonOperatorChain();
		testHarness.getExecutionConfig().setLatencyTrackingInterval(-1);

		StreamConfig streamConfig = testHarness.getStreamConfig();
		StreamSource<Long, ?> sourceOperator = new StreamSource<>(source);
		streamConfig.setStreamOperator(sourceOperator);
		streamConfig.setOperatorID(new OperatorID());
		return testHarness;
	}

	private SynchronousSavepointLatch waitForSyncSavepointFutureToBeSet(final StreamTask streamTaskUnderTest) throws InterruptedException {
		final SynchronousSavepointLatch syncSavepointFuture = streamTaskUnderTest.getSynchronousSavepointLatch();
		while (!syncSavepointFuture.isWaiting()) {
			Thread.sleep(10L);

			validateNoExceptionsWereThrown();
		}
		return syncSavepointFuture;
	}

	private void verifyNextElement(BlockingQueue<Object> output, long expectedElement) throws InterruptedException {
		Object next = output.take();
		assertTrue("next element is not an event", next instanceof StreamRecord);
		assertEquals("wrong event", expectedElement, ((StreamRecord<Long>) next).getValue().longValue());
	}

	private void verifyWatermark(BlockingQueue<Object> output, Watermark expectedWatermark) throws InterruptedException {
		Object next = output.take();
		assertTrue("next element is not a watermark", next instanceof Watermark);
		assertEquals("wrong watermark", expectedWatermark, next);
	}

	private void verifyCheckpointBarrier(BlockingQueue<Object> output, long checkpointId) throws InterruptedException {
		Object next = output.take();
		assertTrue("next element is not a checkpoint barrier", next instanceof CheckpointBarrier);
		assertEquals("wrong checkpoint id", checkpointId, ((CheckpointBarrier) next).getId());
	}
}
