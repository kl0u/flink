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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.util.NoOpIntMap;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the blocking shuffles.
 */
public class MiniClusterBlockingShuffleITCase extends TestLogger {

	@Test
	public void testBlockingShuffleScheduling() throws Exception {

		final JobGraph jobGraph = getJobGraphUnderTest();
		runHandleJobsWhenNotEnoughSlots(jobGraph);
	}

	private JobGraph getJobGraphUnderTest() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.disableOperatorChaining();
		env.setParallelism(5); // TODO: 24.06.20 but if I set it to 5 it fails

//		List<Integer> input = new ArrayList<>();
//		input.add(1);
//		input.add(2);
//		input.add(3);
//		input.add(4);

		// TODO: 26.06.20 so now we have 15 independent regions with BLOCKING results but still the job cannot be executed with less than 5 slots.
//		env.fromCollection(input)
		env.continuousSource(new MockSource(Boundedness.BOUNDED, 2), WatermarkStrategy.noWatermarks(), "bounded")
//				.setParallelism(1)
				.slotSharingGroup("group1")
//				.keyBy(value -> value)
				.map(new NoOpIntMap())
				.addSink(new DiscardingSink<>())
				.slotSharingGroup("group2");

		final StreamGraph streamGraph = env.getStreamGraph();
		streamGraph.setGlobalDataExchangeMode(GlobalDataExchangeMode.ALL_EDGES_BLOCKING);
		streamGraph.setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES);

		final JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
		for (JobVertex jobVertex : jobGraph.getVerticesSortedTopologicallyFromSources()) {

			for (JobEdge e : jobVertex.getInputs()) {
				System.out.println("HERE: " + e.getSource().getProducer().getName());
			}

			for (IntermediateDataSet interm : jobVertex.getProducedDataSets()) {
				assertEquals(ResultPartitionType.BLOCKING, interm.getResultType());
			}
		}
		return jobGraph;
	}

	private void runHandleJobsWhenNotEnoughSlots(final JobGraph jobGraph) throws Exception {
		final Configuration configuration = getDefaultConfiguration();
		configuration.setLong(JobManagerOptions.SLOT_REQUEST_TIMEOUT, 100L);

		final MiniClusterConfiguration cfg = new MiniClusterConfiguration.Builder()
				.setNumTaskManagers(1)
				.setNumSlotsPerTaskManager(1)
				.setConfiguration(configuration)
				.build();

		try (final MiniCluster miniCluster = new MiniCluster(cfg)) {
			miniCluster.start();

			miniCluster.executeJobBlocking(jobGraph);
		}
	}

	private Configuration getDefaultConfiguration() {
		final Configuration configuration = new Configuration();
		configuration.setString(RestOptions.BIND_PORT, "0");

		return configuration;
	}

}
