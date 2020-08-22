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

package org.apache.flink.streaming.api.operators.sink;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeServiceAware;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc.
 */
public class BatchSinkOperatorFactory extends AbstractStreamOperatorFactory<Object>
		implements CoordinatedOperatorFactory<Object>, ProcessingTimeServiceAware {

	private static final long serialVersionUID = 1L;

	private final Sink<?, ?, ?, ?> sink;

	public BatchSinkOperatorFactory(final Sink<?, ?, ?, ?> sink) {
		this.sink = checkNotNull(sink);
	}

	@Override
	public OperatorCoordinator.Provider getCoordinatorProvider(String operatorName, OperatorID operatorID) {
		return new SinkCoordinatorProvider<>(operatorName, operatorID, sink);
	}

	@Override
	public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
		return BatchSinkOperator.class;
	}

	@Override
	public <T extends StreamOperator<Object>> T createStreamOperator(StreamOperatorParameters<Object> parameters) {
		final OperatorID operatorId = parameters.getStreamConfig().getOperatorID();
		final OperatorEventGateway gateway = parameters.getOperatorEventDispatcher().getOperatorEventGateway(operatorId);
		final ProcessingTimeService timeService = parameters.getProcessingTimeService();

		final BatchSinkOperator<?, ?, ?, ?> sinkOperator = new BatchSinkOperator<>(sink, gateway, timeService);
		sinkOperator.setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
		return (T) sinkOperator;
	}
}
