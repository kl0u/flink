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
import org.apache.flink.runtime.operators.coordination.RecreateOnResetOperatorCoordinator;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc.
 */
public class SinkCoordinatorProvider<Committable> extends RecreateOnResetOperatorCoordinator.Provider {

	private final String operatorName;

	private final Sink<?, Committable, ?, ?> sink;

	public SinkCoordinatorProvider(
			final String operatorName,
			final OperatorID operatorID,
			final Sink<?, Committable, ?, ?> sink) {
		super(operatorID);
		this.operatorName = checkNotNull(operatorName);
		this.sink = checkNotNull(sink);
	}

	@Override
	protected OperatorCoordinator getCoordinator(OperatorCoordinator.Context context) {
		return new SinkCoordinator<>(operatorName, sink, context);
	}
}
