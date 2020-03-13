/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.application;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherInitializer;
import org.apache.flink.runtime.dispatcher.runner.SessionDispatcherLeaderProcess;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.rpc.FatalErrorHandler;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Process which encapsulates the job recovery logic and life cycle management of a
 * {@link Dispatcher} for an application cluster.
 */
@Internal
public class ApplicationDispatcherLeaderProcess extends SessionDispatcherLeaderProcess {

	private final ApplicationRunner applicationRunner;

	protected ApplicationDispatcherLeaderProcess(
			UUID leaderSessionId,
			DispatcherGatewayServiceFactory dispatcherGatewayServiceFactory,
			JobGraphStore jobGraphStore,
			Executor ioExecutor,
			FatalErrorHandler fatalErrorHandler,
			ApplicationRunner applicationRunner) {
		super(leaderSessionId, dispatcherGatewayServiceFactory, jobGraphStore, ioExecutor, fatalErrorHandler);
		this.applicationRunner = checkNotNull(applicationRunner);
	}

	@Override
	protected DispatcherInitializer getDispatcherInitializer(Collection<JobGraph> recoveredJobGraphs) {
		return new ApplicationDispatcherInitializer(recoveredJobGraphs, applicationRunner);
	}

	// ---------------------------------------------------------------
	// Factory methods
	// ---------------------------------------------------------------

	public static ApplicationDispatcherLeaderProcess create(
			UUID leaderSessionId,
			DispatcherGatewayServiceFactory dispatcherFactory,
			JobGraphStore jobGraphStore,
			Executor ioExecutor,
			FatalErrorHandler fatalErrorHandler,
			ApplicationRunner applicationRunner) {
		return new ApplicationDispatcherLeaderProcess(
				leaderSessionId,
				dispatcherFactory,
				jobGraphStore,
				ioExecutor,
				fatalErrorHandler,
				applicationRunner);
	}
}
