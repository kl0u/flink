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

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.dispatcher.DispatcherFactory;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.jobmanager.JobGraphStoreFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.dispatcher.runner.application.ApplicationSubmitterWithException;

import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc.
 */
@Internal
public class ApplicationDispatcherLeaderProcessFactoryFactory implements DispatcherLeaderProcessFactoryFactory {

	private final ApplicationSubmitterWithException<DispatcherGateway> applicationSubmitter;

	private final DispatcherFactory dispatcherFactory;

	private ApplicationDispatcherLeaderProcessFactoryFactory(
			final DispatcherFactory dispatcherFactory,
			final ApplicationSubmitterWithException<DispatcherGateway> applicationSubmitter) {
		this.dispatcherFactory = dispatcherFactory;
		this.applicationSubmitter = checkNotNull(applicationSubmitter);
	}

	@Override
	public DispatcherLeaderProcessFactory createFactory(
			JobGraphStoreFactory jobGraphStoreFactory,
			Executor ioExecutor,
			RpcService rpcService,
			PartialDispatcherServices partialDispatcherServices,
			FatalErrorHandler fatalErrorHandler) {
		final AbstractDispatcherLeaderProcess.DispatcherGatewayServiceFactory dispatcherGatewayServiceFactory = new DefaultDispatcherGatewayServiceFactory(
				dispatcherFactory,
				rpcService,
				partialDispatcherServices);

		return new ApplicationDispatcherLeaderProcessFactory(
				dispatcherGatewayServiceFactory,
				jobGraphStoreFactory,
				ioExecutor,
				fatalErrorHandler,
				applicationSubmitter);
	}

	public static ApplicationDispatcherLeaderProcessFactoryFactory create(
			final DispatcherFactory dispatcherFactory,
			final ApplicationSubmitterWithException<DispatcherGateway> applicationSubmitter) {
		return new ApplicationDispatcherLeaderProcessFactoryFactory(dispatcherFactory, applicationSubmitter);
	}
}
