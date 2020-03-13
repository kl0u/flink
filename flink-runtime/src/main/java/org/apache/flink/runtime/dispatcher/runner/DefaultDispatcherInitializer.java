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

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.jobgraph.JobGraph;

import java.util.Collection;

import static java.util.Objects.requireNonNull;

/**
 * Javadoc.
 */
@Internal
public class DefaultDispatcherInitializer implements DispatcherInitializer {

	private final Collection<JobGraph> recoveredJobs;

	public DefaultDispatcherInitializer(final Collection<JobGraph> recoveredJobs) {
		this.recoveredJobs = requireNonNull(recoveredJobs);
	}

	@Override
	public Collection<JobGraph> getInitJobGraphs() {
		return recoveredJobs;
	}

	@Override
	public void bootstrap(Dispatcher dispatcher) {
		requireNonNull(dispatcher);

		for (JobGraph recoveredJob : recoveredJobs) {
			FutureUtils.assertNoException(dispatcher.runJob(recoveredJob)
					.handle(dispatcher.handleRecoveredJobStartError(recoveredJob.getJobID())));
		}
		recoveredJobs.clear();
	}
}
