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

package org.apache.flink.runtime.client;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This exception is the base exception for all exceptions that signals the failure of an
 * application with a given {@link ApplicationStatus}.
 */
public class JobExecutionResultException extends JobExecutionException {

	private final ApplicationStatus status;

	public JobExecutionResultException(
			final JobID jobID,
			final ApplicationStatus status,
			final String message,
			final Throwable cause) {
		super(jobID, message, cause);
		this.status = checkNotNull(status);
	}

	public ApplicationStatus getStatus() {
		return status;
	}
}
