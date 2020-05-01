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

package org.apache.flink.formats.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.staging.StagingCommitter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.io.IOException;

/**
 *
 */
public class HadoopStagingFileCommitter implements HadoopFileCommitter {

	private final Configuration configuration;

	private final Path path;

	private final Job job;

	private final TaskAttemptContext taskAttemptContext;

	private final StagingCommitter stagingCommitter;

	public HadoopStagingFileCommitter(Configuration configuration, Path path) throws IOException {
		this.configuration = new Configuration(configuration);
		configuration.set(CommitConstants.CREATE_SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, "false");
		configuration.set(CommitConstants.FS_S3A_COMMITTER_STAGING_UNIQUE_FILENAMES, "false");

		this.path = path;

		job = Job.getInstance(configuration);

		taskAttemptContext = new TaskAttemptContextImpl(
			job.getConfiguration(),
			new TaskAttemptID(path.getName(), 0, TaskType.REDUCE, 0, 0));

		this.stagingCommitter = new StagingCommitter(path.getParent(), taskAttemptContext);
	}

	@Override
	public Path getPath() {
		return path;
	}

	@Override
	public Path getInProgressFilePath() {
		return new Path(
			stagingCommitter.getTaskAttemptPath(taskAttemptContext),
			path.getName());
	}

	@Override
	public void preCommit() throws IOException {
		stagingCommitter.commitTask(taskAttemptContext);
	}

	@Override
	public void commit() throws IOException {
		stagingCommitter.commitJob(job);
	}

	@Override
	public void commitAfterRecovery() throws IOException {
		stagingCommitter.commitJob(job);
	}
}
