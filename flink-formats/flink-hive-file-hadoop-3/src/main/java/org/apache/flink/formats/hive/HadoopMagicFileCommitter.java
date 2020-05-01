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
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.fs.s3a.S3AUtils.deleteWithWarning;

/**
 *
 */
public class HadoopMagicFileCommitter implements HadoopFileCommitter {
	private static final Logger LOG = LoggerFactory.getLogger(HadoopMagicFileCommitter.class);

	private final Configuration configuration;

	private final Path path;

	private final Job job;

	private final TaskAttemptContext taskAttemptContext;

	private final MagicS3GuardCommitter magicCommitter;

	public HadoopMagicFileCommitter(Configuration configuration, Path path) throws IOException {
		this.configuration = new Configuration(configuration);
		configuration.set(CommitConstants.CREATE_SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, "false");

		this.path = path;

		job = Job.getInstance(configuration);

		taskAttemptContext = new TaskAttemptContextImpl(
			job.getConfiguration(),
			new TaskAttemptID(path.getName(), 0, TaskType.REDUCE, 0, 0));

		this.magicCommitter = new RenamableMagicCommitter(
			path.getName(),
			path.getParent(),
			taskAttemptContext);
	}

	@Override
	public Path getPath() {
		return path;
	}

	@Override
	public Path getInProgressFilePath() {
		return new Path(
			magicCommitter.getTaskAttemptPath(taskAttemptContext),
			path.getName());
	}

	@Override
	public void preCommit() throws IOException {
		magicCommitter.commitTask(taskAttemptContext);
	}

	@Override
	public void commit() throws IOException {
		magicCommitter.commitJob(job);
	}

	@Override
	public void commitAfterRecovery() throws IOException {
		magicCommitter.commitJob(job);
	}

	private static class RenamableMagicCommitter extends MagicS3GuardCommitter {
		private final String uniqueName;

		public RenamableMagicCommitter(
			String uniqueName,
			Path outputPath,
			TaskAttemptContext context) throws IOException {

			super(outputPath, context);
			this.uniqueName = uniqueName;
		}

		@Override
		protected Path getJobAttemptPath(int appAttemptId) {
			return new Path(getOutputPath(), "." + uniqueName);
		}

		@Override
		public void cleanupStagingDirs() {
			super.cleanupStagingDirs();

			Path jobAttemptPath = getJobAttemptPath(0);
			Invoker.ignoreIOExceptions(
				LOG,
				"cleanup magic job directory",
				jobAttemptPath.toString(),
				() -> deleteWithWarning(getDestFS(), jobAttemptPath, true));
		}
	}
}
