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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.lang.reflect.Constructor;

/**
 *
 */
public class DefaultHadoopFileCommitterFactory implements HadoopFileCommitterFactory {

	private static final String FS_S3A_COMMITTER_NAME = "fs.s3a.committer.name";

	private static final String COMMITTER_NAME_FILE = "file";

	private static final String COMMITTER_NAME_MAGIC = "magic";

	private static final String COMMITTER_NAME_STAGING = "staging";

	@Override
	public HadoopFileCommitter create(Configuration configuration, Path path) throws IOException {
		FileSystem fileSystem = FileSystem.get(path.toUri(), configuration);

		if (fileSystem.getScheme().equalsIgnoreCase("s3a")) {
			// Try load s3a related committers with reflection
			String committerName = configuration.getTrimmed(FS_S3A_COMMITTER_NAME);

			if (committerName == null || committerName.equalsIgnoreCase(COMMITTER_NAME_FILE)) {
				return new HadoopRenameFileCommitter(configuration, path);
			}

			String commitClassName = null;

			switch (committerName) {
				case COMMITTER_NAME_MAGIC:
					commitClassName = "org.apache.flink.formats.hive.HadoopMagicFileCommitter";
					break;
				case COMMITTER_NAME_STAGING:
					commitClassName = "org.apache.flink.formats.hive.HadoopStagingFileCommitter";
					break;
				default:
					throw new IOException(String.format(
						"Unsupported s3 committer configuration in %s. Only %s, %s and %s are supported",
						FS_S3A_COMMITTER_NAME,
						COMMITTER_NAME_FILE,
						COMMITTER_NAME_MAGIC,
						COMMITTER_NAME_STAGING));
			}

			try {
				Class<? extends HadoopFileCommitter> committerCls =
					(Class<? extends HadoopFileCommitter>) Class.forName(commitClassName);
				Constructor<? extends HadoopFileCommitter> constructor = committerCls.getConstructor(
					Configuration.class,
					Path.class);
				return constructor.newInstance(configuration, path);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(
					String.format("Could not load the specified class %s for %s committer", commitClassName, committerName),
					e);
			} catch (Exception e) {
				throw new RuntimeException(
					String.format("Failed to construct the committer class %s for %s", commitClassName, committerName),
					e);
			}
		} else {
			return new HadoopRenameFileCommitter(configuration, path);
		}
	}
}
