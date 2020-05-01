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
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 *
 */
public class HadoopRenameFileCommitter implements HadoopFileCommitter {
	private final Configuration configuration;
	private final Path path;
	private final Path inProgressFilePath;

	public HadoopRenameFileCommitter(Configuration configuration, Path path) {
		this.configuration = configuration;
		this.path = path;
		this.inProgressFilePath = generateInProgressFilePath();
	}

	@Override
	public Path getPath() {
		return path;
	}

	@Override
	public Path getInProgressFilePath() {
		return inProgressFilePath;
	}

	@Override
	public void preCommit() {
		// Do nothing.
	}

	@Override
	public void commit() throws IOException {
		FileSystem fileSystem = FileSystem.get(path.toUri(), configuration);

		try {
			fileSystem.rename(inProgressFilePath, path);
		} catch (IOException e) {
			throw new IOException(
				String.format("Could not commit file from %s to %s", inProgressFilePath, path),
				e);
		}
	}

	@Override
	public void commitAfterRecovery() throws IOException {
		FileSystem fileSystem = FileSystem.get(path.toUri(), configuration);

		if (fileSystem.exists(inProgressFilePath)) {
			try {
				// If file exists, it will be overwritten.
				fileSystem.rename(inProgressFilePath, path);
			} catch (IOException e) {
				throw new IOException(
					String.format("Could not commit file from %s to %s", inProgressFilePath, path),
					e);
			}
		}
	}

	private Path generateInProgressFilePath() {
		checkArgument(path.isAbsolute(), "Target file must be absolute");

		Path parent = path.getParent();
		String name = path.getName();

		return new Path(parent, "." + name + ".inprogress");
	}
}
