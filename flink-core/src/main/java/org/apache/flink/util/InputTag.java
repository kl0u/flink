/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import java.io.Serializable;
import java.util.Objects;

/**
 * Javadoc.
 */
public class InputTag implements Serializable {

	private static final long serialVersionUID = 1L;

	// ------------------------------ DEFAULT INPUT TAGS ------------------------------

	public static final InputTag MAIN_INPUT_TAG = InputTag.systemTagWithId("MAIN_INPUT");

	public static final InputTag LEGACY_SECOND_INPUT_TAG = InputTag.systemTagWithId("SECOND_INPUT");

	// ------------------------------                    ------------------------------

	private static final String SYSTEM_TAG_PREFIX = "s-";

	private final String id;


	private InputTag(final String id) {
		Preconditions.checkArgument(id != null && !id.isEmpty());
		this.id = id;
	}

	public String getId() {
		return id;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		return Objects.equals(id, ((InputTag) o).id);
	}

	@Override
	public int hashCode() {
		return id.hashCode();
	}

	@Override
	public String toString() {
		return "InputTag{" + "id='" + id + '}';
	}

	// ------------------------------ Factory Methods and Helpers ------------------------------

	public static InputTag withId(final String tagId) {
		return new InputTag(checkTagValidity(tagId));
	}

	static InputTag systemTagWithId(final String tagId) {
		return new InputTag(SYSTEM_TAG_PREFIX + tagId);
	}

	private static String checkTagValidity(final String tagId) {
		if (tagId == null || tagId.isEmpty() || tagId.startsWith(SYSTEM_TAG_PREFIX)) {
			throw new IllegalArgumentException("Invalid tag id: " + tagId);
		}
		return tagId;
	}
}
