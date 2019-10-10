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
package org.apache.hadoop.fs.obs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

import static org.apache.hadoop.fs.obs.Constants.*;

/** Filesystem input policy. */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public enum OBSInputPolicy {
  Normal(INPUT_FADV_NORMAL),
  Sequential(INPUT_FADV_SEQUENTIAL),
  Random(INPUT_FADV_RANDOM);

  private static final Logger LOG = LoggerFactory.getLogger(OBSInputPolicy.class);
  private final String policy;

  OBSInputPolicy(String policy) {
    this.policy = policy;
  }

  /**
   * Choose an FS access policy. Always returns something, primarily by downgrading to "normal" if
   * there is no other match.
   *
   * @param name strategy name from a configuration option, etc.
   * @return the chosen strategy
   */
  public static OBSInputPolicy getPolicy(String name) {
    String trimmed = name.trim().toLowerCase(Locale.ENGLISH);
    switch (trimmed) {
      case INPUT_FADV_NORMAL:
        return Normal;
      case INPUT_FADV_RANDOM:
        return Random;
      case INPUT_FADV_SEQUENTIAL:
        return Sequential;
      default:
        LOG.warn("Unrecognized " + INPUT_FADVISE + " value: \"{}\"", trimmed);
        return Normal;
    }
  }

  @Override
  public String toString() {
    return policy;
  }
}
