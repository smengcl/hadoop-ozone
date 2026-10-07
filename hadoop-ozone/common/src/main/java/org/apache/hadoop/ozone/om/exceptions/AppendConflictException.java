/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.exceptions;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendConflictInfo;

/**
 * Append admission was rejected because the file already has a writer. The conflict info tells the caller what kind
 * of writer it is and, for an append session, when it becomes eligible for lease recovery.
 */
public class AppendConflictException extends OMException {
  private final transient AppendConflictInfo conflict;

  public AppendConflictException(String message, AppendConflictInfo conflict) {
    super(message, ResultCodes.APPEND_WRITER_CONFLICT);
    this.conflict = conflict;
  }

  public AppendConflictInfo getConflict() {
    return conflict;
  }
}
