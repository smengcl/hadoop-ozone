/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.shell.s3;

import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine;

import java.io.IOException;

/**
 * Ozone s3 user create
 */
@CommandLine.Command(name = "create",
    description = "Create a new S3 user")
public class UserCreateHandler extends S3Handler {
  /*
  @CommandLine.Option(names = "-e",
      description = "Print out variables together with 'export' prefix, to "
          + "use it from 'eval $(ozone s3 getsecret)'")
  private boolean export;
  */

  // TODO: Limit to security enabled only later?
/*
  @Override
  protected boolean isApplicable() {
    return securityEnabled();
  }
*/

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {
    /*
    String userName = UserGroupInformation.getCurrentUser().getUserName();

    final S3SecretValue secret = client.getObjectStore().getS3Secret(userName);
    if (export) {
      out().println("export AWS_ACCESS_KEY_ID=" + secret.getAwsAccessKey());
      out().println("export AWS_SECRET_ACCESS_KEY=" + secret.getAwsSecret());
    } else {
      out().println(secret);
    }
    */
  }
}
