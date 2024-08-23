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

package org.apache.hadoop.ozone.shell.checknative;

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.utils.NativeLibraryNotLoadedException;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRawSSTFileReader;
import org.apache.hadoop.io.erasurecode.ErasureCodeNative;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * CLI command to check if native libraries are loaded.
 */
@CommandLine.Command(name = "ozone checknative",
    description = "Checks if native libraries are loaded")
public class CheckNative extends GenericCli {

  private static final Logger LOG =
      LoggerFactory.getLogger(CheckNative.class);

  public static void main(String[] argv) {
    new CheckNative().run(argv);
  }

  @Override
  public Void call() throws Exception {
    boolean nativeHadoopLoaded = org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded();
    String hadoopLibraryName = "";
    String isalDetail = "";
    boolean isalLoaded = false;
    if (nativeHadoopLoaded) {
      hadoopLibraryName = org.apache.hadoop.util.NativeCodeLoader.getLibraryName();

      isalDetail = ErasureCodeNative.getLoadingFailureReason();
      if (isalDetail != null) {
        isalLoaded = false;
      } else {
        isalDetail = ErasureCodeNative.getLibraryName();
        isalLoaded = true;
      }
    }
    System.out.println("Native library checking:");
    System.out.printf("hadoop:  %b %s%n", nativeHadoopLoaded,
        hadoopLibraryName);
    System.out.printf("ISA-L:   %b %s%n", isalLoaded, isalDetail);

    // Attempt to load the rocks tools lib
    try {
      ManagedRawSSTFileReader.loadLibrary();
    } catch (NativeLibraryNotLoadedException e) {
      LOG.debug("Failed to load rocks-tools library", e);
    }

    boolean nativeRocksToolsLoaded = org.apache.hadoop.hdds.utils.NativeLibraryLoader.isLibraryLoaded();
    String rocksToolsDetail = "";
    if (nativeRocksToolsLoaded) {
      rocksToolsDetail = org.apache.hadoop.hdds.utils.NativeLibraryLoader.getJniLibraryFileName();
    }
    System.out.printf("rocks-tools: %b %s%n", nativeRocksToolsLoaded, rocksToolsDetail);
    return null;
  }
}
