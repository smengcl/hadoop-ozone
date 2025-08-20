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

package org.apache.hadoop.ozone.utils;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Test for the AutoCompletion utility.
 */
public class TestAutoCompletion {

  @Test
  public void testBashCompletionIncludesAllFirstLevelCommands() {
    // Capture the output of bash completion
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    System.setOut(new PrintStream(baos));
    
    try {
      AutoCompletion autoCompletion = new AutoCompletion();
      CommandLine commandLine = new CommandLine(autoCompletion);
      commandLine.execute("bash");
      
      String completionScript = baos.toString();
      
      // Verify that all first-level commands from ozone script are included
      String[] expectedCommands = {
          "classpath", "datanode", "envvars", "daemonlog", "freon", "fs",
          "om", "scm", "s3g", "httpfs", "csi", "recon", "insight", "version",
          "dtutil", "genconf", "getconf", "completion", "repair", "ratis",
          "debug", "s3", "admin", "tenant", "sh"
      };
      
      for (String command : expectedCommands) {
        assertTrue(completionScript.contains(command), 
            "Completion script should include command: " + command);
      }
      
      // Verify the script is not empty and contains expected structure
      assertFalse(completionScript.isEmpty(), "Completion script should not be empty");
      assertTrue(completionScript.contains("function _complete_ozone"), 
          "Completion script should contain main completion function");
      assertTrue(completionScript.contains("#!/usr/bin/env bash"), 
          "Completion script should be a bash script");
      
    } finally {
      System.setOut(originalOut);
    }
  }

  @Test
  public void testZshCompletion() {
    // Capture the output of zsh completion
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    System.setOut(new PrintStream(baos));
    
    try {
      AutoCompletion autoCompletion = new AutoCompletion();
      CommandLine commandLine = new CommandLine(autoCompletion);
      commandLine.execute("zsh");
      
      String completionScript = baos.toString();
      
      // Verify zsh completion works (currently delegates to bash)
      assertFalse(completionScript.isEmpty(), "ZSH completion script should not be empty");
      assertTrue(completionScript.contains("#!/usr/bin/env bash"), 
          "ZSH completion currently delegates to bash");
      
    } finally {
      System.setOut(originalOut);
    }
  }

  @Test
  public void testAutoCompletionHelp() {
    // Test that help works
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    System.setOut(new PrintStream(baos));
    
    try {
      AutoCompletion autoCompletion = new AutoCompletion();
      CommandLine commandLine = new CommandLine(autoCompletion);
      commandLine.execute("--help");
      
      String helpOutput = baos.toString();
      
      assertTrue(helpOutput.contains("Generate autocompletion script"), 
          "Help should contain description");
      assertTrue(helpOutput.contains("Commands:"), 
          "Help should list subcommands");
      assertTrue(helpOutput.contains("bash"), 
          "Help should mention bash subcommand");
      assertTrue(helpOutput.contains("zsh"), 
          "Help should mention zsh subcommand");
      
    } finally {
      System.setOut(originalOut);
    }
  }
}