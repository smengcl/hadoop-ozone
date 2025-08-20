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

import java.util.Objects;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.ratis.util.ReflectionUtils;
import org.reflections.Reflections;
import picocli.AutoComplete;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/** Tool to generate auto-completion scripts for Ozone CLI. */
@Command(name = "ozone completion",
    header = "Generate autocompletion script for the specified shell.",
    synopsisHeading = "%nUsage: ",
    description = AutoCompletion.DESCRIPTION,
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
public final class AutoCompletion extends GenericCli {

  public static final String DESCRIPTION = "%nThe generated shell code must " +
      "be evaluated to provide interactive completion of ozone commands. " +
      "See each sub-command's help for details on how to use the generated script.%n";

  private static final String BASH_DESCRIPTION = "%nTo load completions in your current shell session: %n" +
      "%n\t source <(ozone completion bash) %n" +
      "%nTo load completions for every new session automatically, %n" +
      "add this to your `~/.bash_profile`: %n" +
      "%n\t eval \"$(ozone completion bash)\" %n";

  private static final String ZSH_DESCRIPTION = "%nTo load completions in your current shell session: %n" +
      "%n\t source <(ozone completion zsh)%n" +
      "%nTo load completions for every new session automatically, %n" +
      "generate a `_ozone` completion script and put it in your `$fpath`: %n" +
      "%n\t ozone completion zsh > \"$${fpath[1]}/_ozone\" %n";

  private static final String OZONE_COMMAND = "ozone ";
  private static final int PREFIX_LENGTH = OZONE_COMMAND.length();
  private static final String[] PACKAGES_TO_SCAN = {
      "org.apache.hadoop.hdds",
      "org.apache.hadoop.ozone",
      "org.apache.ozone",
  };

  /** Generates bash auto-completion script for Ozone CLI. */
  @Command(name = "bash",
      header = "Generate the shell completion code for bash.",
      synopsisHeading = "%nUsage: ",
      description = AutoCompletion.BASH_DESCRIPTION,
      versionProvider = HddsVersionProvider.class,
      mixinStandardHelpOptions = true)
  public void bashCompletion() {
    out().println(getBashCompletion());
  }

  /** Generates zsh auto-completion script for Ozone CLI. */
  @Command(name = "zsh",
      header = "Generate the shell completion code for zsh.",
      synopsisHeading = "%nUsage: ",
      description = AutoCompletion.ZSH_DESCRIPTION,
      versionProvider = HddsVersionProvider.class,
      mixinStandardHelpOptions = true)
  public void zshCompletion() {
    bashCompletion();
  }

  private String getBashCompletion() {
    final CommandLine ozone = new CommandLine(new Ozone());
    
    // Add first-level subcommands that are defined in the ozone shell script
    // but not implemented as GenericCli classes
    addShellScriptSubcommands(ozone);
    
    // Add second-level subcommands discovered via reflection
    for (String pkg : PACKAGES_TO_SCAN) {
      new Reflections(pkg).getSubTypesOf(GenericCli.class).stream()
          .map(AutoCompletion::getCommand)
          .filter(Objects::nonNull)
          .filter(AutoCompletion::isPublicCommand)
          .forEach(command -> ozone.addSubcommand(getCommandName(command), command));
    }
    return AutoComplete.bash("ozone", ozone);
  }

  /**
   * Add first-level subcommands that are defined in the ozone shell script
   * but not implemented as GenericCli classes.
   */
  private void addShellScriptSubcommands(CommandLine ozone) {
    // Add shell script commands that don't have GenericCli implementations
    ozone.addSubcommand("classpath", new CommandLine(new ClasspathCommand()));
    ozone.addSubcommand("datanode", new CommandLine(new DatanodeCommand()));
    ozone.addSubcommand("envvars", new CommandLine(new EnvvarsCommand()));
    ozone.addSubcommand("daemonlog", new CommandLine(new DaemonlogCommand()));
    ozone.addSubcommand("freon", new CommandLine(new FreonCommand()));
    ozone.addSubcommand("fs", new CommandLine(new FsCommand()));
    ozone.addSubcommand("om", new CommandLine(new OmCommand()));
    ozone.addSubcommand("scm", new CommandLine(new ScmCommand()));
    ozone.addSubcommand("s3g", new CommandLine(new S3gCommand()));
    ozone.addSubcommand("httpfs", new CommandLine(new HttpfsCommand()));
    ozone.addSubcommand("csi", new CommandLine(new CsiCommand()));
    ozone.addSubcommand("recon", new CommandLine(new ReconCommand()));
    ozone.addSubcommand("insight", new CommandLine(new InsightCommand()));
    ozone.addSubcommand("version", new CommandLine(new VersionCommand()));
    ozone.addSubcommand("dtutil", new CommandLine(new DtutilCommand()));
  }

  private static CommandLine getCommand(Class<? extends GenericCli> clazz) {
    CommandLine command = null;
    try {
      command = ReflectionUtils.newInstance(clazz).getCmd();
    } catch (UnsupportedOperationException ignored) {
      // Skip the GenericCli subclasses that do not have no-args constructor.
    }
    return command;
  }

  private static boolean isPublicCommand(CommandLine command) {
    final CommandLine.Model.CommandSpec spec = command.getCommandSpec();
    return !spec.usageMessage().hidden() &&
      !CommandLine.Model.CommandSpec.DEFAULT_COMMAND_NAME.equals(spec.name());
  }

  private static String getCommandName(CommandLine command) {
    final CommandLine.Model.CommandSpec spec = command.getCommandSpec();
    final String qualifiedName = spec.qualifiedName();
    return qualifiedName.startsWith(OZONE_COMMAND) ?
      qualifiedName.substring(PREFIX_LENGTH) : qualifiedName;
  }

  public static void main(String[] args) {
    new AutoCompletion().run(args);
  }

  /** Ozone top level command, used only to generate auto-complete. */
  @Command(name = "ozone",
      description = "Ozone top level command")
  private static final class Ozone {

    @Option(names = {"--buildpaths"},
        description = "attempt to add class files from build tree")
    private String buildpaths;

    @Option(names = {"--config"},
        description = "Ozone config directory")
    private String config;

    @Option(names = {"--debug"},
        description = "turn on shell script debug mode")
    private String debug;

    @Option(names = {"--daemon"},
        description = "attempt to add class files from build tree")
    private String daemon;

    @Option(names = {"--help"},
        description = "usage information")
    private String help;

    @Option(names = {"--hostnames"},
        description = "hosts to use in worker mode")
    private String hostnames;

    @Option(names = {"--hosts"},
        description = "list of hosts to use in worker mode")
    private String hosts;

    @Option(names = {"--loglevel"},
        description = "set the log4j level for this command")
    private String loglevel;

    @Option(names = {"--workers"},
        description = "turn on worker mode")
    private String workers;

    @Option(names = {"--jvmargs"},
        description = "append JVM options to any existing options defined in the OZONE_OPTS environment variable. " +
            "Any defined in OZONE_CLIENT_OPTS will be appended after these jvmargs")
    private String jvmargs;

    @Option(names = {"--validate"},
        description = "validates if all jars as indicated in the corresponding OZONE_RUN_ARTIFACT_NAME classpath " +
            "file are present, command execution shall continue post validation failure if 'continue' is passed")
    private String validate;
  }

  // Simple command classes for shell script subcommands that don't have GenericCli implementations

  @Command(name = "classpath", description = "prints the class path needed for running ozone commands")
  private static final class ClasspathCommand {
  }

  @Command(name = "datanode", description = "run a HDDS datanode")
  private static final class DatanodeCommand {
  }

  @Command(name = "envvars", description = "display computed Hadoop environment variables")
  private static final class EnvvarsCommand {
  }

  @Command(name = "daemonlog", description = "get/set the log level for each daemon")
  private static final class DaemonlogCommand {
  }

  @Command(name = "freon", description = "runs an ozone data generator")
  private static final class FreonCommand {
  }

  @Command(name = "fs", description = "run a filesystem command on Ozone file system. Equivalent to 'hadoop fs'")
  private static final class FsCommand {
  }

  @Command(name = "om", description = "Ozone Manager")
  private static final class OmCommand {
  }

  @Command(name = "scm", description = "run the Storage Container Manager service")
  private static final class ScmCommand {
  }

  @Command(name = "s3g", description = "run the S3 compatible REST gateway")
  private static final class S3gCommand {
  }

  @Command(name = "httpfs", description = "run the HTTPFS compatible REST gateway")
  private static final class HttpfsCommand {
  }

  @Command(name = "csi", description = "run the standalone CSI daemon")
  private static final class CsiCommand {
  }

  @Command(name = "recon", description = "run the Recon service")
  private static final class ReconCommand {
  }

  @Command(name = "insight", description = "tool to get runtime operation information")
  private static final class InsightCommand {
  }

  @Command(name = "version", description = "print the version")
  private static final class VersionCommand {
  }

  @Command(name = "dtutil", description = "operations related to delegation tokens")
  private static final class DtutilCommand {
  }
}
