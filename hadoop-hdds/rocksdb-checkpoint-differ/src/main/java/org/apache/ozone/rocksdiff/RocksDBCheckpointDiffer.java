/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ozone.rocksdiff;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.AbstractEventListener;
import org.rocksdb.Checkpoint;
import org.rocksdb.CompactionJobInfo;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileReader;
import org.rocksdb.TableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// TODO
//  1. Create a local instance of RocksDiff-local-RocksDB. This is the
//  rocksDB that we can use for maintaining DAG and any other state. This is
//  a per node state so it it doesn't have to go through RATIS anyway.
//  2. Store fwd DAG in Diff-Local-RocksDB in Compaction Listener
//  3. Store fwd DAG in Diff-Local-RocksDB in Compaction Listener
//  4. Store last-Snapshot-counter/Compaction-generation-counter in Diff-Local
//  -RocksDB in Compaction Listener
//  5. System Restart handling. Read the DAG from Disk and load it in memory.
//  6. Take the base snapshot. All the SST file nodes in the base snapshot
//  should be arked with that Snapshot generation. Subsequently, all SST file
//  node should have a snapshot-generation count and Compaction-generation
//  count.
//  6. DAG based SST file pruning. Start from the oldest snapshot and we can
//  unlink any SST
//  file from the SaveCompactedFilePath directory that is reachable in the
//  reverse DAG.
//  7. DAG pruning : For each snapshotted bucket, We can recycle the part of
//  the DAG that is older than the predefined policy for the efficient snapdiff.
//  E.g. we may decide not to support efficient snapdiff from any snapshot that
//  is older than 2 weeks.
//  Note on 8. & 9 .
//  ==================
//  A simple handling is to just iterate over all keys in keyspace when the
//  compaction DAG is lost, instead of optimizing every case. And start
//  Compaction-DAG afresh from the latest snapshot.
//  --
//  8. Handle bootstrapping rocksDB for a new OM follower node
//      - new node will receive Active object store as well as all existing
//      rocksDB checkpoints.
//      - This bootstrapping should also receive the compaction-DAG information
//  9. Handle rebuilding the DAG for a lagging follower. There are two cases
//      - recieve RATIS transactions to replay. Nothing needs to be done in
//      thise case.
//      - Getting the DB sync. This case needs to handle getting the
//      compaction-DAG information as well.

/**
 *  RocksDBCheckpointDiffer class.
 */
public class RocksDBCheckpointDiffer {
  private final String rocksDbPath;
  private String cpPath;
  private String saveCompactedFilePath;
  private int maxSnapshots;
  private static final Logger LOG =
      LoggerFactory.getLogger(RocksDBCheckpointDiffer.class);

  // keeps track of all the snapshots created so far.
  private int lastSnapshotCounter;
  private String lastSnapshotPrefix;

  // Something to track all the snapshots created so far. TODO: REMOVE
  private Snapshot[] allSnapshots;

  private String compactionLogParentDir = null;
  private String compactionLogDir = null;

  // Name of the directory that holds compaction logs (under metadata dir)
  private static final String COMPACTION_LOG_DIR = "compaction-log/";

  /**
   * Compaction log path for DB compaction history persistence.
   * This is the source of truth for in-memory SST DAG reconstruction upon
   * OM restarts.
   *
   * Initialized to the latest sequence number on OM startup. And the log rolls
   * over to a new file whenever an Ozone snapshot is taken.
   */
  private volatile String currentCompactionLogPath = null;

  private static final String COMPACTION_LOG_FILENAME_SUFFIX = ".log";

  /**
   * Marks the beginning of a comment line in the compaction log.
   */
  private static final String COMPACTION_LOG_COMMENT_LINE_PREFIX = "# ";

  /**
   * Marks the beginning of a compaction log entry.
   */
  private static final String COMPACTION_LOG_ENTRY_LINE_PREFIX = "C ";

  /**
   * Prefix for the sequence number line when writing to compaction log
   * right after taking an Ozone snapshot.
   */
  private static final String COMPACTION_LOG_SEQNUM_LINE_PREFIX = "S ";

  /**
   * SST file extension. Must be lower case.
   * Used to trim the file extension when writing compaction entries to the log
   * to save space.
   */
  private static final String SST_FILE_EXTENSION = ".sst";
  private static final int SST_FILE_EXTENSION_LENGTH =
      SST_FILE_EXTENSION.length();

  public void setCompactionLogParentDir(String parentDir) {
    this.compactionLogParentDir = parentDir;

    // Append /
    if (!compactionLogParentDir.endsWith("/")) {
      compactionLogParentDir += "/";
    }

    File pDir = new File(compactionLogParentDir);
    if (!pDir.exists()) {
      if (!pDir.mkdir()) {
        LOG.error("Error creating compaction log parent dir.");
        return;
      }
    }

    compactionLogDir =
        Paths.get(compactionLogParentDir, COMPACTION_LOG_DIR).toString();
    File clDir = new File(compactionLogDir);
    if (!clDir.exists() && !clDir.mkdir()) {
      LOG.error("Error creating compaction log dir.");
      return;
    }

    // TODO: Write a README there explaining what the dir is for
  }

  private static final int LONG_MAX_STRLEN =
      String.valueOf(Long.MAX_VALUE).length();

  /**
   * Set the current compaction log filename with a given RDB sequence number.
   * @param latestSequenceNum latest sequence number of RDB.
   */
  public void setCurrentCompactionLog(long latestSequenceNum) {
    String latestSequenceIdStr = String.valueOf(latestSequenceNum);

    if (latestSequenceIdStr.length() < LONG_MAX_STRLEN) {
      // Pad zeroes to the left for ordered file listing
      latestSequenceIdStr =
          StringUtils.leftPad(latestSequenceIdStr, LONG_MAX_STRLEN, "0");
    }

    // Local temp variable for storing the new compaction log file path
    final String newCompactionLog = compactionLogParentDir +
        COMPACTION_LOG_DIR + latestSequenceIdStr +
        COMPACTION_LOG_FILENAME_SUFFIX;

    File clFile = new File(newCompactionLog);
    if (clFile.exists()) {
      LOG.warn("Compaction log exists: {}. Will append", newCompactionLog);
    }

    this.currentCompactionLogPath = compactionLogParentDir +
        COMPACTION_LOG_DIR + latestSequenceIdStr +
        COMPACTION_LOG_FILENAME_SUFFIX;

    // Create empty file if it doesn't exist
    appendToCurrentCompactionLog("");
  }

  public RocksDBCheckpointDiffer(String dbPath,
                                 int maxSnapshots,
                                 String checkpointPath,
                                 String sstFileSaveDir,
                                 int initialSnapshotCounter,
                                 String snapPrefix) {
    this.maxSnapshots = maxSnapshots;
    allSnapshots = new Snapshot[this.maxSnapshots];
    cpPath = checkpointPath;

    saveCompactedFilePath = sstFileSaveDir;

    // Append /
    if (!saveCompactedFilePath.endsWith("/")) {
      saveCompactedFilePath += "/";
    }

    // Create the directory if SST backup path does not already exist
    File dir = new File(saveCompactedFilePath);

    if (!dir.exists() && !dir.mkdir()) {
      LOG.error("Failed to create SST file backup directory!");
      // TODO: Throw custom checked exception instead?
      throw new RuntimeException("Failed to create SST file backup directory. "
          + "Check write permission.");
    }

    rocksDbPath = dbPath;

    // TODO: This module should be self sufficient in tracking the last
    //  snapshotCounter and currentCompactionGen for a given dbPath. It needs
    //  to be persisted.
    // TODO: Only used in UT. Move
    lastSnapshotCounter = initialSnapshotCounter;
    lastSnapshotPrefix = snapPrefix;

    // TODO: this should also independently persist every compaction e.g.
    //  (input files) ->
    //  {  (output files) + lastSnapshotCounter + currentCompactionGen }
    //  mapping.

    // Note: Previous compaction log files are loaded in RDBStore, not here.
  }

  // Node in the DAG to represent an SST file
  private static class CompactionNode {
    // Name of the SST file
    private String fileName;
    // The last snapshot created before this node came into existence
    private String snapshotId;
    private long snapshotGeneration;
    private long totalNumberOfKeys;
    private long cumulativeKeysReverseTraversal;

    CompactionNode(String file, String ssId, long numKeys, long seqNum) {
      fileName = file;
      snapshotId = ssId;
      totalNumberOfKeys = numKeys;
      snapshotGeneration = seqNum;
      cumulativeKeysReverseTraversal = 0L;
    }
  }

  private static class Snapshot {
    private String dbPath;
    private String snapshotID;
    private long snapshotGeneration;

    Snapshot(String db, String id, long gen) {
      dbPath = db;
      snapshotID = id;
      snapshotGeneration = gen;
    }
  }

  // Hash table to track CompactionNode for a given SST File.
  private ConcurrentHashMap<String, CompactionNode> compactionNodeTable =
      new ConcurrentHashMap<>();

  // We are maintaining a two way DAG. This allows easy traversal from
  // source snapshot to destination snapshot as well as the other direction.

  private MutableGraph<CompactionNode> compactionDAGFwd =
      GraphBuilder.directed().build();

  private MutableGraph<CompactionNode> compactionDAGReverse =
      GraphBuilder.directed().build();

  public static final Integer DEBUG_DAG_BUILD_UP = 2;
  public static final Integer DEBUG_DAG_TRAVERSAL = 3;
  public static final Integer DEBUG_DAG_LIVE_NODES = 4;
  public static final Integer DEBUG_READ_ALL_DB_KEYS = 5;
  private static final HashSet<Integer> DEBUG_LEVEL = new HashSet<>();

  static {
    addDebugLevel(DEBUG_DAG_BUILD_UP);
    addDebugLevel(DEBUG_DAG_TRAVERSAL);
    addDebugLevel(DEBUG_DAG_LIVE_NODES);
  }

  static {
    RocksDB.loadLibrary();
  }

  public static void addDebugLevel(Integer level) {
    DEBUG_LEVEL.add(level);
  }

  // Flushes the WAL and Creates a RocksDB checkpoint
  @SuppressWarnings({"NM_METHOD_NAMING_CONVENTION"})
  public void createCheckPoint(String dbPathArg, String cpPathArg,
                               RocksDB rocksDB) {
    LOG.debug("Creating RocksDB '{}' checkpoint at '{}'",
        dbPathArg, cpPathArg);
    try {
      rocksDB.flush(new FlushOptions());
      Checkpoint cp = Checkpoint.create(rocksDB);
      cp.createCheckpoint(cpPathArg);
    } catch (RocksDBException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  /**
   * Append (then flush) to the current compaction log file path.
   * Note: This does NOT automatically append newline to the log.
   */
  private synchronized void appendToCurrentCompactionLog(String content) {
    if (currentCompactionLogPath == null) {
      LOG.error("Unable to append compaction log. "
          + "Compaction log path is not set. "
          + "Please check initialization.");
      throw new RuntimeException("Compaction log path not set");
    }
    try (BufferedWriter bw = Files.newBufferedWriter(
        Paths.get(currentCompactionLogPath),
        StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
      bw.write(content);
      bw.flush();
    } catch (IOException e) {
      throw new RuntimeException("Failed to append compaction log to " +
          currentCompactionLogPath, e);
    }
  }

  /**
   * Append a sequence number to the compaction log (roughly) when an Ozone
   * snapshot (RDB checkpoint) is taken.
   * @param sequenceNum RDB sequence number
   */
  public void appendSequenceNumberToCompactionLog(long sequenceNum) {
    final String line = COMPACTION_LOG_SEQNUM_LINE_PREFIX + sequenceNum + "\n";
    appendToCurrentCompactionLog(line);
  }

  /**
   * Takes {@link org.rocksdb.Options}.
   */
  public void setRocksDBForCompactionTracking(Options rocksOptions,
      List<AbstractEventListener> list) {
    list.add(newCompactionBeginListener());
    list.add(newCompactionCompletedListener());
    rocksOptions.setListeners(list);
  }

  public void setRocksDBForCompactionTracking(Options rocksOptions) {
    setRocksDBForCompactionTracking(rocksOptions, new ArrayList<>());
  }

  /**
   * Takes {@link org.rocksdb.DBOptions}.
   */
  public void setRocksDBForCompactionTracking(DBOptions rocksOptions,
      List<AbstractEventListener> list) {
    list.add(newCompactionBeginListener());
    list.add(newCompactionCompletedListener());
    rocksOptions.setListeners(list);
  }

  public void setRocksDBForCompactionTracking(DBOptions rocksOptions)
      throws RocksDBException {
    setRocksDBForCompactionTracking(rocksOptions, new ArrayList<>());
  }

  private AbstractEventListener newCompactionBeginListener() {
    return new AbstractEventListener() {
      @Override
      public void onCompactionBegin(RocksDB db,
          CompactionJobInfo compactionJobInfo) {

        synchronized (db) {

          if (compactionJobInfo.inputFiles().size() == 0) {
            LOG.error("Compaction input files list is empty");
            return;
          }

          // Create hardlink backups for the SST files that are going
          // to be deleted after this RDB compaction.
          for (String file : compactionJobInfo.inputFiles()) {
            LOG.debug("Creating hard link for '{}'", file);
            String saveLinkFileName =
                saveCompactedFilePath + new File(file).getName();
            Path link = Paths.get(saveLinkFileName);
            Path srcFile = Paths.get(file);
            try {
              Files.createLink(link, srcFile);
            } catch (IOException e) {
              LOG.error("Exception in creating hard link for {}", file);
              throw new RuntimeException("Failed to create hard link", e);
            }
          }

        }
      }
    };
  }

  /**
   * Helper function to append the list of SST files to a StringBuilder
   * for a compaction log entry. Does not append a new line.
   * @param files
   * @param sb
   */
  private static void appendCompactionLogStringBuilder(List<String> files,
      StringBuilder sb) {

    Iterator<String> it = files.iterator();
    while (it.hasNext()) {
      final String filename = it.next();
      sb.append(filename);
      // Do not append delimiter if this is the last one
      if (it.hasNext()) {
        sb.append(',');
      }
    }
  }

  private AbstractEventListener newCompactionCompletedListener() {
    return new AbstractEventListener() {
      @Override
      @SuppressFBWarnings({
          "AT_OPERATION_SEQUENCE_ON_CONCURRENT_ABSTRACTION",
          "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE"})
      public void onCompactionCompleted(RocksDB db,
          CompactionJobInfo compactionJobInfo) {

        synchronized (db) {

          if (compactionJobInfo.inputFiles().size() == 0) {
            LOG.error("Compaction input files list is empty");
            return;
          }

          final StringBuilder sb = new StringBuilder();

          if (LOG.isDebugEnabled()) {
            // Print compaction reason for this entry in the log file
            // e.g. kLevelL0FilesNum / kLevelMaxLevelSize.
            sb.append(COMPACTION_LOG_COMMENT_LINE_PREFIX)
                .append(compactionJobInfo.compactionReason())
                .append('\n');
          }

          // Mark the beginning of a compaction log
          sb.append(COMPACTION_LOG_ENTRY_LINE_PREFIX);

          // Trim DB path, only keep the SST file name
          final int filenameOffset =
              compactionJobInfo.inputFiles().get(0).lastIndexOf("/") + 1;

          // Append the list of input files
          final List<String> inputFiles = compactionJobInfo.inputFiles();
          inputFiles.replaceAll(s -> s.substring(
              filenameOffset, s.length() - SST_FILE_EXTENSION_LENGTH));
          appendCompactionLogStringBuilder(inputFiles, sb);

          // Insert delimiter between input files an output files
          sb.append(':');

          // Append the list of output files
          final List<String> outputFiles = compactionJobInfo.outputFiles();
          outputFiles.replaceAll(s -> s.substring(
              filenameOffset, s.length() - SST_FILE_EXTENSION_LENGTH));
          appendCompactionLogStringBuilder(outputFiles, sb);
          sb.append('\n');

          // Write input and output file names to compaction log
          appendToCurrentCompactionLog(sb.toString());

          // Populate the DAG
          populateCompactionDAG(inputFiles, outputFiles,
              db.getLatestSequenceNumber());

//          if (debugEnabled(DEBUG_DAG_BUILD_UP)) {
//            printMutableGraph(null, null, compactionDAGFwd);
//          }
        }
      }
    };
  }

  public RocksDB getRocksDBInstanceWithCompactionTracking(String dbPath)
      throws RocksDBException {
    final Options opt = new Options().setCreateIfMissing(true);
//    opt.setWriteBufferSize(1L);  // Unit in bytes. Default is 64 MB.
//    opt.setMaxWriteBufferNumber(1);  // Default is 2
//    opt.setCompressionType(CompressionType.NO_COMPRESSION);
//    opt.setMaxBytesForLevelMultiplier(2);
    setRocksDBForCompactionTracking(opt);
    return RocksDB.open(opt, dbPath);
  }

  /**
   * Get number of entries in an SST file.
   * @param filename
   * @return
   * @throws RocksDBException
   */
  public long getSSTFileSummary(String filename)
      throws RocksDBException {

    if (!filename.endsWith(SST_FILE_EXTENSION)) {
      filename += SST_FILE_EXTENSION;
    }

    Options option = new Options();
    SstFileReader reader = new SstFileReader(option);
    try {
      reader.open(saveCompactedFilePath + filename);
    } catch (RocksDBException e) {
      reader.open(rocksDbPath + "/" + filename);
    }
    TableProperties properties = reader.getTableProperties();

    if (LOG.isDebugEnabled()) {
      LOG.debug("{} has {} keys", filename, properties.getNumEntries());
    }
    return properties.getNumEntries();
  }

  /**
   * Helper method to trim the filename retrieved from LiveFileMetaData.
   */
  private String trimSSTFilename(String filename) {
    if (!filename.startsWith("/")) {
      final String errorMsg = String.format(
          "Invalid start of filename: '%s'. Expected '/'", filename);
      LOG.error(errorMsg);
      throw new RuntimeException(errorMsg);
    }
    if (!filename.endsWith(SST_FILE_EXTENSION)) {
      final String errorMsg = String.format(
          "Invalid extension of file: '%s'. Expected '%s'",
          filename, SST_FILE_EXTENSION_LENGTH);
      LOG.error(errorMsg);
      throw new RuntimeException(errorMsg);
    }
    return filename.substring("/".length(),
        filename.length() - SST_FILE_EXTENSION_LENGTH);
  }

  /**
   * Read the current Live manifest for a given RocksDB instance (Active or
   * Checkpoint).
   * @param dbPathArg
   * @return a list of SST files (without extension) in the DB.
   */
  @SuppressFBWarnings({"NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE"})
  public HashSet<String> readRocksDBLiveFiles(String dbPathArg) {
    RocksDB rocksDB = null;
    HashSet<String> liveFiles = new HashSet<>();

    try (Options options = new Options()
        .setParanoidChecks(true)
        .setForceConsistencyChecks(false)) {

      rocksDB = RocksDB.openReadOnly(options, dbPathArg);
      List<LiveFileMetaData> liveFileMetaDataList =
          rocksDB.getLiveFilesMetaData();
      LOG.debug("SST File Metadata for DB: " + dbPathArg);
      for (LiveFileMetaData m : liveFileMetaDataList) {
        LOG.debug("File: {}, Level: {}", m.fileName(), m.level());
        final String trimmedFilename = trimSSTFilename(m.fileName());
        liveFiles.add(trimmedFilename);
      }
    } catch (RocksDBException e) {
      LOG.error("Error during RocksDB operation: {}", e.getMessage());
      e.printStackTrace();
    } finally {
      if (rocksDB != null) {
        rocksDB.close();
      }
    }
    return liveFiles;
  }

  private long reconstructionSnapshotGeneration;

  /**
   * Process each line of compaction log text file input and populate the DAG.
   */
  private synchronized void processCompactionLogLine(String line) {

    LOG.debug("Processing line: {}", line);

    if (line.startsWith("#")) {
      // Skip comments
      LOG.debug("Comment line, skipped");
    } else if (line.startsWith(COMPACTION_LOG_SEQNUM_LINE_PREFIX)) {
      // Read sequence number
      LOG.debug("Reading sequence number as snapshot generation");
      final String seqNumStr =
          line.substring(COMPACTION_LOG_SEQNUM_LINE_PREFIX.length()).trim();
      // This would the snapshot generation for the nodes to come
      reconstructionSnapshotGeneration = Long.parseLong(seqNumStr);
    } else if (line.startsWith(COMPACTION_LOG_ENTRY_LINE_PREFIX)) {
      // Read compaction log entry

      // Trim the beginning
      line = line.substring(COMPACTION_LOG_SEQNUM_LINE_PREFIX.length());
      final String[] io = line.split(":");
      if (io.length != 2) {
        LOG.error("Invalid line in compaction log: {}", line);
        return;
      }
      final String[] inputFiles = io[0].split(",");
      final String[] outputFiles = io[1].split(",");
      populateCompactionDAG(Arrays.asList(inputFiles),
          Arrays.asList(outputFiles), reconstructionSnapshotGeneration);
    } else {
      LOG.error("Invalid line in compaction log: {}", line);
    }
  }

  /**
   * Helper to read compaction log to the internal DAG.
   */
  private void readCompactionLogToDAG(String currCompactionLogPath) {
    LOG.debug("Loading compaction log: {}", currCompactionLogPath);
    try (Stream<String> logLineStream =
        Files.lines(Paths.get(currCompactionLogPath), StandardCharsets.UTF_8)) {
      logLineStream.forEach(this::processCompactionLogLine);
    } catch (IOException ioEx) {
      throw new RuntimeException(ioEx);
    }
  }

  /**
   * Returns a set of SST nodes that doesn't exist in the in-memory DAG.
   */
  private Set<String> getNonExistentSSTSet(Set<String> sstSet) {

    // Make a copy of sstSet
    HashSet<String> loadSet = new HashSet<>(sstSet);

    // Check if all the nodes in the provided SST set is already loaded in DAG
    for (String sstFile : sstSet) {
      if (compactionNodeTable.containsKey(sstFile)) {
        loadSet.remove(sstFile);
      }
    }

    return loadSet;
  }

  /**
   * Returns true only when all nodes in sstSet exists in DAG.
   */
  private boolean isSSTSetLoaded(HashSet<String> sstSet) {

    return getNonExistentSSTSet(sstSet).size() == 0;
  }

  /**
   * Read compaction log until all dest (and src) db checkpoint SST
   * nodes show up in the graph, or when it reaches the end of the log.
   */
  private boolean loadCompactionDAGBySSTSet(HashSet<String> sstSet) {

    // Get a set of SSTs that doesn't exist in the current in-memory DAG
    Set<String> loadSet = getNonExistentSSTSet(sstSet);

    if (loadSet.size() == 0) {
      // All expected nodes in the sstSet are already there,
      //  no need to read/load any compaction log from disk.
      return true;
    }

    // Otherwise, load compaction logs in order until all nodes are present in
    //  the DAG.
    try {
      try (Stream<Path> pathStream = Files.list(Paths.get(compactionLogDir))
          .filter(e -> e.toString()
              .toLowerCase().endsWith(COMPACTION_LOG_FILENAME_SUFFIX))
          .sorted()) {
        for (Path logPath : pathStream.collect(Collectors.toList())) {

          // TODO: Potential optimization: stop reading as soon as all nodes are
          //  there. Currently it loads an entire file at a time.
          readCompactionLogToDAG(logPath.toString());

          for (Iterator<String> it = loadSet.iterator(); it.hasNext();) {
            String sstFile = it.next();
            if (compactionNodeTable.containsKey(sstFile)) {
              LOG.debug("Found SST node: {}", sstFile);
              it.remove();
            }
          }

          if (loadSet.size() == 0) {
            break;
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Error listing compaction log dir " +
          compactionLogDir, e);
    }

    // Just in case there are still nodes to be expected not loaded.
    if (loadSet.size() > 0) {
      LOG.warn("The following nodes are missing from the compaction log: {}. "
          + "Possibly because those a newly flushed SSTs that haven't gone "
          + "though any compaction yet", loadSet);
      return false;
    }

    return true;
  }

  /**
   * Load existing compaction log files to the in-memory DAG.
   * This only needs to be done once during OM startup.
   */
  public synchronized void loadAllCompactionLogs() {
    if (compactionLogDir == null) {
      throw new RuntimeException("Compaction log directory must be set first");
    }
    reconstructionSnapshotGeneration = 0L;
    try {
      try (Stream<Path> pathStream = Files.list(Paths.get(compactionLogDir))
          .filter(e -> e.toString().toLowerCase().endsWith(".log"))
          .sorted()) {
        for (Path logPath : pathStream.collect(Collectors.toList())) {
          readCompactionLogToDAG(logPath.toString());
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Error listing compaction log dir " +
          compactionLogDir, e);
    }
  }

  /**
   * Get a list of SST files that differs between src and destination snapshots.
   * <p>
   * Expected input: src is a snapshot taken AFTER the dest.
   *
   * @param src source snapshot
   * @param dest destination snapshot
   */
  public synchronized List<String> getSSTDiffList(Snapshot src, Snapshot dest) {

    LOG.debug("src '{}' -> dest '{}'", src.dbPath, dest.dbPath);
    HashSet<String> srcSnapFiles = readRocksDBLiveFiles(src.dbPath);
    HashSet<String> destSnapFiles = readRocksDBLiveFiles(dest.dbPath);

    HashSet<String> fwdDAGSameFiles = new HashSet<>();
    HashSet<String> fwdDAGDifferentFiles = new HashSet<>();

    LOG.debug("Doing forward diff between src and dest snapshots: " +
        src.dbPath + " to " + dest.dbPath);
    internalGetSSTDiffList(src, dest, srcSnapFiles, destSnapFiles,
        compactionDAGFwd, fwdDAGSameFiles, fwdDAGDifferentFiles);

    List<String> res = new ArrayList<>();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Result of diff from src '" + src.dbPath + "' to dest '" +
          dest.dbPath + "':");
      StringBuilder logSB = new StringBuilder();

      logSB.append("Fwd DAG same SST files:      ");
      for (String file : fwdDAGSameFiles) {
        logSB.append(file).append(" ");
      }
      LOG.debug(logSB.toString());

      logSB.setLength(0);
      logSB.append("Fwd DAG different SST files: ");
      for (String file : fwdDAGDifferentFiles) {
        logSB.append(file).append(" ");
        res.add(file);
      }
      LOG.debug(logSB.toString());

    } else {
      res.addAll(fwdDAGDifferentFiles);
    }

    return res;
  }

  @SuppressFBWarnings({"NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE"})
  public synchronized void internalGetSSTDiffList(
      Snapshot src, Snapshot dest,
      HashSet<String> srcSnapFiles,
      HashSet<String> destSnapFiles,
      MutableGraph<CompactionNode> mutableGraph,
      HashSet<String> sameFiles, HashSet<String> differentFiles) {

    for (String fileName : srcSnapFiles) {
      if (destSnapFiles.contains(fileName)) {
        LOG.debug("Source '{}' and destination '{}' share the same SST '{}'",
            src.dbPath, dest.dbPath, fileName);
        sameFiles.add(fileName);
        continue;
      }
      CompactionNode infileNode =
          compactionNodeTable.get(Paths.get(fileName).getFileName().toString());
      if (infileNode == null) {
        LOG.debug("Src " + src.dbPath + " File " + fileName +
            " was never compacted");
        differentFiles.add(fileName);
        continue;
      }
      LOG.debug("Expanding SST file: " + fileName);
      Set<CompactionNode> currentLevel = new HashSet<>();
      currentLevel.add(infileNode);
      Set<CompactionNode> nextLevel = new HashSet<>();
      int i = 1;
      while (currentLevel.size() != 0) {
        LOG.debug("DAG Level: " + i++);
        for (CompactionNode current : currentLevel) {
          LOG.debug("Acknowledging file " + current.fileName);
          if (current.snapshotGeneration <= dest.snapshotGeneration) {
            LOG.debug("Reached dest generation count. Src: " +
                src.dbPath + " and Dest: " + dest.dbPath +
                " have different file: " + current.fileName);
            differentFiles.add(current.fileName);
            continue;
          }
          Set<CompactionNode> successors = mutableGraph.successors(current);
          if (successors.size() == 0) {
            LOG.debug("No further compaction happened for the current file. " +
                "src: " + src.dbPath + " and dest: " + dest.dbPath +
                " have different file: " + current.fileName);
            differentFiles.add(current.fileName);
          } else {
            for (CompactionNode oneSucc : successors) {
              if (sameFiles.contains(oneSucc.fileName) ||
                  differentFiles.contains(oneSucc.fileName)) {
                LOG.debug("Skipping known same file: " + oneSucc.fileName);
                continue;
              }
              if (destSnapFiles.contains(oneSucc.fileName)) {
                LOG.debug("src: " + src.dbPath + " and dest: " + dest.dbPath +
                    " have the same file: " + oneSucc.fileName);
                sameFiles.add(oneSucc.fileName);
                continue;
              } else {
                LOG.debug("src " + src.dbPath + " and dest " + dest.dbPath +
                    " have a different SST: " + oneSucc.fileName);
                nextLevel.add(oneSucc);
              }
            }
          }
        }
        currentLevel = new HashSet<>(nextLevel);
        nextLevel = new HashSet<>();
      }
    }
  }

  @SuppressFBWarnings("SIC_INNER_SHOULD_BE_STATIC")
  class NodeComparator implements Comparator<CompactionNode> {
    public int compare(CompactionNode a, CompactionNode b) {
      return a.fileName.compareToIgnoreCase(b.fileName);
    }

    @Override
    public Comparator<CompactionNode> reversed() {
      return null;
    }
  }

  public void dumpCompactionNodeTable() {
    List<CompactionNode> nodeList = compactionNodeTable.values().stream()
        .sorted(new NodeComparator()).collect(Collectors.toList());
    for (CompactionNode n : nodeList) {
      LOG.info("File '{}' total keys: {}", n.fileName, n.totalNumberOfKeys);
      LOG.info("File '{}' cumulative keys: {}", n.fileName,
          n.cumulativeKeysReverseTraversal);
    }
  }

  @SuppressFBWarnings({"NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE"})
  public synchronized void printMutableGraphFromAGivenNode(
      String fileName, int level, MutableGraph<CompactionNode> mutableGraph) {
    CompactionNode infileNode =
        compactionNodeTable.get(Paths.get(fileName).getFileName().toString());
    if (infileNode == null) {
      return;
    }
    LOG.info("\nCompaction Level: " + level + " Expanding File: " + fileName);
    Set<CompactionNode> nextLevel = new HashSet<>();
    nextLevel.add(infileNode);
    Set<CompactionNode> currentLevel = new HashSet<>(nextLevel);
    int i = 1;
    while (currentLevel.size() != 0) {
      LOG.info("DAG Level: " + i++);
      StringBuilder sb = new StringBuilder();
      for (CompactionNode current : currentLevel) {
        Set<CompactionNode> successors = mutableGraph.successors(current);
        for (CompactionNode oneSucc : successors) {
          sb.append(oneSucc.fileName).append(" ");
          nextLevel.add(oneSucc);
        }
      }
      LOG.info(sb.toString());
      currentLevel = new HashSet<>();
      currentLevel.addAll(nextLevel);
      nextLevel = new HashSet<>();
    }
  }

  public synchronized void printMutableGraph(
      String srcSnapId, String destSnapId,
      MutableGraph<CompactionNode> mutableGraph) {
    LOG.warn("Printing the Graph");
    Set<CompactionNode> topLevelNodes = new HashSet<>();
    Set<CompactionNode> allNodes = new HashSet<>();
    for (CompactionNode n : mutableGraph.nodes()) {
      if (srcSnapId == null ||
          n.snapshotId.compareToIgnoreCase(srcSnapId) == 0) {
        topLevelNodes.add(n);
      }
    }
    Iterator<CompactionNode> iter = topLevelNodes.iterator();
    while (iter.hasNext()) {
      CompactionNode n = iter.next();
      Set<CompactionNode> succ = mutableGraph.successors(n);
      LOG.warn("Parent Node: " + n.fileName);
      if (succ.size() == 0) {
        LOG.warn("No child node");
        allNodes.add(n);
        iter.remove();
        iter = topLevelNodes.iterator();
        continue;
      }
      for (CompactionNode oneSucc : succ) {
        LOG.warn("Children Node: " + oneSucc.fileName);
        if (srcSnapId == null ||
            oneSucc.snapshotId.compareToIgnoreCase(destSnapId) == 0) {
          allNodes.add(oneSucc);
        } else {
          topLevelNodes.add(oneSucc);
        }
      }
      iter.remove();
      iter = topLevelNodes.iterator();
    }
    LOG.warn("src snap: " + srcSnapId);
    LOG.warn("dest snap: " + destSnapId);
    for (CompactionNode n : allNodes) {
      LOG.warn("Files are: " + n.fileName);
    }
  }

  /**
   * Helper function that recursively deletes the dir. TODO: REMOVE
   */
  boolean deleteDirectory(File directoryToBeDeleted) {
    File[] allContents = directoryToBeDeleted.listFiles();
    if (allContents != null) {
      for (File file : allContents) {
        deleteDirectory(file);
      }
    }
    return directoryToBeDeleted.delete();
  }

  @VisibleForTesting
  public void createSnapshot(RocksDB rocksDB) throws InterruptedException {

    LOG.trace("Current time: " + System.currentTimeMillis());
    long t1 = System.currentTimeMillis();

    cpPath = cpPath + lastSnapshotCounter;
    // Delete the checkpoint dir if it already exists
    File dir = new File(cpPath);
    if (dir.exists()) {
      deleteDirectory(dir);  // TODO: TESTING ONLY
    }

    final long dbLatestSequenceNumber = rocksDB.getLatestSequenceNumber();

    createCheckPoint(rocksDbPath, cpPath, rocksDB);
    allSnapshots[lastSnapshotCounter] =
        new Snapshot(cpPath, lastSnapshotPrefix, lastSnapshotCounter);

    // Does what OmSnapshotManager#createOmSnapshotCheckpoint would do
    appendSequenceNumberToCompactionLog(dbLatestSequenceNumber);

    setCompactionLogParentDir(".");
    setCurrentCompactionLog(dbLatestSequenceNumber);

    long t2 = System.currentTimeMillis();
    LOG.trace("Current time: " + t2);
    LOG.debug("Time elapsed: " + (t2 - t1) + " ms");
    Thread.sleep(100);
    ++lastSnapshotCounter;
    lastSnapshotPrefix = "sid_" + lastSnapshotCounter;
  }

  public void printAllSnapshots() {
    for (Snapshot snap : allSnapshots) {
      if (snap == null) {
        break;
      }
      LOG.warn("Snapshot id" + snap.snapshotID);
      LOG.warn("Snapshot path" + snap.dbPath);
      LOG.warn("Snapshot Generation" + snap.snapshotGeneration);
      LOG.warn("");
    }
  }

  public void diffAllSnapshots() {
    for (Snapshot snap : allSnapshots) {
      if (snap == null) {
        break;
      }
      System.out.println();
      // Returns a list of SST files to be fed into RocksDiff
      List<String> sstListForRocksDiff =
          getSSTDiffList(allSnapshots[lastSnapshotCounter - 1], snap);
      LOG.debug("getSSTDiffList returns: {}", sstListForRocksDiff);
    }
  }

  public MutableGraph<CompactionNode> getCompactionFwdDAG() {
    return compactionDAGFwd;
  }

  public MutableGraph<CompactionNode> getCompactionReverseDAG() {
    return compactionDAGReverse;
  }

  /**
   * Populate the compaction DAG with input and output SST files lists.
   */
  @SuppressFBWarnings({"AT_OPERATION_SEQUENCE_ON_CONCURRENT_ABSTRACTION"})
  private void populateCompactionDAG(List<String> inputFiles,
      List<String> outputFiles, long seqNum) {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Input {} -> Output {}", inputFiles, outputFiles);
    }

    for (String outfile : outputFiles) {
      CompactionNode outfileNode = compactionNodeTable.get(outfile);
      if (outfileNode == null) {
        long numKeys = 0L;
        try {
          numKeys = getSSTFileSummary(outfile);
        } catch (Exception e) {
          LOG.warn("Exception in getSSTFileSummary: {}", e.getMessage());
        }
        outfileNode = new CompactionNode(outfile, lastSnapshotPrefix, numKeys,
            seqNum);
        compactionDAGFwd.addNode(outfileNode);
        compactionDAGReverse.addNode(outfileNode);
        compactionNodeTable.put(outfile, outfileNode);
      }

      for (String infile : inputFiles) {
        CompactionNode infileNode = compactionNodeTable.get(infile);
        if (infileNode == null) {
          long numKeys = 0L;
          try {
            numKeys = getSSTFileSummary(infile);
          } catch (Exception e) {
            LOG.warn("Exception in getSSTFileSummary: {}", e.getMessage());
          }
          infileNode = new CompactionNode(infile, lastSnapshotPrefix, numKeys,
              seqNum);
          compactionDAGFwd.addNode(infileNode);
          compactionDAGReverse.addNode(infileNode);
          compactionNodeTable.put(infile, infileNode);
        }
        // Draw the edges
        if (outfileNode.fileName.compareToIgnoreCase(
            infileNode.fileName) != 0) {
          compactionDAGFwd.putEdge(outfileNode, infileNode);
          compactionDAGReverse.putEdge(infileNode, outfileNode);
        }
      }
    }

  }

  public synchronized void traverseGraph(
      MutableGraph<CompactionNode> reverseMutableGraph,
      MutableGraph<CompactionNode> fwdMutableGraph) {

    List<CompactionNode> nodeList = compactionNodeTable.values().stream()
        .sorted(new NodeComparator()).collect(Collectors.toList());

    for (CompactionNode infileNode : nodeList) {
      // fist go through fwdGraph to find nodes that don't have succesors.
      // These nodes will be the top level nodes in reverse graph
      Set<CompactionNode> successors = fwdMutableGraph.successors(infileNode);
      if (successors.size() == 0) {
        LOG.debug("No successors. Cumulative keys: {}, total keys: {}",
            infileNode.cumulativeKeysReverseTraversal,
            infileNode.totalNumberOfKeys);
        infileNode.cumulativeKeysReverseTraversal =
            infileNode.totalNumberOfKeys;
      }
    }

    HashSet<CompactionNode> visited = new HashSet<>();
    for (CompactionNode infileNode : nodeList) {
      if (visited.contains(infileNode)) {
        continue;
      }
      visited.add(infileNode);
      LOG.debug("Visiting node '{}'", infileNode.fileName);
      Set<CompactionNode> nextLevel = new HashSet<>();
      nextLevel.add(infileNode);
      Set<CompactionNode> currentLevel = new HashSet<>(nextLevel);
      nextLevel = new HashSet<>();
      int i = 1;
      while (currentLevel.size() != 0) {
        LOG.debug("DAG Level {}", i++);
        for (CompactionNode current : currentLevel) {
          LOG.debug("Expanding node {}", current.fileName);
          Set<CompactionNode> successors =
              reverseMutableGraph.successors(current);
          if (successors.size() == 0) {
            LOG.debug("No successors. Cumulative keys: {}",
                current.cumulativeKeysReverseTraversal);
          } else {
            for (CompactionNode oneSucc : successors) {
              LOG.debug("Adding to the next level: {}", oneSucc.fileName);
              LOG.debug("'{}' cumulative keys: {}. parent '{}' total keys: {}",
                  oneSucc.fileName, oneSucc.cumulativeKeysReverseTraversal,
                  current.fileName, current.totalNumberOfKeys);
              oneSucc.cumulativeKeysReverseTraversal +=
                  current.cumulativeKeysReverseTraversal;
              nextLevel.add(oneSucc);
            }
          }
        }
        currentLevel = new HashSet<>(nextLevel);
        nextLevel = new HashSet<>();
      }
    }
  }

  public boolean debugEnabled(Integer level) {
    return DEBUG_LEVEL.contains(level);
  }

  @VisibleForTesting
  public static Logger getLog() {
    return LOG;
  }
}
