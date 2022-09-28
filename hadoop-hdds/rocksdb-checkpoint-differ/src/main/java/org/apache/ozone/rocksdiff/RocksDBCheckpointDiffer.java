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

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.rocksdb.AbstractEventListener;
import org.rocksdb.Checkpoint;
import org.rocksdb.CompactionJobInfo;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileReader;
import org.rocksdb.TableProperties;

import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

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
//
//
/**
 *  RocksDBCheckpointDiffer class.
 */
//CHECKSTYLE:OFF
@SuppressWarnings({"NM_METHOD_NAMING_CONVENTION"})
public class RocksDBCheckpointDiffer {
  private final String rocksDbPath;
  private String cpPath;
  private String cfDBPath;
  private String saveCompactedFilePath;
  private int maxSnapshots;
  private static final Logger LOG =
      LoggerFactory.getLogger(RocksDBCheckpointDiffer.class);

  // keeps track of all the snapshots created so far.
  private int lastSnapshotCounter;
  private String lastSnapshotPrefix;

  // tracks number of compactions so far
  private static final long UNKNOWN_COMPACTION_GEN = 0;
  private long currentCompactionGen = 0;

  // Something to track all the snapshots created so far.
  private Snapshot[] allSnapshots;

  private static final String COMPACTION_LOG_FILENAME_PREFIX = "compactions";  // TODO: Merge vars
  private static final String COMPACTION_LOG_FILENAME_SUFFIX = ".log";

  // For DB compaction SST DAG persistence and reconstruction
  private static final String CURRENT_COMPACTION_LOG_FILENAME =
      COMPACTION_LOG_FILENAME_PREFIX + COMPACTION_LOG_FILENAME_SUFFIX;

  public static String getCurrentCompactionLogFilename() {
    return CURRENT_COMPACTION_LOG_FILENAME;
  }

  public static String getChkptCompactionLogFilename(String checkpointName) {
//    return COMPACTION_LOG_FILENAME_PREFIX + checkpointName + COMPACTION_LOG_FILENAME_SUFFIX;
    return checkpointName + "-" + CURRENT_COMPACTION_LOG_FILENAME;
  }

  public RocksDBCheckpointDiffer(String dbPath,
                                 int maxSnapshots,
                                 String checkpointPath,
                                 String sstFileSaveDir,
                                 String cfPath,
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
    if (dir.exists()) {
      deleteDirectory(dir);  // TODO: FOR EASE OF TESTING ONLY. DO NOT DELETE DIR WHEN MERGING
    }
    if (!dir.mkdir()) {
      LOG.error("Failed to create SST file backup directory!");
      // TODO: Throw unrecoverable exception and Crash OM ?
      throw new RuntimeException("Failed to create SST file backup directory. "
          + "Check write permission.");
    }

    rocksDbPath = dbPath;
    cfDBPath = cfPath;

    // TODO: This module should be self sufficient in tracking the last
    //  snapshotCounter and currentCompactionGen for a given dbPath. It needs
    //  to be persisted.
    lastSnapshotCounter = initialSnapshotCounter;
    lastSnapshotPrefix = snapPrefix;
    currentCompactionGen = lastSnapshotCounter;

    // TODO: this should also independently persist every compaction e.g.
    //  (input files) ->
    //  {  (output files) + lastSnapshotCounter + currentCompactionGen }
    //  mapping.
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

  // Node in the DAG to represent an SST file
  private class CompactionNode {
    public String fileName;   // Name of the SST file
    public String snapshotId; // The last snapshot that was created before this
    // node came into existance;
    public long snapshotGeneration;
    public long totalNumberOfKeys;
    public long cumulativeKeysReverseTraversal;

    CompactionNode (String f, String sid, long numKeys, long compactionGen) {
      fileName = f;
      snapshotId = sid;
      snapshotGeneration = lastSnapshotCounter;
      totalNumberOfKeys = numKeys;
      cumulativeKeysReverseTraversal = 0;
    }
  }

  private static class Snapshot {
    String dbPath;
    String snapshotID;
    long snapshotGeneration;

    Snapshot(String db, String id, long gen) {
      dbPath = db;
      snapshotID = id;
      snapshotGeneration = gen;
    }
  }

  public enum GType {FNAME, KEYSIZE, CUMUTATIVE_SIZE};


  // Hash table to track Compaction node for a given SST File.
  private ConcurrentHashMap<String, CompactionNode> compactionNodeTable =
      new ConcurrentHashMap<>();

  // We are mainiting a two way DAG. This allows easy traversal from
  // source snapshot to destination snapshot as well as the other direction.
  // TODO : Persist this information to the disk.
  // TODO: A system crash while the edge is inserted in DAGFwd but not in
  //  DAGReverse will compromise the two way DAG. Set of input/output files shud
  //  be written to // disk(RocksDB) first, would avoid this problem.

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
    LOG.warn("Creating Checkpoint for RocksDB instance : " +
        dbPathArg + "in a CheckPoint Location" + cpPathArg);
    try {
      rocksDB.flush(new FlushOptions());
      Checkpoint cp = Checkpoint.create(rocksDB);
      cp.createCheckpoint(cpPathArg);
    } catch (RocksDBException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  public void setRocksDBForCompactionTracking(DBOptions rocksOptions)
      throws RocksDBException {
    setRocksDBForCompactionTracking(rocksOptions,
        new ArrayList<AbstractEventListener>());
  }

  /**
   * This takes DBOptions.
   */
  public void setRocksDBForCompactionTracking(
      DBOptions rocksOptions, List<AbstractEventListener> list) {
    final AbstractEventListener onCompactionCompletedListener =
        new AbstractEventListener() {
          @Override
          @SuppressFBWarnings({
              "AT_OPERATION_SEQUENCE_ON_CONCURRENT_ABSTRACTION",
              "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE"})
          public void onCompactionCompleted(
              final RocksDB db, final CompactionJobInfo compactionJobInfo) {
            synchronized (db) {

              LOG.warn(compactionJobInfo.compactionReason().toString());
              LOG.warn("List of input files:");

              if (compactionJobInfo.inputFiles().size() == 0) {
                LOG.error("Compaction input files list is empty?");
                return;
              }

              final StringBuilder sb = new StringBuilder();

              // kLevelL0FilesNum / kLevelMaxLevelSize. TODO: REMOVE
              sb.append("# ").append(compactionJobInfo.compactionReason()).append('\n');

              // Trim DB path, only keep the SST file name
              final int filenameBegin =
                  compactionJobInfo.inputFiles().get(0).lastIndexOf("/");

              for (String file : compactionJobInfo.inputFiles()) {
                final String fn = file.substring(filenameBegin + 1);
                sb.append(fn).append('\t');  // TODO: Trim last delimiter

                // Create hardlink backups for the SST files that are going
                // to be deleted after this RDB compaction.
                LOG.warn(file);
                String saveLinkFileName =
                    saveCompactedFilePath + new File(file).getName();
                Path link = Paths.get(saveLinkFileName);
                Path srcFile = Paths.get(file);
                try {
                  Files.createLink(link, srcFile);
                } catch (IOException e) {
                  LOG.warn("Exception in creating hardlink");
                  e.printStackTrace();
                }
              }
              sb.append('\n');

              LOG.warn("List of output files:");
              for (String file : compactionJobInfo.outputFiles()) {
                final String fn = file.substring(filenameBegin + 1);
                sb.append(fn).append('\t');
                LOG.warn(file + ",");
              }
              sb.append('\n');

              // Persist infile/outfile to file
              try (BufferedWriter bw = Files.newBufferedWriter(
                  Paths.get(CURRENT_COMPACTION_LOG_FILENAME),
                  StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                bw.write(sb.toString());
                bw.flush();
              } catch (IOException e) {
                throw new RuntimeException(
                    "Failed to append to log for DB compaction tracking", e);
              }

              // Let us also build the graph
//              populateCompactionDAG(compactionJobInfo.inputFiles(),
//                  compactionJobInfo.outputFiles());

//              if (debugEnabled(DEBUG_DAG_BUILD_UP)) {
//                printMutableGraph(null, null, compactionDAGFwd);
//              }
            }
          }
        };

    list.add(onCompactionCompletedListener);
    rocksOptions.setListeners(list);
  }



  public void setRocksDBForCompactionTracking(Options rocksOptions)
      throws RocksDBException {
    setRocksDBForCompactionTracking(rocksOptions,
        new ArrayList<AbstractEventListener>());
  }

  /**
   * This takes Options.
   */
  public void setRocksDBForCompactionTracking(
      Options rocksOptions, List<AbstractEventListener> list) {
    final AbstractEventListener onCompactionCompletedListener =
        new AbstractEventListener() {
          @Override
          @SuppressFBWarnings({
              "AT_OPERATION_SEQUENCE_ON_CONCURRENT_ABSTRACTION",
              "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE"})
          public void onCompactionCompleted(
              final RocksDB db,final CompactionJobInfo compactionJobInfo) {
            synchronized (db) {

              LOG.warn(compactionJobInfo.compactionReason().toString());
              LOG.warn("List of input files:");

              if (compactionJobInfo.inputFiles().size() == 0) {
                LOG.error("Compaction input files list is empty?");
                return;
              }

              final StringBuilder sb = new StringBuilder();

              // kLevelL0FilesNum / kLevelMaxLevelSize
              sb.append("# ").append(compactionJobInfo.compactionReason()).append('\n');

              // Trim DB path, only keep the SST file name
              final int filenameBegin =
                  compactionJobInfo.inputFiles().get(0).lastIndexOf("/");

              for (String file : compactionJobInfo.inputFiles()) {
                final String fn = file.substring(filenameBegin + 1);
                sb.append(fn).append('\t');  // TODO: Trim last delimiter

                LOG.warn(file);
                String saveLinkFileName =
                    saveCompactedFilePath + new File(file).getName();
                Path link = Paths.get(saveLinkFileName);
                Path srcFile = Paths.get(file);
                try {
                  Files.createLink(link, srcFile);
                } catch (IOException e) {
                  LOG.warn("Exception in creating hardlink");
                  e.printStackTrace();
                }
              }
              sb.append('\n');

              LOG.warn("List of output files:");
              for (String file : compactionJobInfo.outputFiles()) {
                final String fn = file.substring(filenameBegin + 1);
                sb.append(fn).append('\t');
                LOG.warn(file);
              }
              sb.append('\n');

              // Persist infile/outfile to file
              try (BufferedWriter bw = Files.newBufferedWriter(
                  Paths.get(CURRENT_COMPACTION_LOG_FILENAME),
                  StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                bw.write(sb.toString());
                bw.flush();
              } catch (IOException e) {
                throw new RuntimeException(
                    "Failed to append to log for DB compaction tracking", e);
              }

              // Let us also build the graph
//              populateCompactionDAG(compactionJobInfo.inputFiles(),
//                  compactionJobInfo.outputFiles());

//              if (debugEnabled(DEBUG_DAG_BUILD_UP)) {
//                printMutableGraph(null, null,
//                    compactionDAGFwd);
//              }
            }
          }
        };

    list.add(onCompactionCompletedListener);
    rocksOptions.setListeners(list);
  }

  public RocksDB getRocksDBInstanceWithCompactionTracking(String dbPath)
      throws RocksDBException {
    final Options opt = new Options().setCreateIfMissing(true)
//        .setWriteBufferSize(1L)  // Default is 64 MB. No idea if this is 1B or 1KB or 1MB. Try it
//        .setMaxWriteBufferNumber(1)  // Default is 2
//        .setCompressionType(CompressionType.NO_COMPRESSION)
//        .setMaxBytesForLevelMultiplier(2)
        ;
    setRocksDBForCompactionTracking(opt);
    return RocksDB.open(opt, dbPath);
  }

  // Get a summary of a given SST file
  public long getSSTFileSummary(String filename)
      throws RocksDBException {
    Options option = new Options();
    SstFileReader reader = new SstFileReader(option);
    try {
      reader.open(saveCompactedFilePath + filename);
    } catch (RocksDBException e) {
      reader.open(rocksDbPath + "/"+ filename);
    }
    TableProperties properties = reader.getTableProperties();
    LOG.warn("getSSTFileSummary " + filename + ":: " +
        properties.getNumEntries());
    return properties.getNumEntries();
  }

  // Read the current Live manifest for a given RocksDB instance (Active or
  // Checkpoint). Returns the list of currently active SST FileNames.
  @SuppressFBWarnings({"NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE"})
  public HashSet<String> readRocksDBLiveFiles(String dbPathArg) {
    RocksDB rocksDB = null;
    HashSet<String> liveFiles = new HashSet<>();
    //
    try (final Options options =
             new Options().setParanoidChecks(true)
                 .setCreateIfMissing(true)
                 .setCompressionType(CompressionType.NO_COMPRESSION)
                 .setForceConsistencyChecks(false)) {
      rocksDB = RocksDB.openReadOnly(options, dbPathArg);
      List<LiveFileMetaData> liveFileMetaDataList =
          rocksDB.getLiveFilesMetaData();
      LOG.warn("Live File Metadata for DB: " + dbPathArg);
      for (LiveFileMetaData m : liveFileMetaDataList) {
        LOG.warn("\tFile :" + m.fileName());
        LOG.warn("\tLevel :" + m.level());
        liveFiles.add(Paths.get(m.fileName()).getFileName().toString());
      }
    } catch (RocksDBException e) {
      e.printStackTrace();
    } finally {
      if (rocksDB != null) {
        rocksDB.close();
      }
    }
    return liveFiles;
  }

  private int getSnapshotIdx(Snapshot src) {
    // Jump an O(n) search (for the UT)
    for (int i = 0; i < allSnapshots.length; i++) {
      if (allSnapshots[i].snapshotID.equals(src.snapshotID)) {
        return i;
      }
    }
    return -1;
  }

  private String[] inputFilesRead;

  /**
   * Process each line of compaction log text file input and populate the DAG.
   * TODO: Drop synchronized? and make this thread safe?
   */
  private synchronized void processCompactionLogLine(String line) {
    LOG.info("Processing line: {}", line);

    // Skip comments
    if (line.startsWith("#")) {
      LOG.info("Skipped comment.");
      return;
    }

    if (inputFilesRead == null) {
      // Store the tokens in the first line
      inputFilesRead = line.split("\t");
      LOG.info("Length of inputFiles = {}", inputFilesRead.length);
      if (inputFilesRead.length == 0) {
        // Sanity check. inputFiles should never be empty. outputFiles could.
        throw new RuntimeException(
            "Compaction inputFiles list is empty. File is corrupted?");
      }
    } else {
      final String[] outputFilesRead = line.split("\t");
      LOG.info("Length of outputFiles = {}", outputFilesRead.length);

      // Populate the compaction DAG
      populateCompactionDAG(inputFilesRead, outputFilesRead);

      // Reset inputFilesRead to null so
      inputFilesRead = null;
    }
  }

  private void loadCompactionDAGOfDBChkpt(String dbCpPath) {

    LOG.info("dbPath={}", dbCpPath);

    // Sanitize DB CP path. Trim ./ at the beginning
    final String prefixToTrim = "./";
    final int trimStart = dbCpPath.indexOf(prefixToTrim);
    if (trimStart >= 0) {
      dbCpPath = dbCpPath.substring(prefixToTrim.length());
    }

    // Current DB checkpoint's compaction log path
    final String currCompactionLogPath =
        getCompactionLogFilenameGivenCpPath(dbCpPath);

    if (!new File(currCompactionLogPath).exists()) {
      LOG.warn("Compaction log does not exist for DB CP, skipping");
      return;
    }

    // Read compaction log
    try (Stream<String> stream =
        Files.lines(Paths.get(currCompactionLogPath), StandardCharsets.UTF_8)) {
      assert(inputFilesRead == null);
      stream.forEach(this::processCompactionLogLine);
      if (inputFilesRead != null) {
        // Sanity check. Temp variable must be null after parsing.
        // Otherwise it means the compaction log is corrupted.
        throw new RuntimeException("Missing output files line. Corrupted?");
      }
    } catch (IOException ioEx) {
      throw new RuntimeException(ioEx);
    }

  }

  /**
   * Given the src and destination Snapshots, it prints a Diff list.
   *
   * Expected index src > dest . e.g. src = 6, dest = 0
   *
   * @param src
   * @param dest
   * @throws RocksDBException
   */
  private synchronized void printSnapdiffSSTFiles(Snapshot src, Snapshot dest) {

    LOG.warn("src db checkpoint:" + src.dbPath);  // from
    HashSet<String> srcSnapFiles = readRocksDBLiveFiles(src.dbPath);
    LOG.warn("dest db checkpoint :" + dest.dbPath);  //to
    HashSet<String> destSnapFiles = readRocksDBLiveFiles(dest.dbPath);

    // In prod code, use Snapshot chain list https://github.com/apache/ozone/pull/3658
    // to get src place in allSnapshots chain
    final int srcIndex = getSnapshotIdx(src);

    // get dest place in allSnapshots chain
    final int destIndex = getSnapshotIdx(dest);

    if (srcIndex < destIndex) {
      // Sanity check
      throw new RuntimeException("Incorrect usage. srcIndex=" + srcIndex + ", "
          + "destIndex=" + destIndex);
    } else if (srcIndex == destIndex) {
      LOG.warn("src and dest are the same");
      return;
    }

    // TODO: Clear dag

    // Load all compaction logs between src (excl) and dest (incl),
    //  with populateCompactionDAG
    final String fromDB = allSnapshots[srcIndex].dbPath;
    System.out.println(fromDB);

    final String toDB = allSnapshots[destIndex].dbPath;
    System.out.println(toDB);

    for (int i = destIndex + 1; i <= srcIndex; i++) {
      final String currDB = allSnapshots[i].dbPath;
      System.out.println(currDB);
      loadCompactionDAGOfDBChkpt(currDB);
    }

    HashSet<String> fwdDAGSameFiles = new HashSet<>();
    HashSet<String> fwdDAGDifferentFiles = new HashSet<>();

    LOG.warn("Doing forward diff between source and destination " +
        "Snapshots:" + src.dbPath + ", " + dest.dbPath);
    realPrintSnapdiffSSTFiles(src, dest, srcSnapFiles, destSnapFiles,
        compactionDAGFwd,
        fwdDAGSameFiles,
        fwdDAGDifferentFiles);

    LOG.warn("Overall Summary \n" +
            "Doing Overall diff between source and destination Snapshots:" +
        src.dbPath + ", " + dest.dbPath);
    System.out.print("fwd DAG Same files :");
    for (String file : fwdDAGSameFiles) {
      System.out.print(file + ", ");
    }
    LOG.warn("");
    System.out.print("\nFwd DAG Different files :");
    for (String file : fwdDAGDifferentFiles) {
      CompactionNode n = compactionNodeTable.get(file);
      System.out.print(file + ", ");
    }
    LOG.warn("");
  }

  @SuppressFBWarnings({"NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE"})
  public synchronized void realPrintSnapdiffSSTFiles(
      Snapshot src, Snapshot dest,
      HashSet<String> srcSnapFiles,
      HashSet<String> destSnapFiles,
      MutableGraph<CompactionNode> mutableGraph,
      HashSet<String> sameFiles, HashSet<String> differentFiles) {


    for (String fileName : srcSnapFiles) {
      if (destSnapFiles.contains(fileName)) {
        LOG.warn("SrcSnapshot : " + src.dbPath + " and Dest " +
            "Snapshot" + dest.dbPath + " Contain Same file " + fileName);
        sameFiles.add(fileName);
        continue;
      }
      CompactionNode infileNode =
          compactionNodeTable.get(Paths.get(fileName).getFileName().toString());
      if (infileNode == null) {
        LOG.warn("SrcSnapshot : " + src.dbPath + "File " + fileName + "was " +
            "never compacted");
        differentFiles.add(fileName);
        continue;
      }
      System.out.print(" Expandin File:" + fileName + ":\n");
      Set<CompactionNode> nextLevel = new HashSet<>();
      nextLevel.add(infileNode);
      Set<CompactionNode> currentLevel = new HashSet<>();
      currentLevel.addAll(nextLevel);
      nextLevel = new HashSet<>();
      int i = 1;
      while (currentLevel.size() != 0) {
        LOG.warn("DAG Level :" + i++);
        for (CompactionNode current : currentLevel) {
          LOG.warn("acknowledging file " + current.fileName);
          if (current.snapshotGeneration <= dest.snapshotGeneration) {
            LOG.warn("Reached dest generation count. SrcSnapshot : " +
                src.dbPath + " and Dest " + "Snapshot" + dest.dbPath +
                " Contain Diffrent file " + current.fileName);
            differentFiles.add(current.fileName);
            continue;
          }
          Set<CompactionNode> successors = mutableGraph.successors(current);
          if (successors == null || successors.size() == 0) {
            LOG.warn("No further compaction for the file" +
                ".SrcSnapshot : " + src.dbPath + " and Dest " +
                "Snapshot" + dest.dbPath + " Contain Diffrent file " +
                current.fileName);
            differentFiles.add(current.fileName);
          } else {
            for (CompactionNode oneSucc : successors) {
              if (sameFiles.contains(oneSucc.fileName) ||
                  differentFiles.contains(oneSucc.fileName)) {
                LOG.warn("Skipping file :" + oneSucc.fileName);
                continue;
              }
              if (destSnapFiles.contains(oneSucc.fileName)) {
                LOG.warn("SrcSnapshot : " + src.dbPath + " and Dest " +
                    "Snapshot" + dest.dbPath + " Contain Same file " +
                    oneSucc.fileName);
                sameFiles.add(oneSucc.fileName);
                continue;
              } else {
                LOG.warn("SrcSnapshot : " + src.dbPath + " and Dest " +
                    "Snapshot" + dest.dbPath + " Contain Diffrent file " +
                    oneSucc.fileName);
                nextLevel.add(oneSucc);
              }
            }
          }
        }
        currentLevel = new HashSet<>();
        currentLevel.addAll(nextLevel);
        nextLevel = new HashSet<>();
        LOG.warn("");
      }
    }
    LOG.warn("Summary :");
    for (String file : sameFiles) {
      System.out.println("Same File: " + file);
    }
//    LOG.warn("");

    for (String file : differentFiles) {
      System.out.println("Different File: " + file);
    }
//    LOG.warn("");
  }

  @SuppressFBWarnings("SIC_INNER_SHOULD_BE_STATIC")
  class NodeComparator implements Comparator<CompactionNode>
  {
    public int compare(CompactionNode a, CompactionNode b)
    {
      return a.fileName.compareToIgnoreCase(b.fileName);
    }

    @Override
    public Comparator<CompactionNode> reversed() {
      return null;
    }
  }


  public void dumpCompactioNodeTable() {
    List<CompactionNode> nodeList =
        compactionNodeTable.values().stream().collect(Collectors.toList());
    Collections.sort(nodeList, new NodeComparator());
    for (CompactionNode n : nodeList ) {
      LOG.warn("File : " + n.fileName + " :: Total keys : "
          + n.totalNumberOfKeys);
      LOG.warn("File : " + n.fileName + " :: Cumulative keys : "  +
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
    System.out.print("\nCompaction Level : " + level + " Expandin File:" +
        fileName + ":\n");
    Set<CompactionNode> nextLevel = new HashSet<>();
    nextLevel.add(infileNode);
    Set<CompactionNode> currentLevel = new HashSet<>();
    currentLevel.addAll(nextLevel);
    int i = 1;
    while (currentLevel.size() != 0) {
      LOG.warn("DAG Level :" + i++);
      for (CompactionNode current : currentLevel) {
        Set<CompactionNode> successors = mutableGraph.successors(current);
        for (CompactionNode oneSucc : successors) {
          System.out.print(oneSucc.fileName + " ");
          nextLevel.add(oneSucc);
        }
      }
      currentLevel = new HashSet<>();
      currentLevel.addAll(nextLevel);
      nextLevel = new HashSet<>();
      LOG.warn("");
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
    Iterator iter = topLevelNodes.iterator();
    while (iter.hasNext()) {
      CompactionNode n = (CompactionNode) iter.next();
      Set<CompactionNode> succ = mutableGraph.successors(n);
      LOG.warn("Parent Node :" + n.fileName);
      if (succ.size() == 0) {
        LOG.warn("No Children Node ");
        allNodes.add(n);
        iter.remove();
        iter = topLevelNodes.iterator();
        continue;
      }
      for (CompactionNode oneSucc : succ) {
        LOG.warn("Children Node :" + oneSucc.fileName);
        if (srcSnapId == null||
            oneSucc.snapshotId.compareToIgnoreCase(destSnapId) == 0) {
          allNodes.add(oneSucc);
        } else {
          topLevelNodes.add(oneSucc);
        }
      }
      iter.remove();
      iter = topLevelNodes.iterator();
    }
    LOG.warn("src snap:" + srcSnapId);
    LOG.warn("dest snap:" + destSnapId);
    for (CompactionNode n : allNodes) {
      LOG.warn("Files are :" + n.fileName);
    }
  }

  private String getCompactionLogFilenameGivenCpPath(String cpPath) {
    return cpPath + "-" + CURRENT_COMPACTION_LOG_FILENAME;
  }

  public void createSnapshot(RocksDB rocksDB) throws InterruptedException {

    LOG.warn("Current time is::" + System.currentTimeMillis());
    long t1 = System.currentTimeMillis();

    cpPath = cpPath + lastSnapshotCounter;
    // Delete the checkpoint dir if it already exists
    File dir = new File(cpPath);
    if (dir.exists()) {
      deleteDirectory(dir);  // TODO: FOR EASE OF TESTING ONLY. DO NOT DELETE DIR WHEN MERGING
    }

    createCheckPoint(rocksDbPath, cpPath, rocksDB);
    allSnapshots[lastSnapshotCounter] = new Snapshot(cpPath,
    lastSnapshotPrefix, lastSnapshotCounter);

    // Rename compactions.log
    final String existingFilename = CURRENT_COMPACTION_LOG_FILENAME;
    final String newFilename = getCompactionLogFilenameGivenCpPath(cpPath);
    try {

      // Dev: Remove target is already exists
      if (new File(newFilename).exists()) {
        new File(newFilename).delete();
      }

      if (new File(existingFilename).exists()) {
        Files.move(Paths.get(existingFilename), Paths.get(newFilename));
      } else {
        // TODO: Create empty file at newFilename.
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    long t2 = System.currentTimeMillis();
    LOG.warn("Current time is::" + t2);

    LOG.warn("millisecond difference is ::" + (t2 - t1));
   Thread.sleep(100);
   ++lastSnapshotCounter;
   lastSnapshotPrefix = "sid_" + lastSnapshotCounter;
   LOG.warn("done :: 1");
  }


  public void printAllSnapshots() throws InterruptedException {
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

  public void diffAllSnapshots() throws InterruptedException, RocksDBException {
    for (Snapshot snap : allSnapshots) {
      if (snap == null) {
        break;
      }
      printSnapdiffSSTFiles(allSnapshots[lastSnapshotCounter - 1], snap);
    }
  }

  public MutableGraph<CompactionNode> getCompactionFwdDAG() {
    return compactionDAGFwd;
  }

  public MutableGraph<CompactionNode> getCompactionReverseDAG() {
    return compactionDAGFwd;
  }

  /**
   * Populate the compaction DAG with input and outout SST files lists.
   */
  private void populateCompactionDAG(String[] inputFiles,
      String[] outputFiles) {

    LOG.info("Populating compaction DAG with lists of input and output files");

    for (String outFilePath : outputFiles) {
      String outfile = Paths.get(outFilePath).getFileName().toString();
      CompactionNode outfileNode = compactionNodeTable.get(outfile);
      if (outfileNode == null) {
        long numKeys = 0;
        try {
          numKeys = getSSTFileSummary(outfile);
        } catch (Exception e) {
          LOG.warn(e.getMessage());
        }
        outfileNode = new CompactionNode(outfile,
            lastSnapshotPrefix,
            numKeys, currentCompactionGen);
        compactionDAGFwd.addNode(outfileNode);
        compactionDAGReverse.addNode(outfileNode);
        compactionNodeTable.put(outfile, outfileNode);
      }

      for (String inFilePath : inputFiles) {
        String infile =
            Paths.get(inFilePath).getFileName().toString();
        CompactionNode infileNode = compactionNodeTable.get(infile);
        if (infileNode == null) {
          long numKeys = 0;
          try {
            numKeys = getSSTFileSummary(infile);
          } catch (Exception e) {
            LOG.warn(e.getMessage());
          }
          infileNode = new CompactionNode(infile,
              lastSnapshotPrefix, numKeys,
              UNKNOWN_COMPACTION_GEN);
          compactionDAGFwd.addNode(infileNode);
          compactionDAGReverse.addNode(infileNode);
          compactionNodeTable.put(infile, infileNode);
        }
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

      List<CompactionNode> nodeList =
        compactionNodeTable.values().stream().collect(Collectors.toList());
    Collections.sort(nodeList, new NodeComparator());

    for (CompactionNode  infileNode : nodeList ) {
      // fist go through fwdGraph to find nodes that don't have succesors.
      // These nodes will be the top level nodes in reverse graph
      Set<CompactionNode> successors = fwdMutableGraph.successors(infileNode);
      if (successors == null || successors.size() == 0) {
        LOG.warn("traverseGraph : No successors. cumulative " +
            "keys : " + infileNode.cumulativeKeysReverseTraversal + "::total " +
            "keys ::" + infileNode.totalNumberOfKeys);
        infileNode.cumulativeKeysReverseTraversal =
            infileNode.totalNumberOfKeys;
      }
    }

    HashSet<CompactionNode> visited = new HashSet<>();
    for (CompactionNode  infileNode : nodeList ) {
      if (visited.contains(infileNode)) {
        continue;
      }
      visited.add(infileNode);
      System.out.print("traverseGraph: Visiting node " + infileNode.fileName +
          ":\n");
      Set<CompactionNode> nextLevel = new HashSet<>();
      nextLevel.add(infileNode);
      Set<CompactionNode> currentLevel = new HashSet<>();
      currentLevel.addAll(nextLevel);
      nextLevel = new HashSet<>();
      int i = 1;
      while (currentLevel.size() != 0) {
        LOG.warn("traverseGraph : DAG Level :" + i++);
        for (CompactionNode current : currentLevel) {
          LOG.warn("traverseGraph : expanding node " + current.fileName);
          Set<CompactionNode> successors =
              reverseMutableGraph.successors(current);
          if (successors == null || successors.size() == 0) {
            LOG.warn("traverseGraph : No successors. cumulative " +
                "keys : " + current.cumulativeKeysReverseTraversal);
          } else {
            for (CompactionNode oneSucc : successors) {
              LOG.warn("traverseGraph : Adding to the next level : " +
                  oneSucc.fileName);
              LOG.warn("traverseGraph : " + oneSucc.fileName + "cum" + " keys"
                  + oneSucc.cumulativeKeysReverseTraversal + "parent" + " " +
                  current.fileName + " total " + current.totalNumberOfKeys);
              oneSucc.cumulativeKeysReverseTraversal +=
                  current.cumulativeKeysReverseTraversal;
              nextLevel.add(oneSucc);
            }
          }
        }
        currentLevel = new HashSet<>();
        currentLevel.addAll(nextLevel);
        nextLevel = new HashSet<>();
        LOG.warn("");
      }
    }
  }

  public boolean debugEnabled(Integer level) {
    return DEBUG_LEVEL.contains(level);
  }
}
