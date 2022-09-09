/**
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
package org.apache.hadoop.ozone.om.snapshot;

import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import org.rocksdb.AbstractEventListener;
import org.rocksdb.CompactionJobInfo;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileReader;
import org.rocksdb.TableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * RocksDB checkpoint helper methods.
 *
 * This isolates rocksdb imports, and provides the listener to be attached
 * during OM main DB load (in OmMetadataManagerImpl#loadDB).
 *
 * Questions:
 * 1. How often does OM DB currently do compaction? Every X writes?
 */
public class RDBCheckpointHelper {

  private static final Logger LOG =
      LoggerFactory.getLogger(RDBCheckpointHelper.class);

  private final String rocksDbPath;
  private String cpPath;
  private final String cfdbPath;
  private final String SaveCompactedFilePath;
  private final int MAX_SNAPSHOTS;

  // keeps track of all the snapshots created so far.
  private int lastSnapshotCounter;
  private String lastSnapshotPrefix;

  // tracks number of compactions so far
  private long UNKNOWN_COMPACTION_GEN = 0;
  private long currentCompactionGen = 0;

  // Something to track all the snapshots created so far.
  private Snapshot[] allSnapshots;

  // DAG

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
    RocksDB.loadLibrary();  // TODO: REMOVE THIS?
  }

  public static void addDebugLevel(Integer level) {
    DEBUG_LEVEL.add(level);
  }

  public RDBCheckpointHelper(String dbPath,
      int max_snapshots,
      String checkpointPath,
      String sstFileSaveDir,
      String cfPath,
      int initialSnapshotCounter,
      String snapPrefix) {

    LOG.info("Init RDBCheckpointHelper.");

    MAX_SNAPSHOTS = max_snapshots;
    allSnapshots = new Snapshot[MAX_SNAPSHOTS];
    cpPath = checkpointPath;

    SaveCompactedFilePath = sstFileSaveDir;
    // Empty the directory
    File dir = new File(SaveCompactedFilePath);
    if (dir.exists()) {
      deleteDirectory(dir);
    }
    dir.mkdir();

    rocksDbPath = dbPath;
    cfdbPath = cfPath;

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

  // Ref: https://www.baeldung.com/java-delete-directory
  private static boolean deleteDirectory(File directoryToBeDeleted) {
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
    public long compactionGeneration;
    public long totalNumberOfKeys;
    public long cumulativeKeysReverseTraversal;

    CompactionNode (String f, String sid, long numKeys, long compactionGen) {
      fileName = f;
      snapshotId = sid;
      snapshotGeneration = lastSnapshotCounter;
      totalNumberOfKeys = numKeys;
      cumulativeKeysReverseTraversal = 0;
      compactionGeneration = compactionGen;
    }
  }

  private class Snapshot {
    String dbPath;
    String snapshotID;
    long snapshotGeneration;

    Snapshot(String db, String id, long gen) {
      dbPath = db;
      snapshotID = id;
      snapshotGeneration = gen;
    }
  }

  /**
   * Graph type enum.
   */
  public enum GType {
    FNAME,
    KEYSIZE,
    CUMUTATIVE_SIZE
  };

  public boolean debugEnabled(Integer level) {
    return DEBUG_LEVEL.contains(level);
  }

  // Get a summary of a given SST file
  public long getSSTFileSummary(String filename)
      throws RocksDBException {
    Options option = new Options();
    SstFileReader reader = new SstFileReader(option);
//    reader.newIterator(null).seek();
    try {
      reader.open(SaveCompactedFilePath + filename);
    } catch (RocksDBException e) {
      reader.open(rocksDbPath + "/"+ filename);
    }
    TableProperties properties = reader.getTableProperties();
    System.out.println("getSSTFileSummary " + filename + ":: " +
        properties.getNumEntries());
    return properties.getNumEntries();
  }


  public synchronized void printMutableGraph(String srcSnapId,
      String destSnapId,
      MutableGraph<CompactionNode> mutableGraph) {
    System.out.println("Printing the Graph");
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
      System.out.println("Parent Node :" + n.fileName);
      if (succ.size() == 0) {
        System.out.println("No Children Node ");
        allNodes.add(n);
        iter.remove();
        iter = topLevelNodes.iterator();
        continue;
      }
      for (CompactionNode oneSucc : succ) {
        System.out.println("Children Node :" + oneSucc.fileName);
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
    System.out.println("src snap:" + srcSnapId);
    System.out.println("dest snap:" + destSnapId);
    for (CompactionNode n : allNodes) {
      System.out.println("Files are :" + n.fileName);
    }
  }

  public AbstractEventListener newCompactionCompletedListener() {
    return new AbstractEventListener() {
      @Override
      public void onCompactionCompleted(final RocksDB db, final CompactionJobInfo compactionJobInfo) {

        try (FileOutputStream out = new FileOutputStream("/tmp/dblistener1")) {
          out.write("listener triggered for onCompactionCompleted.".getBytes());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        synchronized (db) {
          System.out.println(compactionJobInfo.compactionReason());
          System.out.println("List of input files:");
          for (String file : compactionJobInfo.inputFiles()) {
            System.out.println(file);
            String saveLinkFileName =
                SaveCompactedFilePath + new File(file).getName();
            Path link = Paths.get(saveLinkFileName);
            Path srcFile = Paths.get(file);
            try {
              Files.createLink(link, srcFile);
            } catch (IOException e) {
              System.out.println("Exception in creating hardlink:");
              e.printStackTrace();
            }
          }
          System.out.println("List of output files:");
          for (String file : compactionJobInfo.outputFiles()) {
            System.out.println(file);
          }
          // Let us also build the graph
          for (String outFilePath : compactionJobInfo.outputFiles()) {
            String outfile = Paths.get(outFilePath).getFileName().toString();
            CompactionNode outfileNode = compactionNodeTable.get(outfile);
            if (outfileNode == null) {
              try {
                outfileNode = new CompactionNode(outfile, lastSnapshotPrefix,
                    getSSTFileSummary(outfile), currentCompactionGen);
              } catch (Exception e) {
                System.out.println(e.getMessage());
              }
              compactionDAGFwd.addNode(outfileNode);
              compactionDAGReverse.addNode(outfileNode);
              compactionNodeTable.put(outfile, outfileNode);
            }
            for (String inFilePath : compactionJobInfo.inputFiles()) {
              String infile = Paths.get(inFilePath).getFileName().toString();
              CompactionNode infileNode = compactionNodeTable.get(infile);
              if (infileNode == null) {
                try {
                  infileNode = new CompactionNode(infile, lastSnapshotPrefix,
                      getSSTFileSummary(infile), UNKNOWN_COMPACTION_GEN);
                } catch (Exception e) {
                  System.out.println(e.getMessage());
                }
                compactionDAGFwd.addNode(infileNode);
                compactionDAGReverse.addNode(infileNode);
                compactionNodeTable.put(infile, infileNode);
              }
              if (outfileNode.fileName.compareToIgnoreCase(infileNode.fileName) != 0) {
                compactionDAGFwd.putEdge(outfileNode, infileNode);
                compactionDAGReverse.putEdge(infileNode, outfileNode);
              }
            }
          }
          if (debugEnabled(DEBUG_DAG_BUILD_UP)) {
            printMutableGraph(null, null, compactionDAGFwd);
          }
        }
      }
    };
  }
}
