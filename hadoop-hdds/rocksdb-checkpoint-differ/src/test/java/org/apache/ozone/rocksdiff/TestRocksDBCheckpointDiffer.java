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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.DEBUG_DAG_LIVE_NODES;
import static org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.DEBUG_READ_ALL_DB_KEYS;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

public class TestRocksDBCheckpointDiffer {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRocksDBCheckpointDiffer.class);

  private static final String testDBPath = "./rocksdb-data";
  private static final int NUM_ROW = 250000;
  private static final int SNAPSHOT_EVERY_SO_MANY_KEYS = 49999;

  /**
   * Graph type.
   */
  enum GType {
    FNAME,
    KEYSIZE,
    CUMUTATIVE_SIZE
  }

  @BeforeAll
  public static void init() {
    // Set differ log level to DEBUG
    GenericTestUtils.setLogLevel(RocksDBCheckpointDiffer.getLog(), Level.DEBUG);
  }

  @Test
  void testMain() throws Exception {

    // Delete the compaction log dir if it already exists
    File dir = new File("./compaction-log");
    if (dir.exists()) {
      deleteDirectory(dir);
    }

    TestRocksDBCheckpointDiffer tester = new TestRocksDBCheckpointDiffer();
    RocksDBCheckpointDiffer differ = new RocksDBCheckpointDiffer(
        "./rocksdb-data",
        1000,
        "./rocksdb-data-cp",
        "./SavedCompacted_Files/", 0,
        "snap_id_");

    RocksDB rocksDB = tester.createRocksDBInstance(testDBPath, differ);
    tester.readRocksDBInstance(testDBPath, rocksDB, null, differ);

    differ.printAllSnapshots();
    differ.traverseGraph(
        differ.getCompactionReverseDAG(),
        differ.getCompactionFwdDAG());

    differ.diffAllSnapshots();
    differ.dumpCompactionNodeTable();

    for (GType gtype : GType.values()) {
      String fname = "fwdGraph_" + gtype.toString() +  ".png";
      String rname = "reverseGraph_"+ gtype.toString() + ".png";

//      differ.pngPrintMutableGrapth(differ.getCompactionFwdDAG(),
//          fname, gtype);
//      differ.pngPrintMutableGrapth(differ.getCompactionReverseDAG(), rname,
//          gtype);
    }

    rocksDB.close();
  }

  private String getRandomString(Random random, int length) {
    // Ref: https://www.baeldung.com/java-random-string
    final int leftLimit = 48; // numeral '0'
    final int rightLimit = 122; // letter 'z'

    return random.ints(leftLimit, rightLimit + 1)
        .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
        .limit(length)
        .collect(StringBuilder::new,
            StringBuilder::appendCodePoint, StringBuilder::append)
        .toString();
  }

  // Test Code to create sample RocksDB instance.
  public RocksDB createRocksDBInstance(String dbPathArg,
                                       RocksDBCheckpointDiffer differ)
      throws RocksDBException, InterruptedException {

    LOG.debug("Creating RocksDB at '{}'", dbPathArg);

    // Delete the test DB dir if it exists
    File dir = new File(dbPathArg);
    if (dir.exists()) {
      deleteDirectory(dir);
    }

    RocksDB rocksDB;
    rocksDB = differ.getRocksDBInstanceWithCompactionTracking(dbPathArg);

    differ.setCompactionLogParentDir(".");
    differ.setCurrentCompactionLog(rocksDB.getLatestSequenceNumber());

    Random random = new Random();
    // key-value
    for (int i = 0; i < NUM_ROW; ++i) {
      String generatedString = getRandomString(random, 7);
      String keyStr = "Key-" + i + "-" + generatedString;
      String valueStr = "Val-" + i + "-" + generatedString;
      byte[] key = keyStr.getBytes(UTF_8);
      rocksDB.put(key, valueStr.getBytes(UTF_8));
      if (i % SNAPSHOT_EVERY_SO_MANY_KEYS == 0) {
        differ.createSnapshot(rocksDB);
      }
    }
    differ.createSnapshot(rocksDB);
    return rocksDB;
  }

  static boolean deleteDirectory(java.io.File directoryToBeDeleted) {
    File[] allContents = directoryToBeDeleted.listFiles();
    if (allContents != null) {
      for (java.io.File file : allContents) {
        if (!deleteDirectory(file)) {
          return false;
        }
      }
    }
    return directoryToBeDeleted.delete();
  }

  //  RocksDB.DEFAULT_COLUMN_FAMILY
  private void UpdateRocksDBInstance(String dbPathArg, RocksDB rocksDB) {
    System.out.println("Updating RocksDB instance at :" + dbPathArg);
    //
    try (final Options options =
             new Options().setCreateIfMissing(true).
                 setCompressionType(CompressionType.NO_COMPRESSION)) {
      if (rocksDB == null) {
        rocksDB = RocksDB.open(options, dbPathArg);
      }

      Random random = new Random();
      // key-value
      for (int i = 0; i< NUM_ROW; ++i) {
        String generatedString = getRandomString(random, 7);
        String keyStr = " MyUpdated" + generatedString + "StringKey" + i;
        String valueStr = " My Updated" + generatedString + "StringValue" + i;
        byte[] key = keyStr.getBytes(UTF_8);
        rocksDB.put(key, valueStr.getBytes(UTF_8));
        System.out.println(toStr(rocksDB.get(key)));
      }
    } catch (RocksDBException e) {
      e.printStackTrace();
    }
  }

  //  RocksDB.DEFAULT_COLUMN_FAMILY
  public void testDefaultColumnFamilyOriginal() {
    System.out.println("testDefaultColumnFamily begin...");
    //
    try (final Options options = new Options().setCreateIfMissing(true)) {
      try (final RocksDB rocksDB = RocksDB.open(options, "./rocksdb-data")) {
        // key-value
        byte[] key = "Hello".getBytes(UTF_8);
        rocksDB.put(key, "World".getBytes(UTF_8));

        System.out.println(toStr(rocksDB.get(key)));

        rocksDB.put("SecondKey".getBytes(UTF_8), "SecondValue".getBytes(UTF_8));

        // List
        List<byte[]> keys = Arrays.asList(key, "SecondKey".getBytes(UTF_8),
            "missKey".getBytes(UTF_8));
        List<byte[]> values = rocksDB.multiGetAsList(keys);
        for (int i = 0; i < keys.size(); i++) {
          System.out.println("multiGet " + toStr(keys.get(i)) + ":" +
              (values.get(i) != null ? toStr(values.get(i)) : null));
        }

        // [key - value]
        RocksIterator iter = rocksDB.newIterator();
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
          System.out.println("iterator key:" + toStr(iter.key()) + ", " +
              "iter value:" + toStr(iter.value()));
        }

        // key
        rocksDB.delete(key);
        System.out.println("after remove key:" + toStr(key));

        iter = rocksDB.newIterator();
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
          System.out.println("iterator key:" + toStr(iter.key()) + ", " +
              "iter value:" + toStr(iter.value()));
        }
      }
    } catch (RocksDBException e) {
      e.printStackTrace();
    }
  }

  // (table)
  public void testCertainColumnFamily() {
    System.out.println("\ntestCertainColumnFamily begin...");
    try (final ColumnFamilyOptions cfOpts =
             new ColumnFamilyOptions().optimizeUniversalStyleCompaction()) {
      String cfName = "my-first-columnfamily";
      // list of column family descriptors, first entry must always be
      // default column family
      final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
          new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
          new ColumnFamilyDescriptor(cfName.getBytes(UTF_8), cfOpts)
      );

      List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
      try (final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true).
          setCreateMissingColumnFamilies(true);
           final RocksDB rocksDB = RocksDB.open(dbOptions, "./rocksdb-data-cf" +
               "/", cfDescriptors, cfHandles)) {
        ColumnFamilyHandle cfHandle = cfHandles.stream().filter(x -> {
          try {
            return (toStr(x.getName())).equals(cfName);
          } catch (RocksDBException e) {
            return false;
          }
        }).collect(Collectors.toList()).get(0);

        // key/value
        String key = "FirstKey";
        rocksDB.put(cfHandles.get(0), key.getBytes(UTF_8),
            "FirstValue".getBytes(UTF_8));
        // key
        byte[] getValue = rocksDB.get(cfHandles.get(0), key.getBytes(UTF_8));
        System.out.println("get Value : " + toStr(getValue));
        // key/value
        rocksDB.put(cfHandles.get(1), "SecondKey".getBytes(UTF_8),
            "SecondValue".getBytes(UTF_8));

        List<byte[]> keys = Arrays.asList(key.getBytes(UTF_8),
            "SecondKey".getBytes(UTF_8));
        List<ColumnFamilyHandle> cfHandleList = Arrays.asList(cfHandle,
            cfHandle);
        // key
        List<byte[]> values = rocksDB.multiGetAsList(cfHandleList, keys);
        for (int i = 0; i < keys.size(); i++) {
          System.out.println("multiGet:" + toStr(keys.get(i)) + "--" +
              (values.get(i) == null ? null : toStr(values.get(i))));
        }
        //rocksDB.compactRange();
        //rocksDB.compactFiles();
        List<LiveFileMetaData> liveFileMetaDataList =
            rocksDB.getLiveFilesMetaData();
        for (LiveFileMetaData m : liveFileMetaDataList) {
          System.out.println("Live File Metadata");
          System.out.println("\tFile :" + m.fileName());
          System.out.println("\tTable :" + toStr(m.columnFamilyName()));
          System.out.println("\tKey Range :" + toStr(m.smallestKey()) +
              " " + "<->" + toStr(m.largestKey()));
        }
        // key
        rocksDB.delete(cfHandle, key.getBytes(UTF_8));

        // key
        RocksIterator iter = rocksDB.newIterator(cfHandle);
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
          System.out.println("Iterator:" + toStr(iter.key()) + ":" +
              toStr(iter.value()));
        }
      } finally {
        // NOTE frees the column family handles before freeing the db
        for (final ColumnFamilyHandle cfHandle : cfHandles) {
          cfHandle.close();
        }
      }
    } catch (RocksDBException e) {
      e.printStackTrace();
    } // frees the column family options
  }

  // Read from a given RocksDB instance and optionally write all the
  // keys to a given file.
  //
  public void readRocksDBInstance(String dbPathArg, RocksDB rocksDB,
                                  FileWriter file,
                                  RocksDBCheckpointDiffer differ) {

    LOG.info("Reading RocksDB: " + dbPathArg);
    boolean createdDB = false;

    try (final Options options = new Options()
        .setParanoidChecks(true)
        .setForceConsistencyChecks(false)) {

      if (rocksDB == null) {
        rocksDB = RocksDB.openReadOnly(options, dbPathArg);
        createdDB = true;
      }

      List<LiveFileMetaData> liveFileMetaDataList =
          rocksDB.getLiveFilesMetaData();
      for (LiveFileMetaData m : liveFileMetaDataList) {
        LOG.info("SST File: " + m.fileName());
        LOG.info("\tLevel: " + m.level());
        LOG.info("\tTable: " + toStr(m.columnFamilyName()));
        LOG.info("\tKey Range: " + toStr(m.smallestKey())
            + " <-> " + toStr(m.largestKey()));
        if (differ.debugEnabled(DEBUG_DAG_LIVE_NODES)) {
          differ.printMutableGraphFromAGivenNode(m.fileName(), m.level(),
              differ.getCompactionFwdDAG());
        }
      }

      if(differ.debugEnabled(DEBUG_READ_ALL_DB_KEYS)) {
        RocksIterator iter = rocksDB.newIterator();
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
          System.out.println("Iterator key:" + toStr(iter.key()) + ", " +
              "iter value:" + toStr(iter.value()));
          if (file != null) {
            file.write("iterator key:" + toStr(iter.key()) + ", iter " +
                "value:" + toStr(iter.value()));
            file.write("\n");
          }
        }
      }
    } catch (IOException | RocksDBException e) {
      e.printStackTrace();
    } finally {
      if (createdDB){
        rocksDB.close();
      }
    }
  }

  /**
   * Return String object encoded in UTF-8 from a byte array.
   */
  private String toStr(byte[] bytes) {
    return new String(bytes, UTF_8);
  }
}