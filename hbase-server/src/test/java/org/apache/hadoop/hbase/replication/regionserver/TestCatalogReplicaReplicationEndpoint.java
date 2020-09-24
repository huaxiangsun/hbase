/**
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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ReplicationPeerNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.testclassification.FlakeyTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Tests CatalogReplicaReplicationEndpoint class by setting up region replicas and verifying
 * async wal replication replays the edits to the secondary region in various scenarios.
 */
@Category({FlakeyTests.class, LargeTests.class})
public class TestCatalogReplicaReplicationEndpoint {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCatalogReplicaReplicationEndpoint.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestCatalogReplicaReplicationEndpoint.class);

  private static final int NB_SERVERS = 2;

  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();
  private static final int endRowNum = 1000;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void beforeClass() throws Exception {
    Configuration conf = HTU.getConfiguration();
    conf.setFloat("hbase.regionserver.logroll.multiplier", 0.0003f);
    conf.setInt("replication.source.size.capacity", 10240);
    conf.setLong("replication.source.sleepforretries", 100);
    conf.setInt("hbase.regionserver.maxlogs", 10);
    conf.setLong("hbase.master.logcleaner.ttl", 10);
    conf.setInt("zookeeper.recovery.retry", 1);
    conf.setInt("zookeeper.recovery.retry.intervalmill", 10);
    conf.setBoolean(ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_CONF_KEY, true);
    conf.setLong(HConstants.THREAD_WAKE_FREQUENCY, 100);
    conf.setInt("replication.stats.thread.period.seconds", 5);
    conf.setBoolean("hbase.tests.use.shortcircuit.reads", false);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 5); // less number of retries is needed
    conf.setInt(HConstants.HBASE_CLIENT_SERVERSIDE_RETRIES_MULTIPLIER, 1);

    HTU.startMiniCluster(NB_SERVERS);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HTU.shutdownMiniCluster();
  }

  public void testRegionReplicaReplication(final String tname, int regionReplication) throws Exception {
    // test region replica replication. Create a table with single region, write some data
    // ensure that data is replicated to the secondary region
    TableName tableName = TableName.valueOf(tname);

    //TableDescriptor htd = HTU.createTableDescriptor(TableName.valueOf(tableName.toString()),
    //  regionReplication, HConstants.CATALOG_FAMILY);

    TableDescriptor htd = HTU.createTableDescriptor(TableName.valueOf(tableName.toString()),
      regionReplication, new byte[][] {HConstants.CATALOG_FAMILY, HConstants.TABLE_FAMILY}, 1);

    // This implies that there is only one region per table.
    HTU.getAdmin().createTable(htd);
    Connection connection = ConnectionFactory.createConnection(HTU.getConfiguration());
    Table table = connection.getTable(tableName);

    try {
      // load the data to the table
      HTU.loadNumericRows(table, HConstants.CATALOG_FAMILY, 0, endRowNum);
      HTU.loadNumericRows(table, HConstants.TABLE_FAMILY, 0, endRowNum);
      // HConstants.TABLE_FAMILY is filtered out.
      Thread.sleep(2000);
      assertEquals(0L, getReplicaRegionMemstoreSize(tableName, regionReplication,
        HConstants.TABLE_FAMILY));
      verifyReplication(tableName, regionReplication, 0, endRowNum, HConstants.CATALOG_FAMILY);
      HTU.flush(tableName);
      Thread.sleep(2000);

      // After flush, everything is picked up.
      verifyReplication(tableName, regionReplication, 0, endRowNum, HConstants.CATALOG_FAMILY);
      verifyReplication(tableName, regionReplication, 0, endRowNum, HConstants.TABLE_FAMILY);

    } finally {
      table.close();
      connection.close();
      connection.close();
    }
  }

  private long getReplicaRegionMemstoreSize(TableName tableName, int regionReplication,
    byte[] family) {

    final Region[] regions = new Region[regionReplication];

    for (int i = 0; i < NB_SERVERS; i++) {
      HRegionServer rs = HTU.getMiniHBaseCluster().getRegionServer(i);
      List<HRegion> onlineRegions = rs.getRegions(tableName);
      for (HRegion region : onlineRegions) {
        assertNull(regions[region.getRegionInfo().getReplicaId()]);
        regions[region.getRegionInfo().getReplicaId()] = region;
      }
    }

    for (Region region : regions) {
      assertNotNull(region);
    }

    long size = 0;
    for (int i = 1; i < regionReplication; i++) {
      final Region region = regions[i];
      Store s = region.getStore(family);
      assertNotNull(s);
      size += s.getMemStoreSize().getDataSize();
    }
    return size;
  }

  private void verifyReplication(TableName tableName, int regionReplication,
      final int startRow, final int endRow, final byte[] family) throws Exception {
    verifyReplication(tableName, regionReplication, startRow, endRow, family,true);
  }

  private void verifyReplication(TableName tableName, int regionReplication,
      final int startRow, final int endRow, final byte[] family, final boolean present) throws Exception {
    // find the regions
    final Region[] regions = new Region[regionReplication];

    for (int i=0; i < NB_SERVERS; i++) {
      HRegionServer rs = HTU.getMiniHBaseCluster().getRegionServer(i);
      List<HRegion> onlineRegions = rs.getRegions(tableName);
      for (HRegion region : onlineRegions) {
        regions[region.getRegionInfo().getReplicaId()] = region;
      }
    }

    for (Region region : regions) {
      assertNotNull(region);
    }

    for (int i = 1; i < regionReplication; i++) {
      final Region region = regions[i];
      // wait until all the data is replicated to all secondary regions
      Waiter.waitFor(HTU.getConfiguration(), 90000, 1000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          LOG.info("verifying replication for region replica:" + region.getRegionInfo());
          try {
            HTU.verifyNumericRows(region, family, startRow, endRow, present);
          } catch(Throwable ex) {
            LOG.warn("Verification from secondary region is not complete yet", ex);
            // still wait
            return false;
          }
          return true;
        }
      });
    }
  }

  @Test
  public void testRegionReplicaReplicationWith2Replicas() throws Exception {
    //testRegionReplicaReplication(name.getMethodName(), 2);
    testRegionReplicaReplication("testRegionReplicaReplicationWithReplicas", 2);
  }

  @Test
  public void testRegionReplicaReplicationWith3Replicas() throws Exception {
    //testRegionReplicaReplication(name.getMethodName(), 3);
    testRegionReplicaReplication("testRegionReplicaReplicationWithReplicas", 3);
  }

  @Test
  public void testRegionReplicaReplicationWith10Replicas() throws Exception {
    //testRegionReplicaReplication(name.getMethodName(), 10);
    testRegionReplicaReplication("testRegionReplicaReplicationWithReplicas", 10);
  }

  @Test
  public void testRegionReplicaReplicationForFlushAndCompaction() throws Exception {
    // Tests a table with region replication 3. Writes some data, and causes flushes and
    // compactions. Verifies that the data is readable from the replicas. Note that this
    // does not test whether the replicas actually pick up flushed files and apply compaction
    // to their stores
    //TableName tableName = TableName.valueOf(name.getMethodName());
    TableName tableName = TableName.valueOf("testRegionReplicaReplicationWithReplicas");
    int regionReplication = 2;

    TableDescriptor htd = HTU.createTableDescriptor(tableName,
      regionReplication, new byte[][] {HConstants.CATALOG_FAMILY, HConstants.TABLE_FAMILY}, 1);

    // This implies that there is only one region per table.
    HTU.getAdmin().createTable(htd);
    Connection connection = ConnectionFactory.createConnection(HTU.getConfiguration());
    Table table = connection.getTable(tableName);

    try {
      // load the data to the table

      for (int i = 0; i < 5; i ++) {
        LOG.info("Writing data from " + i + " to " + (i+1));
        HTU.loadNumericRows(table, HConstants.CATALOG_FAMILY, i, i+1);
        LOG.info("flushing table");
        Thread loader = new Thread() {
          @Override
          public void run() {
            for (int i = 0; i < 10; i ++) {
              try {
                Thread.sleep(50);
                LOG.info("shxWriting data from " + (i + 1000) + " to " + (i + 1001));
                HTU.loadNumericRows(table, HConstants.CATALOG_FAMILY, i + 1000, i + 1001);
              } catch (IOException ee) {

              } catch (InterruptedException e) {

              }
            }
          }
        };
        loader.start();
        HTU.flush(tableName);
        LOG.info("compacting table");
        if (i < 4) {
          HTU.compact(tableName, false);
        }
      }

      verifyReplication(tableName, regionReplication, 0, 6000,
        HConstants.CATALOG_FAMILY);
    } finally {
      table.close();
      connection.close();
    }
  }
}
