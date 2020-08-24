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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.FlushRequester;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.util.AtomicUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.hadoop.hbase.replication.ReplicationUtils.sleepForRetries;

/**
 * A {@link org.apache.hadoop.hbase.replication.ReplicationEndpoint} endpoint which receives the WAL
 * edits from the WAL, and sends the edits to replicas of regions.
 */

// CatalogReplicaReplicationEndpoint is based on BaseReplicationEndpoint as it does not need
// zookeeper support.
@InterfaceAudience.Private
public class CatalogReplicaReplicationEndpoint extends BaseReplicationEndpoint {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogReplicaReplicationEndpoint.class);

  // Can be configured differently than hbase.client.retries.number
  private static String CLIENT_RETRIES_NUMBER =
    "hbase.region.replica.replication.client.retries.number";

  private Configuration conf;
  private AsyncClusterConnection connection;

  private int numRetries;

  private long operationTimeoutNs;

  private final RetryCounterFactory retryCounterFactory =
    new RetryCounterFactory(Integer.MAX_VALUE, 1000, 60000);

  private CatalogReplicaShipper[] replicaShippers;

  @Override
  public void init(Context context) throws IOException {
    super.init(context);
    this.conf = context.getConfiguration();
    // HRS multiplies client retries by 10 globally for meta operations, but we do not want this.
    // We are resetting it here because we want default number of retries (35) rather than 10 times
    // that which makes very long retries for disabled tables etc.
    int defaultNumRetries = conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    if (defaultNumRetries > 10) {
      int mult = conf.getInt(HConstants.HBASE_CLIENT_SERVERSIDE_RETRIES_MULTIPLIER,
        HConstants.DEFAULT_HBASE_CLIENT_SERVERSIDE_RETRIES_MULTIPLIER);
      defaultNumRetries = defaultNumRetries / mult; // reset if HRS has multiplied this already
    }
    this.numRetries = conf.getInt(CLIENT_RETRIES_NUMBER, defaultNumRetries);
    // use the regular RPC timeout for replica replication RPC's
    this.operationTimeoutNs =
      TimeUnit.MILLISECONDS.toNanos(conf.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
        HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT));
    this.connection = context.getServer().getAsyncClusterConnection();
  }

  /**
   * returns true if the specified entry must be replicated. We should always replicate meta
   * operations (e.g. flush) and use the user HTD flag to decide whether or not replicate the
   * memstore.
   */
  private boolean requiresReplication(Entry entry) {
    // empty edit does not need to be replicated
    if (entry.getEdit().isEmpty()) {
      return false;
    }
    // meta edits (e.g. flush) must be always replicated
    return entry.getEdit().isMetaEdit();
  }

  @Override
  public boolean replicate(ReplicateContext replicateContext) {
    Iterator<Entry> itr = replicateContext.getEntries().iterator();
    while (itr.hasNext()) {
      if (!requiresReplication(itr.next())) {
        itr.remove();
      }
      // TODO: need to break start_flush and commit_flush, do clean up as necessary.
      if (false) {

      }
    }

    for (CatalogReplicaShipper shipper : replicaShippers) {
      shipper.addEditEntry(replicateContext.getEntries());
    }
    return true;
  }

  @Override
  public UUID getPeerUUID() {
    return this.ctx.getClusterId();
  }

  @Override
  public boolean canReplicateToSameCluster() {
    return true;
  }

  @Override
  protected WALEntryFilter getScopeWALEntryFilter() {
    // we do not care about scope. We replicate everything.
    return null;
  }

  @Override
  public void start() {
    startAsync();
  }

  @Override
  public void stop() {
    stopAsync();
  }

  @Override
  protected void doStart() {
    notifyStarted();
  }

  @Override
  protected void doStop() {
    notifyStopped();
  }

  public int getNumRetries() {
    return numRetries;
  }

  public long getOperationTimeoutNs() {
    return operationTimeoutNs;
  }

  private static class CatalogReplicaShipper extends Thread {
    private final CatalogReplicaReplicationEndpoint endpoint;
    private LinkedBlockingQueue<List<Entry>> queue;
    private final AsyncClusterConnection conn;
    //Indicates whether this particular worker is running
    private boolean isRunning = true;
    private HRegionLocation location;
    private final TableDescriptor td = null;
    private RegionInfo regionInfo;
    private int numRetries;
    private long operationTimeoutNs;


    CatalogReplicaShipper (final CatalogReplicaReplicationEndpoint endpoint,
      final AsyncClusterConnection connection) {
      this.endpoint = endpoint;
      this.conn = connection;
      this.queue = new LinkedBlockingQueue<>();
      this.numRetries = endpoint.getNumRetries();
      this.operationTimeoutNs = endpoint.getOperationTimeoutNs();

    }

    private void cleanQueue(final long startFlushSeqOpId) {
      // remove entries with seq# less than startFlushSeqOpId
      // TODO: needs more touchup.
      queue.removeIf(n -> (n.get(0).getKey().getSequenceId() < startFlushSeqOpId));
    }

    // TODO: this needs revisit.
    public void addEditEntry(final List<Entry> entries) {
      try {
        // TODO: for start_flush/commit_flush, it is going to be only one walEdit in the batch
        // logic such as following needs to be added.
        //  if (entries.get(0).getKey is commit_flush) {
        //    cleanQueue(0);
        //  }
        queue.put(entries);
      } catch (InterruptedException e) {

      }
    }

    private CompletableFuture<Long> replicate(List<Entry> entries) {
      CompletableFuture<Long> future = new CompletableFuture<>();
      // For MetaFamily such flush events, the key is the start key of the region,
      // just reuse same thing here.
      byte[] row = CellUtil.cloneRow(entries.get(0).getEdit().getCells().get(0));
      FutureUtils
        .addListener(conn.replay(td.getTableName(), regionInfo.getEncodedNameAsBytes(),
          row, entries, regionInfo.getReplicaId(), 1, operationTimeoutNs), (r, e) -> {
          if (e != null) {
            LOG.warn("Failed to replicate to {}", regionInfo, e);
            future.completeExceptionally(e);
            // TODO: need to refresh meta location in case of error.
          } else {
            future.complete(r.longValue());
          }
        });

      return future;
    }

    @Override
    public final void run() {
      while (isRunning()) { // we only loop back here if something fatal happened to our stream
        try {
          List<Entry> entries = queue.poll(20000, TimeUnit.MILLISECONDS);
          if (entries == null) {
            continue;
          }

          CompletableFuture<Long> future =
            replicate(entries);

          try {
            future.get().longValue();
          } catch (InterruptedException e) {
            // restore the interrupted state
            Thread.currentThread().interrupt();
            continue;
          } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            LOG.warn("Failed to replicate {} entries for region  of meta table", entries.size());
          }
        } catch (InterruptedException | ReplicationRuntimeException e) {
          // It is interrupted and needs to quit.
          LOG.warn("Interrupted while waiting for next replication entry batch", e);
          Thread.currentThread().interrupt();
        }
      }
      // TODO: what if it breaks the loop now.
    }

    /**
     * @return whether the thread is running
     */
    public boolean isRunning() {
      return isRunning && !isInterrupted();
    }

    /**
     * @param running the running to set
     */
    public void setRunning(boolean running) {
      this.isRunning = running;
    }

  }
}

  private void replicate(CompletableFuture<Long> future, RegionLocations locs,
    TableDescriptor tableDesc, byte[] encodedRegionName, byte[] row, List<Entry> entries) {
    if (locs.size() == 1) {
      // Could this happen?
      future.complete(Long.valueOf(entries.size()));
      return;
    }
    RegionInfo defaultReplica = locs.getDefaultRegionLocation().getRegion();
    if (!Bytes.equals(defaultReplica.getEncodedNameAsBytes(), encodedRegionName)) {
      // the region name is not equal, this usually means the region has been split or merged, so
      // give up replicating as the new region(s) should already have all the data of the parent
      // region(s).
      if (LOG.isTraceEnabled()) {
        LOG.trace(
          "Skipping {} entries in table {} because located region {} is different than" +
            " the original region {} from WALEdit",
          tableDesc.getTableName(), defaultReplica.getEncodedName(),
          Bytes.toStringBinary(encodedRegionName));
      }
      future.complete(Long.valueOf(entries.size()));
      return;
    }
    AtomicReference<Throwable> error = new AtomicReference<>();
    AtomicInteger remainingTasks = new AtomicInteger(locs.size() - 1);
    AtomicLong skippedEdits = new AtomicLong(0);

    for (int i = 1, n = locs.size(); i < n; i++) {
      // Do not use the elements other than the default replica as they may be null. We will fail
      // earlier if the location for default replica is null.
      final RegionInfo replica = RegionReplicaUtil.getRegionInfoForReplica(defaultReplica, i);
      FutureUtils
        .addListener(connection.replay(tableDesc.getTableName(), replica.getEncodedNameAsBytes(),
          row, entries, replica.getReplicaId(), numRetries, operationTimeoutNs), (r, e) -> {
          if (e != null) {
            LOG.warn("Failed to replicate to {}", replica, e);
            error.compareAndSet(null, e);
          } else {
            AtomicUtils.updateMax(skippedEdits, r.longValue());
          }
          if (remainingTasks.decrementAndGet() == 0) {
            if (error.get() != null) {
              future.completeExceptionally(error.get());
            } else {
              future.complete(skippedEdits.get());
            }
          }
        });
    }
  }

  private void logSkipped(TableName tableName, List<Entry> entries, String reason) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Skipping {} entries because table {} is {}", entries.size(), tableName, reason);
      for (Entry entry : entries) {
        LOG.trace("Skipping : {}", entry);
      }
    }
  }


