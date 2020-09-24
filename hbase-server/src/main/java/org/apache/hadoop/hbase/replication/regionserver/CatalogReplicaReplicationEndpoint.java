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
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
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
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.AsyncTableRegionLocator;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.FlushRequester;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.util.AtomicUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
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

  private Configuration conf;
  private AsyncClusterConnection connection;

  private int numRetries;

  private long operationTimeoutNs;

  // Primary region
  public Region region;

  private CatalogReplicaShipper[] replicaShippers;
  private byte[][] replicatedFamilies;
  private long skippedEdits;
  private long shippedEdits;

  public CatalogReplicaReplicationEndpoint () {
    replicatedFamilies = new byte[][] { HConstants.CATALOG_FAMILY };
  }

  public CatalogReplicaReplicationEndpoint (final Region region) {
    this.region = region;
  }

  public long getShippedEdits() {
    return shippedEdits;
  }

  public long getSkippedEdits() {
    return skippedEdits;
  }

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

    // Initialize replicaShippers, not all replica location are filled in at this moment.
    int numReplicas = region.getTableDescriptor().getRegionReplication();
    assert(numReplicas > 1);

    replicaShippers = new CatalogReplicaShipper[numReplicas - 1];

    for (int i = 1; i < numReplicas; i ++) {
      // It leaves each ReplicaShipper to resolve its replica region location.
      replicaShippers[i - 1] = new CatalogReplicaShipper(this, this.connection, null,
        RegionInfoBuilder.newBuilder(region.getRegionInfo()).setReplicaId(i).build());

      replicaShippers[i -1].start();
    }
  }

  /**
   * returns true if the specified entry must be replicated. We should always replicate meta
   * operations (e.g. flush) and use the user HTD flag to decide whether or not replicate the
   * memstore.
   */
  private boolean requiresReplication(final WALEdit edit) {

    // TODO: let meta edits go through. Need to filter out flush events which are not for
    // targetting families.
    if (edit.isMetaEdit()) {
      return true;
    }
    // empty edit does not need to be replicated
    if (edit.isEmpty()) {
      return false;
    }

    for (byte[] fam : replicatedFamilies) {
      if (edit.getFamilies().contains(fam)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public boolean replicate(ReplicateContext replicateContext) {
    // Breaks up meta event COMMIT_FLUSH
    List<Entry> origList = replicateContext.getEntries().stream().collect(Collectors.toList());
    ListIterator<Entry> itr = origList.listIterator();
    List<List<Entry>> entries = new ArrayList<>();
    int startIndex = 0;
    List<Entry> subList;

    // If there are START_FLUSH and COMMIT_FLUSH event in the edits, it needs to be separated so it can
    // be processed accordingly.
    while (itr.hasNext()) {
      Entry e = itr.next();
      WALEdit edit = e.getEdit();
      if (!requiresReplication(edit)) {
        skippedEdits ++;
        itr.remove();
        continue;
      }
      shippedEdits ++;
      int editIndex = itr.previousIndex();

      // seperate COMMIT_FLUSH into its own list.
      if (edit.isMetaEdit()) {
        try {
          WALProtos.FlushDescriptor flushDesc = WALEdit.getFlushDescriptor(edit.getCells().get(0));
          if (flushDesc != null) {
            if (editIndex> startIndex) {
              entries.add(origList.subList(startIndex, editIndex));
            }
            subList = new ArrayList<>(1);
            subList.add(e);
            entries.add(subList);
            startIndex = editIndex + 1;
          }
        } catch (IOException ioe) {
          LOG.warn("Failed to parse {}", e.getEdit().getCells().get(0));
        }
      }
    }

    if (startIndex == 0) {
      if (!origList.isEmpty()) {
        entries.add(origList);
      }
    } else {
      if (startIndex < origList.size()) {
        entries.add(origList.subList(startIndex, origList.size()));
      }
    }

    for (List<Entry> l : entries) {
      for (CatalogReplicaShipper shipper : replicaShippers) {
        shipper.addEditEntry(l);
      }
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
    // Stop shippers
    for (CatalogReplicaShipper s : replicaShippers) {
      s.setRunning(false);
    }
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
    private volatile boolean isRunning = true;
    private HRegionLocation location = null;
    private RegionInfo regionInfo;
    private int numRetries;
    private long operationTimeoutNs;
    private AsyncTableRegionLocator locator;

    CatalogReplicaShipper (final CatalogReplicaReplicationEndpoint endpoint,
      final AsyncClusterConnection connection, final HRegionLocation loc,
      final RegionInfo regionInfo) {
      this.endpoint = endpoint;
      this.conn = connection;
      this.location = loc;
      this.regionInfo = regionInfo;
      this.queue = new LinkedBlockingQueue<>();
      this.numRetries = endpoint.getNumRetries();
      this.operationTimeoutNs = endpoint.getOperationTimeoutNs();
      locator = conn.getRegionLocator(regionInfo.getTable());
    }

    private void cleanQueue(final long startFlushSeqOpId) {
      // remove entries with seq# less than startFlushSeqOpId

      // TODO: right now, it is kind of simplified. It assumes one family in the queue.
      // In the future, if more families are involved, it needs to have family into
      // consideration.
      Iterator<List<Entry>> iterator = queue.iterator();
      while (iterator.hasNext()) {
        List<Entry> l = iterator.next();
        for (Entry ll : l) {
          LOG.info("LSHX " + ll.getEdit());
          LOG.info("LSHX " + ll.getKey());
        }
      }

      // TODO: if there are multiple edits in the list, need to remove them one by one.
      queue.removeIf(n -> (n.get(0).getKey().getSequenceId() < startFlushSeqOpId));

      iterator = queue.iterator();
      while (iterator.hasNext()) {
        List<Entry> l = iterator.next();
        for (Entry ll : l) {
          LOG.info("LLSHX " + ll.getEdit());
          LOG.info("LLSHX " + ll.getKey());
        }
      }

    }

    public void addEditEntry(final List<Entry> entries) {
      try {
        if ((entries.size() == 1) && entries.get(0).getEdit().isMetaEdit()) {
          try {
            // When it is a COMMIT_FLUSH event, it can remove all edits in the queue whose
            // seq# is less flushSeq#. Those events include flush events and
            // OPEN_REGION/CLOSE_REGION events, as they work like flush events.
            WALProtos.FlushDescriptor flushDesc = WALEdit.getCommitFlushDescriptor(
              entries.get(0).getEdit().getCells().get(0));
            if (flushDesc != null) {
              LOG.info("SHX1, cleaned the queue" + flushDesc.getFlushSequenceNumber());
              cleanQueue(flushDesc.getFlushSequenceNumber());
            }
          } catch (IOException ioe) {
            LOG.warn("Failed to parse {}", entries.get(0).getEdit().getCells().get(0));
          }
        }
        queue.put(entries);
      } catch (InterruptedException e) {

      }
    }

    private CompletableFuture<Long> replicate(List<Entry> entries) {
      CompletableFuture<Long> future = new CompletableFuture<>();
      FutureUtils
        .addListener(conn.replay(regionInfo.getTable(), regionInfo.getEncodedNameAsBytes(), entries,
          regionInfo.getReplicaId(), 1, operationTimeoutNs, location), (r, e) -> {
          if (e != null) {
            LOG.warn("Failed to replicate to {}", regionInfo, e);
            future.completeExceptionally(e);
          } else {
            future.complete(r.longValue());
          }
        });

      return future;
    }

    @Override
    public final void run() {
      boolean reload = false;
      while (isRunning()) { // we only loop back here if something fatal happened to our stream
        if (location == null) {
          // If location is null, keep searching for it. This applies to replica region server
          // crashes, and waiting to be assigned. reload flag will be set accordingly in case of
          // error.
          try {
            location = locator.getRegionLocation(regionInfo.getStartKey(),
              regionInfo.getReplicaId(), reload).get();
          } catch (ExecutionException ee) {
            LOG.warn("Error getting location for meta region " +regionInfo + ", " + ee);
            reload = true;
            continue;
          } catch (InterruptedException ie) {
            LOG.warn("Interrupted while getting location for meta region " +regionInfo + ", " + ie);
            Thread.currentThread().interrupt();
          }
        }
        try {
          Thread.sleep(150000);
          List<Entry> entries = queue.poll(20000, TimeUnit.MILLISECONDS);
          if (entries == null) {
            continue;
          }

          CompletableFuture<Long> future =
            replicate(entries);

          try {
            future.get();
          } catch (InterruptedException e) {
            // restore the interrupted state
            Thread.currentThread().interrupt();
            continue;
          } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            LOG.warn("Failed to replicate {} entries for region  of meta table", entries.size());
            // If Exception is NotServingRegion, it needs to reload location.
            if (cause instanceof NotServingRegionException) {
              location = null;
              reload = true;
            }
            // TODO: do we give up the current edits.
          }
        } catch (InterruptedException | ReplicationRuntimeException e) {
          // It is interrupted and needs to quit.
          LOG.warn("Interrupted while waiting for next replication entry batch", e);
          Thread.currentThread().interrupt();
        }
      }
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

