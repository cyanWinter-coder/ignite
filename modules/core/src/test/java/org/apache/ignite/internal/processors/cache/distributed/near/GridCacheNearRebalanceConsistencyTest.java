/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests that a client near cache does not serve stale values while partitions
 * are re-balanced on node join/leave, and that partitions are distributed
 * according to the affinity function after rebalancing completes.
 */
public class GridCacheNearRebalanceConsistencyTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "near-rebalance-consistency";

    /** Cache name for the backed-up partition loss check. */
    private static final String BACKED_UP_CACHE_NAME = "with-backups";

    /** Cache name for the unbacked-up partition loss check. */
    private static final String NO_BACKUPS_CACHE_NAME = "no-backups";

    /** Keys count. */
    private static final int KEYS_CNT = 200;

    /** Reader threads count. */
    private static final int READER_THREADS = 3;

    /** Server nodes. */
    private final List<IgniteEx> servers = new ArrayList<>();

    /** Whether to configure a near cache for the node being started. */
    private boolean nearCfg;

    /** Reader threads stop flag. */
    private volatile boolean stopReaders;

    /** Error reported by a reader thread. */
    private final AtomicReference<Throwable> readerErr = new AtomicReference<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<Integer, Integer>(CACHE_NAME)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setBackups(1);

        if (nearCfg)
            ccfg.setNearConfiguration(new NearCacheConfiguration<>());

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        servers.clear();
        stopReaders = false;
        readerErr.set(null);

        super.afterTest();
    }

    /**
     * Checks that near cache reads return up-to-date values while a node joins and leaves the cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNodeJoinLeaveNearCacheConsistency() throws Exception {
        servers.add(startGrid(0));
        servers.add(startGrid(1));

        nearCfg = true;

        IgniteEx client = startClientGrid(2);

        nearCfg = false;

        IgniteCache<Integer, Integer> clientCache = client.cache(CACHE_NAME);

        IgniteCache<Integer, Integer> srvCache = grid(0).cache(CACHE_NAME);

        for (int i = 0; i < KEYS_CNT; i++)
            srvCache.put(i, i);

        // Populate the near cache on the client.
        for (int i = 0; i < KEYS_CNT; i++)
            assertEquals(i, (int)clientCache.get(i));

        boolean nearPopulated = false;

        for (int i = 0; i < KEYS_CNT; i++)
            nearPopulated |= clientCache.localPeek(i, CachePeekMode.NEAR) != null;

        assertTrue("Near cache was not populated on the client", nearPopulated);

        // Values written after the near cache was populated must be visible.
        for (int i = 0; i < KEYS_CNT; i++)
            srvCache.put(i, i + 1);

        for (int i = 0; i < KEYS_CNT; i++)
            assertEquals("Stale value after update [key=" + i + ']', i + 1, (int)clientCache.get(i));

        // Node join: partitions are migrated to the new node while the client keeps reading.
        IgniteEx joined = startGrid(3);

        servers.add(joined);

        List<Thread> readers = startReaders(clientCache, 1);

        try {
            awaitPartitionMapExchange();
        }
        finally {
            stopReaders(readers);
        }

        assertPartitionDistribution();

        // Node leave: partitions are migrated back to the remaining nodes while the client keeps reading.
        for (int i = 0; i < KEYS_CNT; i++)
            srvCache.put(i, i + 2);

        for (int i = 0; i < KEYS_CNT; i++)
            assertEquals("Stale value after update [key=" + i + ']', i + 2, (int)clientCache.get(i));

        stopGrid(3);

        servers.remove(joined);

        readers = startReaders(clientCache, 2);

        try {
            awaitPartitionMapExchange();
        }
        finally {
            stopReaders(readers);
        }

        for (int i = 0; i < KEYS_CNT; i++)
            assertEquals("Stale value after node leave [key=" + i + ']', i + 2, (int)clientCache.get(i));

        assertPartitionDistribution();
    }

    /**
     * Checks that a partition is reported as lost only when neither its primary nor backup copies remain alive.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionLossOnlyForUnbackedUpData() throws Exception {
        startGrids(3);

        IgniteEx stopped = grid(2);

        servers.add(grid(0));
        servers.add(grid(1));
        servers.add(stopped);

        // The caches are created dynamically so that this test does not affect the caches of the
        // node join/leave test above (a cache without backups would have owner-less partitions there).
        IgniteCache<Integer, Integer> backedUpCache = grid(0).createCache(
            new CacheConfiguration<Integer, Integer>(BACKED_UP_CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                .setBackups(1)
                .setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_SAFE));

        IgniteCache<Integer, Integer> noBackupsCache = grid(0).createCache(
            new CacheConfiguration<Integer, Integer>(NO_BACKUPS_CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                .setBackups(0)
                .setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_SAFE));

        for (int i = 0; i < KEYS_CNT; i++) {
            backedUpCache.put(i, i);
            noBackupsCache.put(i, i);
        }

        stopGrid(2);

        servers.remove(stopped);

        GridTestUtils.waitForCondition(() -> !noBackupsCache.lostPartitions().isEmpty(), 30_000);

        // The backed-up cache keeps at least one copy of every partition, so no data loss is reported.
        assertTrue("Backed-up partitions must not be reported as lost", backedUpCache.lostPartitions().isEmpty());

        // The cache without backups loses the partitions that were stored on the stopped node.
        assertFalse("Unbacked-up partitions must be reported as lost", noBackupsCache.lostPartitions().isEmpty());
    }

    /**
     * Starts reader threads that verify that the given value is always returned from the near cache.
     *
     * @param cache Cache.
     * @param expValIncrement Expected value increment (the initial value is the key).
     * @return Reader threads.
     */
    private List<Thread> startReaders(IgniteCache<Integer, Integer> cache, final int expValIncrement) {
        stopReaders = false;
        readerErr.set(null);

        List<Thread> threads = new ArrayList<>(READER_THREADS);

        for (int t = 0; t < READER_THREADS; t++) {
            Thread thread = new Thread(() -> {
                try {
                    while (!stopReaders) {
                        for (int i = 0; i < KEYS_CNT; i++) {
                            Integer val = cache.get(i);

                            if (val == null || val != i + expValIncrement)
                                throw new AssertionError("Stale value during rebalance [key=" + i +
                                    ", exp=" + (i + expValIncrement) + ", act=" + val + ']');
                        }
                    }
                }
                catch (Throwable e) {
                    readerErr.compareAndSet(null, e);
                }
            }, "near-rebalance-reader-" + t);

            thread.setDaemon(true);
            thread.start();

            threads.add(thread);
        }

        return threads;
    }

    /**
     * Stops reader threads and checks that no stale value was observed.
     *
     * @param threads Reader threads.
     * @throws Exception If failed.
     */
    private void stopReaders(List<Thread> threads) throws Exception {
        stopReaders = true;

        for (Thread thread : threads)
            thread.join(30_000);

        Throwable err = readerErr.get();

        if (err != null)
            throw new AssertionError("Near cache returned a stale value during rebalancing", err);
    }

    /**
     * Checks that the set of nodes owning each partition matches the affinity function assignment.
     */
    private void assertPartitionDistribution() {
        Affinity<Object> aff = grid(0).affinity(CACHE_NAME);

        for (int part = 0; part < aff.partitions(); part++) {
            Set<UUID> expOwners = new HashSet<>();

            for (ClusterNode node : aff.mapPartitionToPrimaryAndBackups(part))
                expOwners.add(node.id());

            Set<UUID> owners = new HashSet<>();

            for (IgniteEx srv : servers) {
                GridDhtPartitionState state = srv.context().cache().cache(CACHE_NAME).context().topology()
                    .partitionState(srv.localNode().id(), part);

                if (state == GridDhtPartitionState.OWNING)
                    owners.add(srv.localNode().id());
            }

            assertEquals("Partition owners do not match affinity assignment [part=" + part + ']',
                expOwners, owners);
        }
    }
}
