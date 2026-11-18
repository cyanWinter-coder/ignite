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

package org.apache.ignite.internal.processors.affinity;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests {@link GridAffinityAssignmentCache#primaryChanged} which is used to validate
 * near cache entries against the affinity assignment history.
 */
public class GridAffinityAssignmentCachePrimaryChangedTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).setBackups(1));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Checks that {@code primaryChanged} is consistent with the actual difference
     * between two affinity assignments.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPrimaryChangedConsistentWithAssignments() throws Exception {
        IgniteEx grid0 = startGrid(0);

        AffinityTopologyVersion ver1 = new AffinityTopologyVersion(1, 0);

        startGrid(1);

        awaitPartitionMapExchange();

        GridAffinityAssignmentCache affCache = affinityCache(grid0);

        AffinityTopologyVersion ver2 = affCache.lastVersion();

        AffinityAssignment a1 = affCache.cachedAffinity(ver1);
        AffinityAssignment a2 = affCache.cachedAffinity(ver2);

        boolean changed = false;
        boolean same = false;

        for (int part = 0; part < a1.assignment().size(); part++) {
            ClusterNode primary1 = a1.get(part).get(0);
            ClusterNode primary2 = a2.get(part).get(0);

            boolean expChanged = !primary1.equals(primary2);

            assertEquals("Unexpected primaryChanged result [part=" + part + ']',
                expChanged, affCache.primaryChanged(part, ver1, ver2));

            changed |= expChanged;
            same |= !expChanged;
        }

        // Sanity: with two nodes and default rendezvous affinity both cases should be present.
        assertTrue("Expected partitions with changed primary", changed);
        assertTrue("Expected partitions with unchanged primary", same);
    }

    /**
     * Checks that {@code primaryChanged} reports "changed" when the requested start version
     * is not found in the affinity assignment history: in this case it is impossible to confirm
     * that the primary node did not change, so cached (near cache) values must not be served.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPrimaryChangedUnknownStartVersion() throws Exception {
        IgniteEx grid0 = startGrid(0);

        startGrid(1);

        awaitPartitionMapExchange();

        GridAffinityAssignmentCache affCache = affinityCache(grid0);

        AffinityTopologyVersion lastVer = affCache.lastVersion();

        // Version that was never calculated on this node is not present in the history.
        AffinityTopologyVersion unknownVer = new AffinityTopologyVersion(lastVer.topologyVersion() + 100, 0);

        assertFalse(affCache.cachedVersions().contains(unknownVer));

        assertTrue("Unverifiable version must be reported as changed",
            affCache.primaryChanged(0, unknownVer, lastVer));
    }

    /**
     * @param ignite Ignite.
     * @return Affinity assignment cache for the default cache group.
     */
    private GridAffinityAssignmentCache affinityCache(IgniteEx ignite) {
        CacheGroupContext gctx = ignite.context().cache().cache(DEFAULT_CACHE_NAME).context().group();

        return gctx.affinity();
    }
}
