/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.junit.Before;
import org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStats;
import org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsRequestBuilder;
import org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.index.remote.RemoteRefreshSegmentTracker;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.remote.RemoteTranslogTracker;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 3)
public class RemoteStoreStatsIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "remote-store-test-idx-1";
    private long timeBeforeIndexing;

    @Before
    public void setup() {
        setupRepo();
        timeBeforeIndexing  = RemoteStoreUtils.getCurrentSystemNanoTime() / 1_000_000L;
    }

    public void testStatsResponseFromAllNodes() {

        // Step 1 - We create cluster, create an index, and then index documents into. We also do multiple refreshes/flushes
        // during this time frame. This ensures that the segment upload has started.
        createIndex(INDEX_NAME, remoteStoreIndexSettings(0));
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        indexDocs();

        // Step 2 - We find all the nodes that are present in the cluster. We make the remote store stats api call from
        // each of the node in the cluster and check that the response is coming as expected.
        ClusterState state = getClusterState();
        List<String> nodes = state.nodes().getNodes().values().stream().map(DiscoveryNode::getName).collect(Collectors.toList());
        String shardId = "0";
        for (String node : nodes) {
            RemoteStoreStatsResponse response = client(node).admin().cluster().prepareRemoteStoreStats(INDEX_NAME, shardId).get();
            assertTrue(response.getSuccessfulShards() > 0);
            assertTrue(response.getShards() != null && response.getShards().length != 0);
            final String indexShardId = String.format(Locale.ROOT, "[%s][%s]", INDEX_NAME, shardId);
            List<RemoteStoreStats> matches = Arrays.stream(response.getShards())
                .filter(stat -> indexShardId.equals(stat.getSegmentStats().shardId.toString()))
                .collect(Collectors.toList());
            assertEquals(1, matches.size());
            RemoteRefreshSegmentTracker.Stats segmentStats = matches.get(0).getSegmentStats();
            matches = Arrays.stream(response.getShards())
                .filter(stat -> indexShardId.equals(stat.getTranslogStats().shardId.toString()))
                .collect(Collectors.toList());
            assertEquals(1, matches.size());
            RemoteTranslogTracker.Stats translogStats = matches.get(0).getTranslogStats();
            assertResponseStats(segmentStats, translogStats);
        }
    }

    public void testStatsResponseAllShards() {

        // Step 1 - We create cluster, create an index, and then index documents into. We also do multiple refreshes/flushes
        // during this time frame. This ensures that the segment upload has started.
        createIndex(INDEX_NAME, remoteStoreIndexSettings(0, 3));
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        indexDocs();

        // Step 2 - We find all the nodes that are present in the cluster. We make the remote store stats api call from
        // each of the node in the cluster and check that the response is coming as expected.
        ClusterState state = getClusterState();
        String node = state.nodes().getDataNodes().values().stream().map(DiscoveryNode::getName).findFirst().get();
        RemoteStoreStatsRequestBuilder remoteStoreStatsRequestBuilder = client(node).admin()
            .cluster()
            .prepareRemoteStoreStats(INDEX_NAME, null);
        RemoteStoreStatsResponse response = remoteStoreStatsRequestBuilder.get();
        assertTrue(response.getSuccessfulShards() == 3);
        assertTrue(response.getShards() != null && response.getShards().length == 3);
        RemoteRefreshSegmentTracker.Stats segmentStats = response.getShards()[0].getSegmentStats();
        RemoteTranslogTracker.Stats translogStats = response.getShards()[0].getTranslogStats();
        assertResponseStats(segmentStats, translogStats);
    }

    public void testStatsResponseFromLocalNode() {

        // Step 1 - We create cluster, create an index, and then index documents into. We also do multiple refreshes/flushes
        // during this time frame. This ensures that the segment upload has started.
        createIndex(INDEX_NAME, remoteStoreIndexSettings(0, 3));
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        indexDocs();

        // Step 2 - We find a data node in the cluster. We make the remote store stats api call from
        // each of the data node in the cluster and check that only local shards are returned.
        ClusterState state = getClusterState();
        List<String> nodes = state.nodes().getDataNodes().values().stream().map(DiscoveryNode::getName).collect(Collectors.toList());
        for (String node : nodes) {
            RemoteStoreStatsRequestBuilder remoteStoreStatsRequestBuilder = client(node).admin()
                .cluster()
                .prepareRemoteStoreStats(INDEX_NAME, null);
            remoteStoreStatsRequestBuilder.setLocal(true);
            RemoteStoreStatsResponse response = remoteStoreStatsRequestBuilder.get();
            assertTrue(response.getSuccessfulShards() == 1);
            assertTrue(response.getShards() != null && response.getShards().length == 1);
            RemoteRefreshSegmentTracker.Stats segmentStats = response.getShards()[0].getSegmentStats();
            RemoteTranslogTracker.Stats translogStats = response.getShards()[0].getTranslogStats();
            assertResponseStats(segmentStats, translogStats);
        }
    }

    private void indexDocs() {
        // Indexing documents along with refreshes and flushes.
        for (int i = 0; i < randomIntBetween(5, 10); i++) {
            if (randomBoolean()) {
                flush(INDEX_NAME);
            } else {
                refresh(INDEX_NAME);
            }
            int numberOfOperations = randomIntBetween(20, 50);
            for (int j = 0; j < numberOfOperations; j++) {
                indexSingleDoc(INDEX_NAME);
            }
        }
    }

    private void assertResponseStats(RemoteRefreshSegmentTracker.Stats segmentStats, RemoteTranslogTracker.Stats translogStats) {
        assertResponseSegmentStats(segmentStats);
        assertResponseTranslogStats(translogStats);
    }

    private void assertResponseSegmentStats(RemoteRefreshSegmentTracker.Stats segmentStats) {
        assertEquals(0, segmentStats.refreshTimeLagMs);
        assertEquals(segmentStats.localRefreshNumber, segmentStats.remoteRefreshNumber);
        assertTrue(segmentStats.uploadBytesStarted > 0);
        assertEquals(0, segmentStats.uploadBytesFailed);
        assertTrue(segmentStats.uploadBytesSucceeded > 0);
        assertTrue(segmentStats.totalUploadsStarted > 0);
        assertEquals(0, segmentStats.totalUploadsFailed);
        assertTrue(segmentStats.totalUploadsSucceeded > 0);
        assertEquals(0, segmentStats.rejectionCount);
        assertEquals(0, segmentStats.consecutiveFailuresCount);
        assertEquals(0, segmentStats.bytesLag);
        assertTrue(segmentStats.uploadBytesMovingAverage > 0);
        assertTrue(segmentStats.uploadBytesPerSecMovingAverage > 0);
        assertTrue(segmentStats.uploadTimeMovingAverage > 0);
    }

    private void assertResponseTranslogStats(RemoteTranslogTracker.Stats translogStats) {
        assertTrue(translogStats.lastUploadTimestamp > timeBeforeIndexing);
        assertTrue(translogStats.totalUploadsStarted > 0);
        assertTrue(translogStats.totalUploadsSucceeded > 0);
        assertEquals(0, translogStats.totalUploadsFailed);
        assertTrue(translogStats.uploadBytesStarted > 0);
        assertTrue(translogStats.uploadBytesSucceeded > 0);
        assertEquals(0, translogStats.uploadBytesFailed);
        assertTrue(translogStats.totalUploadTimeInMillis > 0);
        assertTrue(translogStats.uploadBytesMovingAverage > 0);
        assertTrue(translogStats.uploadBytesPerSecMovingAverage > 0);
        assertTrue(translogStats.uploadTimeMovingAverage > 0);
    }
}
