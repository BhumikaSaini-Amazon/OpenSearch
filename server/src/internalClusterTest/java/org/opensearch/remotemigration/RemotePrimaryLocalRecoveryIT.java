/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotemigration;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.client.Client;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.remote.RemoteSegmentStats;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

import java.util.Map;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemotePrimaryLocalRecoveryIT extends MigrationBaseTestCase {

    /**
     * Tests local recovery sanity in the happy path flow
     * @throws Exception
     */
    public void testLocalRecoveryRollingRestart() throws Exception {
        triggerRollingRestartForRemoteMigration();
    }

    /**
     * Tests local recovery sanity during remote migration with a node restart in between
     * @throws Exception
     */
    public void testLocalRecoveryRollingRestartAndNodeFailure() throws Exception {
        triggerRollingRestartForRemoteMigration();

        DiscoveryNodes discoveryNodes = internalCluster().client().admin().cluster().prepareState().get().getState().getNodes();
        String nodeToRestart = discoveryNodes.getClusterManagerNodeId();
        internalCluster().restartNode(nodeToRestart);

        Map<ShardRouting, ShardStats> shardStatsMap = internalCluster().client().admin().indices().prepareStats("idx1").get().asMap();
        shardStatsMap.forEach((shardRouting, shardStats) -> {
            if (nodeToRestart.equals(shardRouting.currentNodeId())) {
                RemoteSegmentStats remoteSegmentStats = shardStats.getStats().getSegments().getRemoteSegmentStats();
                assertTrue(remoteSegmentStats.getTotalUploadTime() > 0);
                assertTrue(remoteSegmentStats.getUploadBytesSucceeded() > 0);
            }
        });
    }

    /**
     * Tests local recovery sanity during remote migration with a node replacement in between
     * @throws Exception
     */
    public void testLocalRecoveryRollingRestartAndNodeReplacement() throws Exception {
        triggerRollingRestartForRemoteMigration();

        internalCluster().stopCurrentClusterManagerNode();
        String newNode = internalCluster().startNode();

        Map<ShardRouting, ShardStats> shardStatsMap = internalCluster().client().admin().indices().prepareStats("idx1").get().asMap();
        shardStatsMap.forEach((shardRouting, shardStats) -> {
            if (newNode.equals(shardRouting.currentNodeId())) {
                RemoteSegmentStats remoteSegmentStats = shardStats.getStats().getSegments().getRemoteSegmentStats();
                assertTrue(remoteSegmentStats.getTotalUploadTime() > 0);
                assertTrue(remoteSegmentStats.getUploadBytesSucceeded() > 0);
            }
        });
    }

    /**
     * Tests local recovery sanity during remote migration with all node replacement in between
     * @throws Exception
     */
    public void testLocalRecoveryRollingRestartAndAllNodeReplacement() throws Exception {
        triggerRollingRestartForRemoteMigration();

        internalCluster().stopAllNodes();
        String newNode1 = internalCluster().startNode();
        String newNode2 = internalCluster().startNode();

        Map<ShardRouting, ShardStats> shardStatsMap = internalCluster().client().admin().indices().prepareStats("idx1").get().asMap();
        DiscoveryNodes discoveryNodes = internalCluster().client().admin().cluster().prepareState().get().getState().getNodes();
        shardStatsMap.forEach((shardRouting, shardStats) -> {
            if (discoveryNodes.get(shardRouting.currentNodeId()).isRemoteStoreNode()) {
                RemoteSegmentStats remoteSegmentStats = shardStats.getStats().getSegments().getRemoteSegmentStats();
                assertTrue(remoteSegmentStats.getTotalUploadTime() > 0);
                assertTrue(remoteSegmentStats.getUploadBytesSucceeded() > 0);
            }
        });
    }

    /**
     * Helper method to run a rolling restart for migration to remote backed cluster
     * @throws Exception
     */
    private void triggerRollingRestartForRemoteMigration() throws Exception {
        String docRepNode = internalCluster().startNode();
        Client client = internalCluster().client(docRepNode);

        // create index
        client().admin().indices().prepareCreate("idx1").setSettings(indexSettings()).setMapping("field", "type=text").get();
        ensureGreen("idx1");

        indexBulk("idx1", randomIntBetween(10, 100));
        refresh("idx1");

        // Index some more docs
        indexBulk("idx1", randomIntBetween(10, 100));

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // add remote node in mixed mode cluster
        addRemote = true;
        String remoteNode = internalCluster().startNode();
        internalCluster().validateClusterFormed();

        updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(MIGRATION_DIRECTION_SETTING.getKey(), "remote_store"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // rolling restart
        final Settings remoteNodeAttributes = remoteStoreClusterSettings(
            REPOSITORY_NAME,
            segmentRepoPath,
            REPOSITORY_2_NAME,
            translogRepoPath
        );
        internalCluster().rollingRestart(new InternalTestCluster.RestartCallback() {
            // Update remote attributes
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                return remoteNodeAttributes;
            }
        });

        ensureStableCluster(2);
        ensureGreen("idx1");
        assertEquals(internalCluster().size(), 2);

        // Assert on remote uploads
        Map<ShardRouting, ShardStats> shardStatsMap = internalCluster().client().admin().indices().prepareStats("idx1").get().asMap();
        DiscoveryNodes discoveryNodes = internalCluster().client().admin().cluster().prepareState().get().getState().getNodes();
        shardStatsMap.forEach((shardRouting, shardStats) -> {
            if (discoveryNodes.get(shardRouting.currentNodeId()).isRemoteStoreNode()) {
                RemoteSegmentStats remoteSegmentStats = shardStats.getStats().getSegments().getRemoteSegmentStats();
                assertTrue(remoteSegmentStats.getTotalUploadTime() > 0);
                assertTrue(remoteSegmentStats.getUploadBytesSucceeded() > 0);
            }
        });
    }
}
