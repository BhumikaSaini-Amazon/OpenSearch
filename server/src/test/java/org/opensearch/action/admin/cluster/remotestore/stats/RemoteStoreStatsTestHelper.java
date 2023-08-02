/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.stats;

import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.index.remote.RemoteSegmentTransferTracker;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteTranslogTracker;
import org.opensearch.index.store.DirectoryFileTransferTracker;

import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.opensearch.test.OpenSearchTestCase.assertEquals;
import static org.opensearch.test.OpenSearchTestCase.randomAlphaOfLength;

/**
 * Helper utilities for Remote Store stats tests
 */
public class RemoteStoreStatsTestHelper {
    static RemoteSegmentTransferTracker.Stats createStatsForNewPrimary(ShardId shardId) {
        return new RemoteSegmentTransferTracker.Stats(
            shardId,
            101,
            102,
            100,
            0,
            10,
            2,
            10,
            5,
            5,
            0,
            0,
            0,
            5,
            5,
            5,
            0,
            0,
            0,
            createZeroDirectoryFileTransferStats()
        );
    }

    static RemoteSegmentTransferTracker.Stats createStatsForNewReplica(ShardId shardId) {
        return new RemoteSegmentTransferTracker.Stats(
            shardId,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            createSampleDirectoryFileTransferStats()
        );
    }

    static RemoteSegmentTransferTracker.Stats createStatsForRemoteStoreRestoredPrimary(ShardId shardId) {
        return new RemoteSegmentTransferTracker.Stats(
            shardId,
            50,
            50,
            0,
            50,
            11,
            11,
            10,
            10,
            0,
            10,
            10,
            0,
            10,
            10,
            0,
            0,
            0,
            100,
            createSampleDirectoryFileTransferStats()
        );
    }

    static DirectoryFileTransferTracker.Stats createSampleDirectoryFileTransferStats() {
        return new DirectoryFileTransferTracker.Stats(10, 0, 10, 12345, 5, 5, 5);
    }

    static DirectoryFileTransferTracker.Stats createZeroDirectoryFileTransferStats() {
        return new DirectoryFileTransferTracker.Stats(0, 0, 0, 0, 0, 0, 0);
    }

    static ShardRouting createShardRouting(ShardId shardId, boolean isPrimary) {
        return TestShardRouting.newShardRouting(shardId, randomAlphaOfLength(4), isPrimary, ShardRoutingState.STARTED);
    }

    static RemoteTranslogTracker.Stats createPressureTrackerTranslogStats(ShardId shardId) {
        return new RemoteTranslogTracker.Stats(shardId, 1L, 2L, 3L, 4, 5L, 6L, 7L, 8L, 9D, 10D, 11D);
    }

    static void compareStatsResponse(
        Map<String, Object> statsObject,
        RemoteSegmentTransferTracker.Stats pressureTrackerSegmentStats,
        RemoteTranslogTracker.Stats pressureTrackerTranslogStats,
        ShardRouting routing
    ) {
        // Compare Remote Segment Store stats
        assertEquals(
            ((Map) statsObject.get(RemoteStoreStats.Fields.ROUTING)).get(RemoteStoreStats.RoutingFields.NODE_ID),
            routing.currentNodeId()
        );
        assertEquals(
            ((Map) statsObject.get(RemoteStoreStats.Fields.ROUTING)).get(RemoteStoreStats.RoutingFields.STATE),
            routing.state().toString()
        );
        assertEquals(
            ((Map) statsObject.get(RemoteStoreStats.Fields.ROUTING)).get(RemoteStoreStats.RoutingFields.PRIMARY),
            routing.primary()
        );

        Map<String, Object> segment = ((Map) statsObject.get(RemoteStoreStats.Fields.SEGMENT));
        Map<String, Object> segmentDownloads = ((Map) segment.get(RemoteStoreStats.SubFields.DOWNLOAD));
        Map<String, Object> segmentUploads = ((Map) segment.get(RemoteStoreStats.SubFields.UPLOAD));

        if (pressureTrackerSegmentStats.directoryFileTransferTrackerStats.transferredBytesStarted != 0) {
            assertEquals(
                segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.LAST_SYNC_TIMESTAMP),
                (int) pressureTrackerSegmentStats.directoryFileTransferTrackerStats.lastTransferTimestampMs
            );
            assertEquals(
                ((Map) segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.TOTAL_DOWNLOADS_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.STARTED
                ),
                (int) pressureTrackerSegmentStats.directoryFileTransferTrackerStats.transferredBytesStarted
            );
            assertEquals(
                ((Map) segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.TOTAL_DOWNLOADS_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.SUCCEEDED
                ),
                (int) pressureTrackerSegmentStats.directoryFileTransferTrackerStats.transferredBytesSucceeded
            );
            assertEquals(
                ((Map) segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.TOTAL_DOWNLOADS_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.FAILED
                ),
                (int) pressureTrackerSegmentStats.directoryFileTransferTrackerStats.transferredBytesFailed
            );
            assertEquals(
                ((Map) segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.DOWNLOAD_SIZE_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.LAST_SUCCESSFUL
                ),
                (int) pressureTrackerSegmentStats.directoryFileTransferTrackerStats.lastSuccessfulTransferInBytes
            );
            assertEquals(
                ((Map) segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.DOWNLOAD_SIZE_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.MOVING_AVG
                ),
                pressureTrackerSegmentStats.directoryFileTransferTrackerStats.transferredBytesMovingAverage
            );
            assertEquals(
                ((Map) segmentDownloads.get(RemoteStoreStats.DownloadStatsFields.DOWNLOAD_SPEED_IN_BYTES_PER_SEC)).get(
                    RemoteStoreStats.SubFields.MOVING_AVG
                ),
                pressureTrackerSegmentStats.directoryFileTransferTrackerStats.transferredBytesPerSecMovingAverage
            );
        } else {
            assertTrue(segmentDownloads.isEmpty());
        }

        if (pressureTrackerSegmentStats.totalUploadsStarted != 0) {
            assertEquals(
                segmentUploads.get(RemoteStoreStats.UploadStatsFields.LOCAL_REFRESH_TIMESTAMP),
                (int) pressureTrackerSegmentStats.localRefreshClockTimeMs
            );
            assertEquals(
                segmentUploads.get(RemoteStoreStats.UploadStatsFields.REMOTE_REFRESH_TIMESTAMP),
                (int) pressureTrackerSegmentStats.remoteRefreshClockTimeMs
            );
            assertEquals(
                segmentUploads.get(RemoteStoreStats.UploadStatsFields.REFRESH_TIME_LAG_IN_MILLIS),
                (int) pressureTrackerSegmentStats.refreshTimeLagMs
            );
            assertEquals(
                segmentUploads.get(RemoteStoreStats.UploadStatsFields.REFRESH_LAG),
                (int) (pressureTrackerSegmentStats.localRefreshNumber - pressureTrackerSegmentStats.remoteRefreshNumber)
            );
            assertEquals(segmentUploads.get(RemoteStoreStats.UploadStatsFields.BYTES_LAG), (int) pressureTrackerSegmentStats.bytesLag);

            assertEquals(
                segmentUploads.get(RemoteStoreStats.UploadStatsFields.BACKPRESSURE_REJECTION_COUNT),
                (int) pressureTrackerSegmentStats.rejectionCount
            );
            assertEquals(
                segmentUploads.get(RemoteStoreStats.UploadStatsFields.CONSECUTIVE_FAILURE_COUNT),
                (int) pressureTrackerSegmentStats.consecutiveFailuresCount
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.TOTAL_UPLOADS_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.STARTED
                ),
                (int) pressureTrackerSegmentStats.uploadBytesStarted
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.TOTAL_UPLOADS_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.SUCCEEDED
                ),
                (int) pressureTrackerSegmentStats.uploadBytesSucceeded
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.TOTAL_UPLOADS_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.FAILED
                ),
                (int) pressureTrackerSegmentStats.uploadBytesFailed
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.REMOTE_REFRESH_SIZE_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.MOVING_AVG
                ),
                pressureTrackerSegmentStats.uploadBytesMovingAverage
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.REMOTE_REFRESH_SIZE_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.LAST_SUCCESSFUL
                ),
                (int) pressureTrackerSegmentStats.lastSuccessfulRemoteRefreshBytes
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.UPLOAD_LATENCY_IN_BYTES_PER_SEC)).get(
                    RemoteStoreStats.SubFields.MOVING_AVG
                ),
                pressureTrackerSegmentStats.uploadBytesPerSecMovingAverage
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.TOTAL_SYNCS_TO_REMOTE)).get(
                    RemoteStoreStats.SubFields.STARTED
                ),
                (int) pressureTrackerSegmentStats.totalUploadsStarted
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.TOTAL_SYNCS_TO_REMOTE)).get(
                    RemoteStoreStats.SubFields.SUCCEEDED
                ),
                (int) pressureTrackerSegmentStats.totalUploadsSucceeded
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.TOTAL_SYNCS_TO_REMOTE)).get(RemoteStoreStats.SubFields.FAILED),
                (int) pressureTrackerSegmentStats.totalUploadsFailed
            );
            assertEquals(
                ((Map) segmentUploads.get(RemoteStoreStats.UploadStatsFields.REMOTE_REFRESH_LATENCY_IN_MILLIS)).get(
                    RemoteStoreStats.SubFields.MOVING_AVG
                ),
                pressureTrackerSegmentStats.uploadTimeMovingAverage
            );
        } else {
            assertTrue(segmentUploads.isEmpty());
        }

        // Compare Remote Translog Store stats
        Map<?, ?> tlogStatsObj = (Map<?, ?>) statsObject.get("translog");
        Map<?, ?> tlogUploadStatsObj = (Map<?, ?>) tlogStatsObj.get("upload");
        assertEquals(
            pressureTrackerTranslogStats.lastUploadTimestamp,
            Long.parseLong(tlogUploadStatsObj.get(RemoteStoreStats.UploadStatsFields.LAST_UPLOAD_TIMESTAMP).toString())
        );

        assertEquals(
            pressureTrackerTranslogStats.totalUploadsStarted,
            Long.parseLong(
                ((Map<?, ?>) tlogUploadStatsObj.get(RemoteStoreStats.UploadStatsFields.TOTAL_UPLOADS)).get(
                    RemoteStoreStats.SubFields.STARTED
                ).toString()
            )
        );
        assertEquals(
            pressureTrackerTranslogStats.totalUploadsSucceeded,
            Long.parseLong(
                ((Map<?, ?>) tlogUploadStatsObj.get(RemoteStoreStats.UploadStatsFields.TOTAL_UPLOADS)).get(
                    RemoteStoreStats.SubFields.SUCCEEDED
                ).toString()
            )
        );
        assertEquals(
            pressureTrackerTranslogStats.totalUploadsFailed,
            Long.parseLong(
                ((Map<?, ?>) tlogUploadStatsObj.get(RemoteStoreStats.UploadStatsFields.TOTAL_UPLOADS)).get(
                    RemoteStoreStats.SubFields.FAILED
                ).toString()
            )
        );

        assertEquals(
            pressureTrackerTranslogStats.uploadBytesStarted,
            Long.parseLong(
                ((Map<?, ?>) tlogUploadStatsObj.get(RemoteStoreStats.UploadStatsFields.TOTAL_UPLOADS_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.STARTED
                ).toString()
            )
        );
        assertEquals(
            pressureTrackerTranslogStats.uploadBytesSucceeded,
            Long.parseLong(
                ((Map<?, ?>) tlogUploadStatsObj.get(RemoteStoreStats.UploadStatsFields.TOTAL_UPLOADS_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.SUCCEEDED
                ).toString()
            )
        );
        assertEquals(
            pressureTrackerTranslogStats.uploadBytesFailed,
            Long.parseLong(
                ((Map<?, ?>) tlogUploadStatsObj.get(RemoteStoreStats.UploadStatsFields.TOTAL_UPLOADS_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.FAILED
                ).toString()
            )
        );

        assertEquals(
            pressureTrackerTranslogStats.totalUploadTimeInMillis,
            Long.parseLong(tlogUploadStatsObj.get(RemoteStoreStats.UploadStatsFields.TOTAL_UPLOAD_TIME_IN_MILLIS).toString())
        );

        assertEquals(
            pressureTrackerTranslogStats.uploadBytesMovingAverage,
            ((Map<?, ?>) tlogUploadStatsObj.get(RemoteStoreStats.UploadStatsFields.UPLOAD_BYTES)).get(RemoteStoreStats.SubFields.MOVING_AVG)
        );
        assertEquals(
            pressureTrackerTranslogStats.uploadBytesPerSecMovingAverage,
            ((Map<?, ?>) tlogUploadStatsObj.get(RemoteStoreStats.UploadStatsFields.UPLOAD_LATENCY_IN_BYTES_PER_SEC)).get(
                RemoteStoreStats.SubFields.MOVING_AVG
            )
        );
        assertEquals(
            pressureTrackerTranslogStats.uploadTimeMovingAverage,
            ((Map<?, ?>) tlogUploadStatsObj.get(RemoteStoreStats.UploadStatsFields.UPLOAD_TIME_IN_MILLIS)).get(
                RemoteStoreStats.SubFields.MOVING_AVG
            )
        );
    }
}
