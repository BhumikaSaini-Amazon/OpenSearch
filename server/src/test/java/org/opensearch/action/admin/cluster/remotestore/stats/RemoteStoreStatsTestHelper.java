/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.stats;

import org.opensearch.index.remote.RemoteRefreshSegmentTracker;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteTranslogTracker;

import java.util.Map;

import static org.opensearch.test.OpenSearchTestCase.assertEquals;

/**
 * Helper utilities for Remote Store stats tests
 */
public class RemoteStoreStatsTestHelper {
    static RemoteRefreshSegmentTracker.Stats createPressureTrackerSegmentStats(ShardId shardId) {
        return new RemoteRefreshSegmentTracker.Stats(shardId, 101, 102, 100, 3, 2, 10, 5, 5, 10, 5, 5, 3, 2, 5, 2, 3, 4, 9);
    }

    static RemoteTranslogTracker.Stats createPressureTrackerTranslogStats(ShardId shardId) {
        return new RemoteTranslogTracker.Stats(shardId, 1L, 2L, 3L, 4, 5L, 6L, 7L, 8L, 9D, 10D, 11D);
    }

    static void compareStatsResponse(
        Map<String, Object> statsObject,
        RemoteRefreshSegmentTracker.Stats pressureTrackerSegmentStats,
        RemoteTranslogTracker.Stats pressureTrackerTranslogStats
    ) {
        // Compare Remote Segment Store stats
        assertEquals(pressureTrackerSegmentStats.shardId.toString(), statsObject.get(RemoteStoreStats.Fields.SHARD_ID));
        assertEquals(
            (int) pressureTrackerSegmentStats.localRefreshClockTimeMs,
            statsObject.get(RemoteStoreStats.Fields.LOCAL_REFRESH_TIMESTAMP)
        );
        assertEquals(
            (int) pressureTrackerSegmentStats.remoteRefreshClockTimeMs,
            statsObject.get(RemoteStoreStats.Fields.REMOTE_REFRESH_TIMESTAMP)
        );
        assertEquals(
            (int) pressureTrackerSegmentStats.refreshTimeLagMs,
            statsObject.get(RemoteStoreStats.Fields.REFRESH_TIME_LAG_IN_MILLIS)
        );
        assertEquals(
            (int) (pressureTrackerSegmentStats.localRefreshNumber - pressureTrackerSegmentStats.remoteRefreshNumber),
            statsObject.get(RemoteStoreStats.Fields.REFRESH_LAG)
        );
        assertEquals((int) pressureTrackerSegmentStats.bytesLag, statsObject.get(RemoteStoreStats.Fields.BYTES_LAG));

        assertEquals(
            (int) pressureTrackerSegmentStats.rejectionCount,
            statsObject.get(RemoteStoreStats.Fields.BACKPRESSURE_REJECTION_COUNT)
        );
        assertEquals(
            (int) pressureTrackerSegmentStats.consecutiveFailuresCount,
            statsObject.get(RemoteStoreStats.Fields.CONSECUTIVE_FAILURE_COUNT)
        );

        assertEquals(
            (int) pressureTrackerSegmentStats.uploadBytesStarted,
            ((Map<?, ?>) statsObject.get(RemoteStoreStats.Fields.TOTAL_UPLOADS_IN_BYTES)).get(RemoteStoreStats.SubFields.STARTED)
        );
        assertEquals(
            (int) pressureTrackerSegmentStats.uploadBytesSucceeded,
            ((Map<?, ?>) statsObject.get(RemoteStoreStats.Fields.TOTAL_UPLOADS_IN_BYTES)).get(RemoteStoreStats.SubFields.SUCCEEDED)
        );
        assertEquals(
            (int) pressureTrackerSegmentStats.uploadBytesFailed,
            ((Map<?, ?>) statsObject.get(RemoteStoreStats.Fields.TOTAL_UPLOADS_IN_BYTES)).get(RemoteStoreStats.SubFields.FAILED)
        );
        assertEquals(
            pressureTrackerSegmentStats.uploadBytesMovingAverage,
            ((Map<?, ?>) statsObject.get(RemoteStoreStats.Fields.REMOTE_REFRESH_SIZE_IN_BYTES)).get(RemoteStoreStats.SubFields.MOVING_AVG)
        );
        assertEquals(
            (int) pressureTrackerSegmentStats.lastSuccessfulRemoteRefreshBytes,
            ((Map<?, ?>) statsObject.get(RemoteStoreStats.Fields.REMOTE_REFRESH_SIZE_IN_BYTES)).get(
                RemoteStoreStats.SubFields.LAST_SUCCESSFUL
            )
        );
        assertEquals(
            pressureTrackerSegmentStats.uploadBytesPerSecMovingAverage,
            ((Map<?, ?>) statsObject.get(RemoteStoreStats.Fields.UPLOAD_LATENCY_IN_BYTES_PER_SEC)).get(
                RemoteStoreStats.SubFields.MOVING_AVG
            )
        );
        assertEquals(
            (int) pressureTrackerSegmentStats.totalUploadsStarted,
            ((Map<?, ?>) statsObject.get(RemoteStoreStats.Fields.TOTAL_REMOTE_REFRESH)).get(RemoteStoreStats.SubFields.STARTED)
        );
        assertEquals(
            (int) pressureTrackerSegmentStats.totalUploadsSucceeded,
            ((Map<?, ?>) statsObject.get(RemoteStoreStats.Fields.TOTAL_REMOTE_REFRESH)).get(RemoteStoreStats.SubFields.SUCCEEDED)
        );
        assertEquals(
            (int) pressureTrackerSegmentStats.totalUploadsFailed,
            ((Map<?, ?>) statsObject.get(RemoteStoreStats.Fields.TOTAL_REMOTE_REFRESH)).get(RemoteStoreStats.SubFields.FAILED)
        );
        assertEquals(
            pressureTrackerSegmentStats.uploadTimeMovingAverage,
            ((Map<?, ?>) statsObject.get(RemoteStoreStats.Fields.REMOTE_REFRESH_LATENCY_IN_MILLIS)).get(
                RemoteStoreStats.SubFields.MOVING_AVG
            )
        );

        // Compare Remote Translog Store stats
        Map<?, ?> tlogStatsObj = (Map<?, ?>) statsObject.get("translog");
        Map<?, ?> tlogUploadStatsObj = (Map<?, ?>) tlogStatsObj.get("upload");

        assertEquals(pressureTrackerTranslogStats.shardId.toString(), statsObject.get(RemoteStoreStats.Fields.SHARD_ID));
        assertEquals(
            pressureTrackerTranslogStats.lastUploadTimestamp,
            Long.parseLong(tlogUploadStatsObj.get(RemoteStoreStats.Fields.LAST_UPLOAD_TIMESTAMP).toString())
        );

        assertEquals(
            pressureTrackerTranslogStats.totalUploadsStarted,
            Long.parseLong(
                ((Map<?, ?>) tlogUploadStatsObj.get(RemoteStoreStats.Fields.TOTAL_UPLOADS)).get(RemoteStoreStats.SubFields.STARTED)
                    .toString()
            )
        );
        assertEquals(
            pressureTrackerTranslogStats.totalUploadsSucceeded,
            Long.parseLong(
                ((Map<?, ?>) tlogUploadStatsObj.get(RemoteStoreStats.Fields.TOTAL_UPLOADS)).get(RemoteStoreStats.SubFields.SUCCEEDED)
                    .toString()
            )
        );
        assertEquals(
            pressureTrackerTranslogStats.totalUploadsFailed,
            Long.parseLong(
                ((Map<?, ?>) tlogUploadStatsObj.get(RemoteStoreStats.Fields.TOTAL_UPLOADS)).get(RemoteStoreStats.SubFields.FAILED)
                    .toString()
            )
        );

        assertEquals(
            pressureTrackerTranslogStats.uploadBytesStarted,
            Long.parseLong(
                ((Map<?, ?>) tlogUploadStatsObj.get(RemoteStoreStats.Fields.TOTAL_UPLOADS_IN_BYTES)).get(RemoteStoreStats.SubFields.STARTED)
                    .toString()
            )
        );
        assertEquals(
            pressureTrackerTranslogStats.uploadBytesSucceeded,
            Long.parseLong(
                ((Map<?, ?>) tlogUploadStatsObj.get(RemoteStoreStats.Fields.TOTAL_UPLOADS_IN_BYTES)).get(
                    RemoteStoreStats.SubFields.SUCCEEDED
                ).toString()
            )
        );
        assertEquals(
            pressureTrackerTranslogStats.uploadBytesFailed,
            Long.parseLong(
                ((Map<?, ?>) tlogUploadStatsObj.get(RemoteStoreStats.Fields.TOTAL_UPLOADS_IN_BYTES)).get(RemoteStoreStats.SubFields.FAILED)
                    .toString()
            )
        );

        assertEquals(
            pressureTrackerTranslogStats.totalUploadTimeInMillis,
            Long.parseLong(tlogUploadStatsObj.get(RemoteStoreStats.Fields.TOTAL_UPLOAD_TIME_IN_MILLIS).toString())
        );

        assertEquals(
            pressureTrackerTranslogStats.uploadBytesMovingAverage,
            ((Map<?, ?>) tlogUploadStatsObj.get(RemoteStoreStats.Fields.UPLOAD_BYTES)).get(RemoteStoreStats.SubFields.MOVING_AVG)
        );
        assertEquals(
            pressureTrackerTranslogStats.uploadBytesPerSecMovingAverage,
            ((Map<?, ?>) tlogUploadStatsObj.get(RemoteStoreStats.Fields.UPLOAD_LATENCY_IN_BYTES_PER_SEC)).get(
                RemoteStoreStats.SubFields.MOVING_AVG
            )
        );
        assertEquals(
            pressureTrackerTranslogStats.uploadTimeMovingAverage,
            ((Map<?, ?>) tlogUploadStatsObj.get(RemoteStoreStats.Fields.UPLOAD_TIME_IN_MILLIS)).get(RemoteStoreStats.SubFields.MOVING_AVG)
        );
    }
}
