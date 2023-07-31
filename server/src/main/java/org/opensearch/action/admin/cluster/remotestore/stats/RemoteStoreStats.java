/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.remote.RemoteRefreshSegmentTracker;
import org.opensearch.index.remote.RemoteTranslogTracker;

import java.io.IOException;

/**
 * Encapsulates all remote store stats
 *
 * @opensearch.internal
 */
public class RemoteStoreStats implements Writeable, ToXContentFragment {
    /**
     * Stats related to Remote Segment Store operations
     */
    private final RemoteRefreshSegmentTracker.Stats remoteSegmentUploadShardStats;

    /**
     * Stats related to Remote Translog Store operations
     */
    private final RemoteTranslogTracker.Stats remoteTranslogShardStats;

    public RemoteStoreStats(
        RemoteRefreshSegmentTracker.Stats remoteSegmentUploadShardStats,
        RemoteTranslogTracker.Stats remoteTranslogShardStats
    ) {
        this.remoteSegmentUploadShardStats = remoteSegmentUploadShardStats;
        this.remoteTranslogShardStats = remoteTranslogShardStats;
    }

    public RemoteStoreStats(StreamInput in) throws IOException {
        remoteSegmentUploadShardStats = in.readOptionalWriteable(RemoteRefreshSegmentTracker.Stats::new);
        remoteTranslogShardStats = in.readOptionalWriteable(RemoteTranslogTracker.Stats::new);
    }

    public RemoteRefreshSegmentTracker.Stats getSegmentStats() {
        return remoteSegmentUploadShardStats;
    }

    public RemoteTranslogTracker.Stats getTranslogStats() {
        return remoteTranslogShardStats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
            .field(Fields.SHARD_ID, remoteSegmentUploadShardStats.shardId)
            .field(Fields.LOCAL_REFRESH_TIMESTAMP, remoteSegmentUploadShardStats.localRefreshClockTimeMs)
            .field(Fields.REMOTE_REFRESH_TIMESTAMP, remoteSegmentUploadShardStats.remoteRefreshClockTimeMs)
            .field(Fields.REFRESH_TIME_LAG_IN_MILLIS, remoteSegmentUploadShardStats.refreshTimeLagMs)
            .field(Fields.REFRESH_LAG, remoteSegmentUploadShardStats.localRefreshNumber - remoteSegmentUploadShardStats.remoteRefreshNumber)
            .field(Fields.BYTES_LAG, remoteSegmentUploadShardStats.bytesLag)

            .field(Fields.BACKPRESSURE_REJECTION_COUNT, remoteSegmentUploadShardStats.rejectionCount)
            .field(Fields.CONSECUTIVE_FAILURE_COUNT, remoteSegmentUploadShardStats.consecutiveFailuresCount);

        builder.startObject(Fields.TOTAL_REMOTE_REFRESH);
        builder.field(SubFields.STARTED, remoteSegmentUploadShardStats.totalUploadsStarted)
            .field(SubFields.SUCCEEDED, remoteSegmentUploadShardStats.totalUploadsSucceeded)
            .field(SubFields.FAILED, remoteSegmentUploadShardStats.totalUploadsFailed);
        builder.endObject();

        builder.startObject(Fields.TOTAL_UPLOADS_IN_BYTES);
        builder.field(SubFields.STARTED, remoteSegmentUploadShardStats.uploadBytesStarted)
            .field(SubFields.SUCCEEDED, remoteSegmentUploadShardStats.uploadBytesSucceeded)
            .field(SubFields.FAILED, remoteSegmentUploadShardStats.uploadBytesFailed);
        builder.endObject();

        builder.startObject(Fields.REMOTE_REFRESH_SIZE_IN_BYTES);
        builder.field(SubFields.LAST_SUCCESSFUL, remoteSegmentUploadShardStats.lastSuccessfulRemoteRefreshBytes);
        builder.field(SubFields.MOVING_AVG, remoteSegmentUploadShardStats.uploadBytesMovingAverage);
        builder.endObject();

        builder.startObject(Fields.UPLOAD_LATENCY_IN_BYTES_PER_SEC);
        builder.field(SubFields.MOVING_AVG, remoteSegmentUploadShardStats.uploadBytesPerSecMovingAverage);
        builder.endObject();
        builder.startObject(Fields.REMOTE_REFRESH_LATENCY_IN_MILLIS);
        builder.field(SubFields.MOVING_AVG, remoteSegmentUploadShardStats.uploadTimeMovingAverage);
        builder.endObject();

        builder.startObject("translog");
        builder.startObject("upload");
        builder.field(Fields.LAST_UPLOAD_TIMESTAMP, remoteTranslogShardStats.lastUploadTimestamp);

        builder.startObject(Fields.TOTAL_UPLOADS);
        builder.field(SubFields.STARTED, remoteTranslogShardStats.totalUploadsStarted)
            .field(SubFields.FAILED, remoteTranslogShardStats.totalUploadsFailed)
            .field(SubFields.SUCCEEDED, remoteTranslogShardStats.totalUploadsSucceeded);
        builder.endObject();

        builder.startObject(Fields.TOTAL_UPLOADS_IN_BYTES);
        builder.field(SubFields.STARTED, remoteTranslogShardStats.uploadBytesStarted)
            .field(SubFields.FAILED, remoteTranslogShardStats.uploadBytesFailed)
            .field(SubFields.SUCCEEDED, remoteTranslogShardStats.uploadBytesSucceeded);
        builder.endObject();

        builder.field(Fields.TOTAL_UPLOAD_TIME_IN_MILLIS, remoteTranslogShardStats.totalUploadTimeInMillis);

        builder.startObject(Fields.UPLOAD_BYTES);
        builder.field(SubFields.MOVING_AVG, remoteTranslogShardStats.uploadBytesMovingAverage);
        builder.endObject();

        builder.startObject(Fields.UPLOAD_LATENCY_IN_BYTES_PER_SEC);
        builder.field(SubFields.MOVING_AVG, remoteTranslogShardStats.uploadBytesPerSecMovingAverage);
        builder.endObject();

        builder.startObject(Fields.UPLOAD_TIME_IN_MILLIS);
        builder.field(SubFields.MOVING_AVG, remoteTranslogShardStats.uploadTimeMovingAverage);
        builder.endObject();

        builder.endObject(); // translog.upload
        builder.endObject(); // translog

        builder.endObject();

        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(remoteSegmentUploadShardStats);
        out.writeOptionalWriteable(remoteTranslogShardStats);
    }

    /**
     * Fields for remote store stats response
     */
    static final class Fields {
        static final String SHARD_ID = "shard_id";

        /**
         * Lag in terms of bytes b/w local and remote store
         */
        static final String BYTES_LAG = "bytes_lag";

        /**
         * No of refresh remote store is lagging behind local
         */
        static final String REFRESH_LAG = "refresh_lag";

        /**
         * Time in millis remote refresh is behind local refresh
         */
        static final String REFRESH_TIME_LAG_IN_MILLIS = "refresh_time_lag_in_millis";

        /**
         * Last successful local refresh timestamp in milliseconds
         */
        static final String LOCAL_REFRESH_TIMESTAMP = "local_refresh_timestamp_in_millis";

        /**
         * Last successful remote refresh timestamp in milliseconds
         */
        static final String REMOTE_REFRESH_TIMESTAMP = "remote_refresh_timestamp_in_millis";

        /**
         * Total write rejections due to remote store backpressure kick in
         */
        static final String BACKPRESSURE_REJECTION_COUNT = "backpressure_rejection_count";

        /**
         * No of consecutive remote refresh failures without a single success since the first failures
         */
        static final String CONSECUTIVE_FAILURE_COUNT = "consecutive_failure_count";

        /**
         * Represents the number of remote refreshes
         */
        static final String TOTAL_REMOTE_REFRESH = "total_remote_refresh";

        /**
         * Represents the total uploads to remote store in bytes
         */
        static final String TOTAL_UPLOADS_IN_BYTES = "total_uploads_in_bytes";

        /**
         * Represents the size of new data to be uploaded as part of a refresh
         */
        static final String REMOTE_REFRESH_SIZE_IN_BYTES = "remote_refresh_size_in_bytes";

        /**
         * Represents the speed of remote store uploads in bytes per sec
         */
        static final String UPLOAD_LATENCY_IN_BYTES_PER_SEC = "upload_latency_in_bytes_per_sec";

        /**
         * Time taken by a single remote refresh
         */
        static final String REMOTE_REFRESH_LATENCY_IN_MILLIS = "remote_refresh_latency_in_millis";

        /**
         * Timestamp of last successful remote store upload
         */
        static final String LAST_UPLOAD_TIMESTAMP = "last_upload_timestamp";

        /**
         * Number of total uploads to remote store
         */
        static final String TOTAL_UPLOADS = "total_uploads";

        /**
         * Total time spent on remote store uplaods
         */
        static final String TOTAL_UPLOAD_TIME_IN_MILLIS = "total_upload_time_in_millis";

        /**
         * Represents the size of new data to be transferred as part of a remote store upload
         */
        static final String UPLOAD_BYTES = "upload_bytes";

        /**
         * Time taken by a remote store upload
         */
        static final String UPLOAD_TIME_IN_MILLIS = "upload_time_in_millis";
    }

    /**
     * Reusable sub fields for {@link Fields}
     */
    static final class SubFields {
        static final String STARTED = "started";
        static final String SUCCEEDED = "succeeded";
        static final String FAILED = "failed";

        /**
         * Moving avg over last N values stat for a {@link Fields}
         */
        static final String MOVING_AVG = "moving_avg";

        /**
         * Most recent successful attempt stat for a {@link Fields}
         */
        static final String LAST_SUCCESSFUL = "last_successful";
    }

}
