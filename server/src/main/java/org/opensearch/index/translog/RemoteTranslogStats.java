/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Encapsulates the stats related to Remote Translog Store operations
 *
 * @opensearch.internal
 */
public class RemoteTranslogStats implements Writeable {
    /**
     * Epoch timestamp of the last successful Remote Translog Store upload.
     */
    public long lastUploadTimestamp;

    /**
     * Total number of Remote Translog Store uploads that have been started.
     */
    public long totalUploadsStarted;

    /**
     * Total number of Remote Translog Store uploads that have failed.
     */
    public long totalUploadsFailed;

    /**
     * Total number of Remote Translog Store that have been successful.
     */
    public long totalUploadsSucceeded;

    /**
     * Total number of byte uploads to Remote Translog Store that have been started.
     */
    public long uploadBytesStarted;

    /**
     * Total number of byte uploads to Remote Translog Store that have failed.
     */
    public long uploadBytesFailed;

    /**
     * Total number of byte uploads to Remote Translog Store that have been successful.
     */
    public long uploadBytesSucceeded;

    /**
     * Total time spent on Remote Translog Store uploads.
     */
    public long totalUploadTimeInMillis;

    /**
     * Size of a Remote Translog Store upload in bytes.
     */
    public double uploadBytesMovingAverage;

    /**
     * Speed of a Remote Translog Store upload in bytes-per-second.
     */
    public double uploadBytesPerSecMovingAverage;

    /**
     *  Time taken by a Remote Translog Store upload.
     */
    public double uploadTimeMovingAverage;

    static final String REMOTE_STORE = "remote_store";

    static final class Flows {
        static final String DOWNLOAD = "download";
        static final String UPLOAD = "upload";
    }

    static final class SubFields {
        static final String STARTED = "started";
        static final String SUCCEEDED = "succeeded";
        static final String FAILED = "failed";
        static final String MOVING_AVG = "moving_avg";
    }

    static final class Fields {
        static final String TOTAL_UPLOADS_IN_BYTES = "total_uploads_in_bytes";
        static final String UPLOAD_LATENCY_IN_BYTES_PER_SEC = "upload_latency_in_bytes_per_sec";
        static final String LAST_UPLOAD_TIMESTAMP = "last_upload_timestamp";
        static final String TOTAL_UPLOADS = "total_uploads";
        static final String TOTAL_UPLOAD_TIME_IN_MILLIS = "total_upload_time_in_millis";
        static final String UPLOAD_BYTES = "upload_bytes";
        static final String UPLOAD_TIME_IN_MILLIS = "upload_time_in_millis";
    }

    public RemoteTranslogStats(
        long lastUploadTimestamp,
        long totalUploadsStarted,
        long totalUploadsSucceeded,
        long totalUploadsFailed,
        long uploadBytesStarted,
        long uploadBytesSucceeded,
        long uploadBytesFailed,
        long totalUploadTimeInMillis,
        double uploadBytesMovingAverage,
        double uploadBytesPerSecMovingAverage,
        double uploadTimeMovingAverage
    ) {
        this.lastUploadTimestamp = lastUploadTimestamp;
        this.totalUploadsStarted = totalUploadsStarted;
        this.totalUploadsFailed = totalUploadsFailed;
        this.totalUploadsSucceeded = totalUploadsSucceeded;
        this.uploadBytesStarted = uploadBytesStarted;
        this.uploadBytesFailed = uploadBytesFailed;
        this.uploadBytesSucceeded = uploadBytesSucceeded;
        this.totalUploadTimeInMillis = totalUploadTimeInMillis;
        this.uploadBytesMovingAverage = uploadBytesMovingAverage;
        this.uploadBytesPerSecMovingAverage = uploadBytesPerSecMovingAverage;
        this.uploadTimeMovingAverage = uploadTimeMovingAverage;
    }

    public RemoteTranslogStats(StreamInput in) throws IOException {
        try {
            if (in.available() > 0) {
                readNodesStatsAPIFields(in);
            } else {
                initializeNodesStatsAPIFields();
            }
        } catch (IOException e) {
            throw e;
        }

        initializeRemotestoreStatsAPIOnlyFields();
    }

    private void readNodesStatsAPIFields(StreamInput in) throws IOException {
        this.totalUploadsStarted = in.readVLong();
        this.totalUploadsFailed = in.readVLong();
        this.totalUploadsSucceeded = in.readVLong();
        this.uploadBytesStarted = in.readVLong();
        this.uploadBytesFailed = in.readVLong();
        this.uploadBytesSucceeded = in.readVLong();
        this.totalUploadTimeInMillis = in.readVLong();
    }

    private void initializeNodesStatsAPIFields() {
        this.totalUploadsStarted = 0L;
        this.totalUploadsFailed = 0L;
        this.totalUploadsSucceeded = 0L;
        this.uploadBytesStarted = 0L;
        this.uploadBytesFailed = 0L;
        this.uploadBytesSucceeded = 0L;
        this.totalUploadTimeInMillis = 0L;
    }

    private void initializeRemotestoreStatsAPIOnlyFields() {
        // Currently, these are only exposed under _remotestore/stats API (until it gets deprecated).
        // Hence, we don't pass them over the wire between nodes in the _nodes/stats API.
        this.lastUploadTimestamp = 0L;
        this.uploadBytesMovingAverage = 0D;
        this.uploadBytesPerSecMovingAverage = 0D;
        this.uploadTimeMovingAverage = 0D;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeNodesStatsAPIFields(out);
    }

    private void writeNodesStatsAPIFields(StreamOutput out) throws IOException {
        out.writeVLong(totalUploadsStarted);
        out.writeVLong(totalUploadsFailed);
        out.writeVLong(totalUploadsSucceeded);
        out.writeVLong(uploadBytesStarted);
        out.writeVLong(uploadBytesFailed);
        out.writeVLong(uploadBytesSucceeded);
        out.writeVLong(totalUploadTimeInMillis);
    }

    // @Override
    // public boolean equals(Object obj) {
    // if (this == obj) return true;
    // if (obj == null || getClass() != obj.getClass()) return false;
    // RemoteTranslogStats other = (RemoteTranslogStats) obj;
    //
    // return this.shardId.toString().equals(other.shardId.toString())
    // && this.lastUploadTimestamp == other.lastUploadTimestamp
    // && this.totalUploadsStarted == other.totalUploadsStarted
    // && this.totalUploadsFailed == other.totalUploadsFailed
    // && this.totalUploadsSucceeded == other.totalUploadsSucceeded
    // && this.uploadBytesStarted == other.uploadBytesStarted
    // && this.uploadBytesFailed == other.uploadBytesFailed
    // && this.uploadBytesSucceeded == other.uploadBytesSucceeded
    // && this.totalUploadTimeInMillis == other.totalUploadTimeInMillis
    // && Double.compare(this.uploadBytesMovingAverage, other.uploadBytesMovingAverage) == 0
    // && Double.compare(this.uploadBytesPerSecMovingAverage, other.uploadBytesPerSecMovingAverage) == 0
    // && Double.compare(this.uploadTimeMovingAverage, other.uploadTimeMovingAverage) == 0;
    // }
    //
    // @Override
    // public int hashCode() {
    // return Objects.hash(
    // shardId.toString(),
    // lastUploadTimestamp,
    // totalUploadsStarted,
    // totalUploadsFailed,
    // totalUploadsSucceeded,
    // uploadBytesStarted,
    // uploadBytesFailed,
    // uploadBytesSucceeded,
    // totalUploadTimeInMillis,
    // uploadBytesMovingAverage,
    // uploadBytesPerSecMovingAverage,
    // uploadTimeMovingAverage
    // );
    // }

    public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
        builder.startObject(REMOTE_STORE);

        builder.startObject(Flows.UPLOAD);
        addRemoteTranslogUploadStatsXContent(builder);
        builder.endObject(); // translog.remote_store.upload

//        builder.startObject(Flows.DOWNLOAD);
//        addRemoteTranslogDownloadStatsXContent(builder);
//        builder.endObject(); // translog.remote_store.download

        builder.endObject(); // translog.remote_store

        return builder;
    }

    void add(RemoteTranslogStats other) {
        if (other == null) {
            return;
        }

        this.totalUploadsStarted += other.totalUploadsStarted;
        this.totalUploadsFailed += other.totalUploadsFailed;
        this.totalUploadsSucceeded += other.totalUploadsSucceeded;
        this.uploadBytesStarted += other.uploadBytesStarted;
        this.uploadBytesFailed += other.uploadBytesFailed;
        this.uploadBytesSucceeded += other.uploadBytesSucceeded;
        this.totalUploadTimeInMillis += other.totalUploadTimeInMillis;
    }

    void addRemoteTranslogUploadStatsXContent(XContentBuilder builder) throws IOException {
        builder.startObject(Fields.TOTAL_UPLOADS);
        builder.field(SubFields.STARTED, totalUploadsStarted)
            .field(SubFields.FAILED, totalUploadsFailed)
            .field(SubFields.SUCCEEDED, totalUploadsSucceeded);
        builder.endObject();

        builder.startObject(Fields.TOTAL_UPLOADS_IN_BYTES);
        builder.field(SubFields.STARTED, uploadBytesStarted)
            .field(SubFields.FAILED, uploadBytesFailed)
            .field(SubFields.SUCCEEDED, uploadBytesSucceeded);
        builder.endObject();

        builder.field(Fields.TOTAL_UPLOAD_TIME_IN_MILLIS, totalUploadTimeInMillis);
    }

    void addRemoteTranslogDownloadStatsXContent(XContentBuilder builder) throws IOException {}
}
