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
public class RemoteTranslogStats implements ToXContentFragment, Writeable {
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

    /**
     * Epoch timestamp of the last successful Remote Translog Store upload.
     */
    public long lastDownloadTimestamp;

    /**
     * Total number of Remote Translog Store uploads that have been started.
     */
    public long totalDownloadsStarted;

    /**
     * Total number of Remote Translog Store uploads that have failed.
     */
    public long totalDownloadsFailed;

    /**
     * Total number of Remote Translog Store that have been successful.
     */
    public long totalDownloadsSucceeded;

    /**
     * Total number of byte uploads to Remote Translog Store that have been started.
     */
    public long downloadBytesStarted;

    /**
     * Total number of byte uploads to Remote Translog Store that have failed.
     */
    public long downloadBytesFailed;

    /**
     * Total number of byte uploads to Remote Translog Store that have been successful.
     */
    public long downloadBytesSucceeded;

    /**
     * Total time spent on Remote Translog Store uploads.
     */
    public long totalDownloadTimeInMillis;

    /**
     * Size of a Remote Translog Store upload in bytes.
     */
    public double downloadBytesMovingAverage;

    /**
     * Speed of a Remote Translog Store upload in bytes-per-second.
     */
    public double downloadBytesPerSecMovingAverage;

    /**
     *  Time taken by a Remote Translog Store upload.
     */
    public double downloadTimeMovingAverage;

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

        static final String TOTAL_DOWNLOADS_IN_BYTES = "total_downloads_in_bytes";
        static final String DOWNLOAD_LATENCY_IN_BYTES_PER_SEC = "download_latency_in_bytes_per_sec";
        static final String LAST_DOWNLOAD_TIMESTAMP = "last_download_timestamp";
        static final String TOTAL_DOWNLOADS = "total_downloads";
        static final String TOTAL_DOWNLOAD_TIME_IN_MILLIS = "total_download_time_in_millis";
        static final String DOWNLOAD_BYTES = "download_bytes";
        static final String DOWNLOAD_TIME_IN_MILLIS = "download_time_in_millis";
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
        double uploadTimeMovingAverage,
        long lastDownloadTimestamp,
        long totalDownloadsStarted,
        long totalDownloadsSucceeded,
        long totalDownloadsFailed,
        long downloadBytesStarted,
        long downloadBytesSucceeded,
        long downloadBytesFailed,
        long totalDownloadTimeInMillis,
        double downloadBytesMovingAverage,
        double downloadBytesPerSecMovingAverage,
        double downloadTimeMovingAverage
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
        this.lastDownloadTimestamp = lastDownloadTimestamp;
        this.totalDownloadsStarted = totalDownloadsStarted;
        this.totalDownloadsFailed = totalDownloadsFailed;
        this.totalDownloadsSucceeded = totalDownloadsSucceeded;
        this.downloadBytesStarted = downloadBytesStarted;
        this.downloadBytesFailed = downloadBytesFailed;
        this.downloadBytesSucceeded = downloadBytesSucceeded;
        this.totalDownloadTimeInMillis = totalDownloadTimeInMillis;
        this.downloadBytesMovingAverage = downloadBytesMovingAverage;
        this.downloadBytesPerSecMovingAverage = downloadBytesPerSecMovingAverage;
        this.downloadTimeMovingAverage = downloadTimeMovingAverage;
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

    public RemoteTranslogStats(RemoteTranslogStats other) {
        this.totalUploadsStarted = other.totalUploadsStarted;
        this.totalUploadsFailed = other.totalUploadsFailed;
        this.totalUploadsSucceeded = other.totalUploadsSucceeded;
        this.uploadBytesStarted = other.uploadBytesStarted;
        this.uploadBytesFailed = other.uploadBytesFailed;
        this.uploadBytesSucceeded = other.uploadBytesSucceeded;
        this.totalUploadTimeInMillis = other.totalUploadTimeInMillis;
        this.totalDownloadsStarted = other.totalDownloadsStarted;
        this.totalDownloadsFailed = other.totalDownloadsFailed;
        this.totalDownloadsSucceeded = other.totalDownloadsSucceeded;
        this.downloadBytesStarted = other.downloadBytesStarted;
        this.downloadBytesFailed = other.downloadBytesFailed;
        this.downloadBytesSucceeded = other.downloadBytesSucceeded;
        this.totalDownloadTimeInMillis = other.totalDownloadTimeInMillis;
        initializeRemotestoreStatsAPIOnlyFields();
    }

    private void readNodesStatsAPIFields(StreamInput in) throws IOException {
        this.totalUploadsStarted = in.readLong();
        this.totalUploadsFailed = in.readLong();
        this.totalUploadsSucceeded = in.readLong();
        this.uploadBytesStarted = in.readLong();
        this.uploadBytesFailed = in.readLong();
        this.uploadBytesSucceeded = in.readLong();
        this.totalUploadTimeInMillis = in.readLong();

        this.totalDownloadsStarted = in.readLong();
        this.totalDownloadsFailed = in.readLong();
        this.totalDownloadsSucceeded = in.readLong();
        this.downloadBytesStarted = in.readLong();
        this.downloadBytesFailed = in.readLong();
        this.downloadBytesSucceeded = in.readLong();
        this.totalDownloadTimeInMillis = in.readLong();
    }

    private void initializeNodesStatsAPIFields() {
        this.totalUploadsStarted = 0L;
        this.totalUploadsFailed = 0L;
        this.totalUploadsSucceeded = 0L;
        this.uploadBytesStarted = 0L;
        this.uploadBytesFailed = 0L;
        this.uploadBytesSucceeded = 0L;
        this.totalUploadTimeInMillis = 0L;

        this.totalDownloadsStarted = 0L;
        this.totalDownloadsFailed = 0L;
        this.totalDownloadsSucceeded = 0L;
        this.downloadBytesStarted = 0L;
        this.downloadBytesFailed = 0L;
        this.downloadBytesSucceeded = 0L;
        this.totalDownloadTimeInMillis = 0L;
    }

    private void initializeRemotestoreStatsAPIOnlyFields() {
        // Currently, these are only exposed under _remotestore/stats API (until it gets deprecated).
        // Hence, we don't pass them over the wire between nodes in the _nodes/stats API.
        this.lastUploadTimestamp = 0L;
        this.uploadBytesMovingAverage = 0D;
        this.uploadBytesPerSecMovingAverage = 0D;
        this.uploadTimeMovingAverage = 0D;

        this.lastDownloadTimestamp = 0L;
        this.downloadBytesMovingAverage = 0D;
        this.downloadBytesPerSecMovingAverage = 0D;
        this.downloadTimeMovingAverage = 0D;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeNodesStatsAPIFields(out);
    }

    private void writeNodesStatsAPIFields(StreamOutput out) throws IOException {
        out.writeLong(totalUploadsStarted);
        out.writeLong(totalUploadsFailed);
        out.writeLong(totalUploadsSucceeded);
        out.writeLong(uploadBytesStarted);
        out.writeLong(uploadBytesFailed);
        out.writeLong(uploadBytesSucceeded);
        out.writeLong(totalUploadTimeInMillis);

        out.writeLong(totalDownloadsStarted);
        out.writeLong(totalDownloadsFailed);
        out.writeLong(totalDownloadsSucceeded);
        out.writeLong(downloadBytesStarted);
        out.writeLong(downloadBytesFailed);
        out.writeLong(downloadBytesSucceeded);
        out.writeLong(totalDownloadTimeInMillis);
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(REMOTE_STORE);

        builder.startObject(Flows.UPLOAD);
        addRemoteTranslogUploadStatsXContent(builder);
        builder.endObject(); // translog.remote_store.upload

        builder.startObject(Flows.DOWNLOAD);
        addRemoteTranslogDownloadStatsXContent(builder);
        builder.endObject(); // translog.remote_store.download

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

        this.totalDownloadsStarted += other.totalDownloadsStarted;
        this.totalDownloadsFailed += other.totalDownloadsFailed;
        this.totalDownloadsSucceeded += other.totalDownloadsSucceeded;
        this.downloadBytesStarted += other.downloadBytesStarted;
        this.downloadBytesFailed += other.downloadBytesFailed;
        this.downloadBytesSucceeded += other.downloadBytesSucceeded;
        this.totalDownloadTimeInMillis += other.totalDownloadTimeInMillis;
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

    void addRemoteTranslogDownloadStatsXContent(XContentBuilder builder) throws IOException {
        builder.startObject(Fields.TOTAL_DOWNLOADS);
        builder.field(SubFields.STARTED, totalDownloadsStarted)
            .field(SubFields.FAILED, totalDownloadsFailed)
            .field(SubFields.SUCCEEDED, totalDownloadsSucceeded);
        builder.endObject();

        builder.startObject(Fields.TOTAL_DOWNLOADS_IN_BYTES);
        builder.field(SubFields.STARTED, downloadBytesStarted)
            .field(SubFields.FAILED, downloadBytesFailed)
            .field(SubFields.SUCCEEDED, downloadBytesSucceeded);
        builder.endObject();

        builder.field(Fields.TOTAL_DOWNLOAD_TIME_IN_MILLIS, totalDownloadTimeInMillis);
    }
}
