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
import org.opensearch.index.remote.RemoteTranslogTransferTracker;

import java.io.IOException;
import java.util.Objects;

/**
 * Encapsulates the stats related to Remote Translog Store operations
 *
 * @opensearch.internal
 */
public class RemoteTranslogStats implements ToXContentFragment, Writeable {
    /**
     * Total number of Remote Translog Store uploads that have been started
     */
    private long totalUploadsStarted;

    /**
     * Total number of Remote Translog Store uploads that have failed.
     */
    private long totalUploadsFailed;

    /**
     * Total number of Remote Translog Store uploads that have been successful.
     */
    private long totalUploadsSucceeded;

    /**
     * Total number of byte uploads to Remote Translog Store that have been started.
     */
    private long uploadBytesStarted;

    /**
     * Total number of byte uploads to Remote Translog Store that have failed.
     */
    private long uploadBytesFailed;

    /**
     * Total number of byte uploads to Remote Translog Store that have been successful.
     */
    private long uploadBytesSucceeded;

    static final String REMOTE_STORE = "remote_store";

    static final String UPLOAD = "upload";

    static final class SubFields {
        static final String STARTED = "started";
        static final String SUCCEEDED = "succeeded";
        static final String FAILED = "failed";
    }

    static final class Fields {
        static final String TOTAL_UPLOADS = "total_uploads";
        static final String TOTAL_UPLOADS_IN_BYTES = "total_uploads_in_bytes";
    }

    public RemoteTranslogStats() {}

    public RemoteTranslogStats(StreamInput in) throws IOException {
        this.totalUploadsStarted = in.readVLong();
        this.totalUploadsFailed = in.readVLong();
        this.totalUploadsSucceeded = in.readVLong();
        this.uploadBytesStarted = in.readVLong();
        this.uploadBytesFailed = in.readVLong();
        this.uploadBytesSucceeded = in.readVLong();
    }

    public RemoteTranslogStats(RemoteTranslogTransferTracker.Stats transferTrackerStats) {
        this.totalUploadsStarted = transferTrackerStats.totalUploadsStarted;
        this.totalUploadsFailed = transferTrackerStats.totalUploadsFailed;
        this.totalUploadsSucceeded = transferTrackerStats.totalUploadsSucceeded;
        this.uploadBytesStarted = transferTrackerStats.uploadBytesStarted;
        this.uploadBytesFailed = transferTrackerStats.uploadBytesFailed;
        this.uploadBytesSucceeded = transferTrackerStats.uploadBytesSucceeded;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalUploadsStarted);
        out.writeVLong(totalUploadsFailed);
        out.writeVLong(totalUploadsSucceeded);
        out.writeVLong(uploadBytesStarted);
        out.writeVLong(uploadBytesFailed);
        out.writeVLong(uploadBytesSucceeded);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        RemoteTranslogStats other = (RemoteTranslogStats) obj;

        return this.totalUploadsStarted == other.totalUploadsStarted
            && this.totalUploadsFailed == other.totalUploadsFailed
            && this.totalUploadsSucceeded == other.totalUploadsSucceeded
            && this.uploadBytesStarted == other.uploadBytesStarted
            && this.uploadBytesFailed == other.uploadBytesFailed
            && this.uploadBytesSucceeded == other.uploadBytesSucceeded;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            totalUploadsStarted,
            totalUploadsFailed,
            totalUploadsSucceeded,
            uploadBytesStarted,
            uploadBytesFailed,
            uploadBytesSucceeded
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(REMOTE_STORE);

        builder.startObject(UPLOAD);
        addRemoteTranslogUploadStatsXContent(builder);
        builder.endObject(); // translog.remote_store.upload

        builder.endObject(); // translog.remote_store

        return builder;
    }

    public long getTotalUploadsStarted() {
        return totalUploadsStarted;
    }

    public long getTotalUploadsFailed() {
        return totalUploadsFailed;
    }

    public long getTotalUploadsSucceeded() {
        return totalUploadsSucceeded;
    }

    public long getUploadBytesStarted() {
        return uploadBytesStarted;
    }

    public long getUploadBytesFailed() {
        return uploadBytesFailed;
    }

    public long getUploadBytesSucceeded() {
        return uploadBytesSucceeded;
    }

    public void add(RemoteTranslogStats other) {
        if (other == null) {
            return;
        }

        this.totalUploadsStarted += other.totalUploadsStarted;
        this.totalUploadsFailed += other.totalUploadsFailed;
        this.totalUploadsSucceeded += other.totalUploadsSucceeded;
        this.uploadBytesStarted += other.uploadBytesStarted;
        this.uploadBytesFailed += other.uploadBytesFailed;
        this.uploadBytesSucceeded += other.uploadBytesSucceeded;
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
    }
}
