/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.common.util.MovingAverage;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Stores Remote Translog Store-related stats for a given IndexShard.
 */
public class RemoteTranslogTracker {
    public final ShardId shardId;

    /**
     * Number of translog operations not persisted to Remote Translog Store.
     * This would be relevant for async translog durability.
     */
    private volatile long lag;

    /**
     * Epoch timestamp of the last successful Remote Translog Store upload.
     */
    private volatile long lastUploadTimestamp;

    /**
     * Total number of Remote Translog Store uploads that have been started.
     */
    private volatile long totalUploadsStarted;

    /**
     * Total number of Remote Translog Store uploads that have failed.
     */
    private volatile long totalUploadsFailed;

    /**
     * Total number of Remote Translog Store that have been successful.
     */
    private volatile long totalUploadsSucceeded;

    /**
     * Total number of byte uploads to Remote Translog Store that have been started.
     */
    private volatile long uploadBytesStarted;

    /**
     * Total number of byte uploads to Remote Translog Store that have failed.
     */
    private volatile long uploadBytesFailed;

    /**
     * Total number of byte uploads to Remote Translog Store that have been successful.
     */
    private volatile long uploadBytesSucceeded;

    /**
     * Total time spent on Remote Translog Store uploads.
     */
    private volatile long totalUploadTimeInMillis;

    /**
     * Size of a Remote Translog Store upload in bytes.
     */
    private volatile double uploadBytesMovingAverage;

    /**
     * Speed of a Remote Translog Store upload in bytes-per-second.
     */
    private volatile double uploadBytesPerSecMovingAverage;

    /**
     *  Time taken by a Remote Translog Store upload.
     */
    private volatile double uploadTimeMovingAverage;

    /**
     * Provides moving average over the last N total size in bytes of segment files uploaded as part of remote refresh.
     * N is window size. Wrapped with {@code AtomicReference} for dynamic changes in window size.
     */
    private final AtomicReference<MovingAverage> uploadBytesMovingAverageReference;

    /**
     * This lock object is used for making sure we do not miss any data
     */
    private final Object uploadBytesMutex = new Object();

    /**
     * Provides moving average over the last N upload speed (in bytes/s) of segment files uploaded as part of remote refresh.
     * N is window size. Wrapped with {@code AtomicReference} for dynamic changes in window size.
     */
    private final AtomicReference<MovingAverage> uploadBytesPerSecMovingAverageReference;

    private final Object uploadBytesPerSecMutex = new Object();

    /**
     * Provides moving average over the last N overall upload time (in nanos) as part of remote refresh.N is window size.
     * Wrapped with {@code AtomicReference} for dynamic changes in window size.
     */
    private final AtomicReference<MovingAverage> uploadTimeMsMovingAverageReference;

    private final Object uploadTimeMsMutex = new Object();

    public RemoteTranslogTracker(ShardId shardId, int uploadBytesMovingAverageWindowSize, int uploadBytesPerSecMovingAverageWindowSize, int uploadTimeMsMovingAverageWindowSize) {
        this.shardId = shardId;
        this.lag = 0; // todo
        long currentTimeMs = System.nanoTime() / 1_000_000L;
        this.lastUploadTimestamp = currentTimeMs;
        this.totalUploadsStarted = 0;
        this.totalUploadsFailed = 0;
        this.totalUploadsSucceeded = 0;
        this.uploadBytesStarted = 0;
        this.uploadBytesFailed = 0;
        this.uploadBytesSucceeded = 0;
        this.totalUploadTimeInMillis = 0;
        this.uploadBytesMovingAverage = 0;
        this.uploadBytesPerSecMovingAverage = 0;
        this.uploadTimeMovingAverage = 0;
        uploadBytesMovingAverageReference = new AtomicReference<>(new MovingAverage(uploadBytesMovingAverageWindowSize));
        uploadBytesPerSecMovingAverageReference = new AtomicReference<>(new MovingAverage(uploadBytesPerSecMovingAverageWindowSize));
        uploadTimeMsMovingAverageReference = new AtomicReference<>(new MovingAverage(uploadTimeMsMovingAverageWindowSize));
    }

    public void incrementUploadsStarted() {
        ++totalUploadsStarted;
    }

    public void incrementUploadsFailed() {
        ++totalUploadsFailed;
    }

    public void incrementUploadsSucceeded() {
        ++totalUploadsSucceeded;
    }

    public void addUploadBytesStarted(long count) {
        uploadBytesStarted += count;
    }

    public void addUploadBytesFailed(long count) {
        uploadBytesFailed += count;
    }

    public void addUploadBytesSucceeded(long count) {
        uploadBytesSucceeded += count;
    }

    public void addUploadTimeInMillis(long duration) {
        totalUploadTimeInMillis += duration;
    }

    public long getLag() {
        return lag;
    }

    public void setLag(long lag) {
        this.lag = lag;
    }

    public long getLastUploadTimestamp() {
        return lastUploadTimestamp;
    }

    public void setLastUploadTimestamp(long lastUploadTimestamp) {
        this.lastUploadTimestamp = lastUploadTimestamp;
    }

    public double getUploadBytesMovingAverage() {
        return uploadBytesMovingAverageReference.get().getAverage();
    }

    public void updateUploadBytesMovingAverage(long count) {
        synchronized (uploadBytesMutex) {
            this.uploadBytesMovingAverageReference.get().record(count);
        }
    }

    public double getUploadBytesPerSecMovingAverage() {
        return uploadBytesPerSecMovingAverageReference.get().getAverage();
    }

    public void updateUploadBytesPerSecMovingAverage(long speed) {
        synchronized (uploadBytesPerSecMutex) {
            this.uploadBytesPerSecMovingAverageReference.get().record(speed);
        }
    }

    public double getUploadTimeMovingAverage() {
        return uploadTimeMsMovingAverageReference.get().getAverage();
    }

    public void updateUploadTimeMovingAverage(long duration) {
        synchronized (uploadTimeMsMutex) {
            this.uploadTimeMsMovingAverageReference.get().record(duration);
        }
    }

    /**
     * Updates the window size for data collection of upload bytes. This also resets any data collected so far.
     *
     * @param updatedSize the updated size
     */
    void updateUploadBytesMovingAverageWindowSize(int updatedSize) {
        synchronized (uploadBytesMutex) {
            this.uploadBytesMovingAverageReference.set(this.uploadBytesMovingAverageReference.get().copyWithSize(updatedSize));
        }
    }

    /**
     * Updates the window size for data collection of upload bytes per second. This also resets any data collected so far.
     *
     * @param updatedSize the updated size
     */
    void updateUploadBytesPerSecMovingAverageWindowSize(int updatedSize) {
        synchronized (uploadBytesPerSecMutex) {
            this.uploadBytesPerSecMovingAverageReference.set(this.uploadBytesPerSecMovingAverageReference.get().copyWithSize(updatedSize));
        }
    }

    /**
     * Updates the window size for data collection of upload time (ms). This also resets any data collected so far.
     *
     * @param updatedSize the updated size
     */
    void updateUploadTimeMsMovingAverageWindowSize(int updatedSize) {
        synchronized (uploadTimeMsMutex) {
            this.uploadTimeMsMovingAverageReference.set(this.uploadTimeMsMovingAverageReference.get().copyWithSize(updatedSize));
        }
    }

    public RemoteTranslogTracker.Stats stats() {
        return new RemoteTranslogTracker.Stats(
            shardId,
            lag,
            lastUploadTimestamp,
            totalUploadsStarted,
            totalUploadsSucceeded,
            totalUploadsFailed,
            uploadBytesStarted,
            uploadBytesSucceeded,
            uploadBytesFailed,
            totalUploadTimeInMillis,
            uploadBytesMovingAverageReference.get().getAverage(),
            uploadBytesPerSecMovingAverageReference.get().getAverage(),
            uploadTimeMsMovingAverageReference.get().getAverage()
        );
    }

    /**
     * Represents the tracker's state as seen in the stats API.
     *
     * @opensearch.internal
     */
    public static class Stats implements Writeable {

        public final ShardId shardId;

        /**
         * Number of translog operations not persisted to Remote Translog Store.
         * This would be relevant for async translog durability.
         */
        public final long lag;

        /**
         * Epoch timestamp of the last successful Remote Translog Store upload.
         */
        public final long lastUploadTimestamp;

        /**
         * Total number of Remote Translog Store uploads that have been started.
         */
        public final long totalUploadsStarted;

        /**
         * Total number of Remote Translog Store uploads that have failed.
         */
        public final long totalUploadsFailed;

        /**
         * Total number of Remote Translog Store that have been successful.
         */
        public final long totalUploadsSucceeded;

        /**
         * Total number of byte uploads to Remote Translog Store that have been started.
         */
        public final long uploadBytesStarted;

        /**
         * Total number of byte uploads to Remote Translog Store that have failed.
         */
        public final long uploadBytesFailed;

        /**
         * Total number of byte uploads to Remote Translog Store that have been successful.
         */
        public final long uploadBytesSucceeded;

        /**
         * Total time spent on Remote Translog Store uploads.
         */
        public final long totalUploadTimeInMillis;

        /**
         * Size of a Remote Translog Store upload in bytes.
         */
        public final double uploadBytesMovingAverage;

        /**
         * Speed of a Remote Translog Store upload in bytes-per-second.
         */
        public final double uploadBytesPerSecMovingAverage;

        /**
         *  Time taken by a Remote Translog Store upload.
         */
        public final double uploadTimeMovingAverage;

        public Stats(
            ShardId shardId,
            long lag,
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
            this.shardId = shardId;
            this.lag = lag;
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

        public Stats(StreamInput in) throws IOException {
            try {
                this.shardId = new ShardId(in);
                this.lag = in.readLong();
                this.lastUploadTimestamp = in.readLong();
                this.totalUploadsStarted = in.readLong();
                this.totalUploadsFailed = in.readLong();
                this.totalUploadsSucceeded = in.readLong();
                this.uploadBytesStarted = in.readLong();
                this.uploadBytesFailed = in.readLong();
                this.uploadBytesSucceeded = in.readLong();
                this.totalUploadTimeInMillis = in.readLong();
                this.uploadBytesMovingAverage = in.readDouble();
                this.uploadBytesPerSecMovingAverage = in.readDouble();
                this.uploadTimeMovingAverage = in.readDouble();
            } catch (IOException e) {
                throw e;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            shardId.writeTo(out);
            out.writeLong(lag);
            out.writeLong(lastUploadTimestamp);
            out.writeLong(totalUploadsStarted);
            out.writeLong(totalUploadsFailed);
            out.writeLong(totalUploadsSucceeded);
            out.writeLong(uploadBytesStarted);
            out.writeLong(uploadBytesFailed);
            out.writeLong(uploadBytesSucceeded);
            out.writeLong(totalUploadTimeInMillis);
            out.writeDouble(uploadBytesMovingAverage);
            out.writeDouble(uploadBytesPerSecMovingAverage);
            out.writeDouble(uploadTimeMovingAverage);
        }
    }
}
