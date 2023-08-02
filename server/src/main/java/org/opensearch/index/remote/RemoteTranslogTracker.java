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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Stores Remote Translog Store-related stats for a given IndexShard.
 */
public class RemoteTranslogTracker {
    /**
     * The shard that this tracker is associated with
     */
    public final ShardId shardId;

    /**
     * Epoch timestamp of the last successful Remote Translog Store upload.
     */
    private final AtomicLong lastUploadTimestamp;

    /**
     * Total number of Remote Translog Store uploads that have been started.
     */
    private final AtomicLong totalUploadsStarted;

    /**
     * Total number of Remote Translog Store uploads that have failed.
     */
    private final AtomicLong totalUploadsFailed;

    /**
     * Total number of Remote Translog Store that have been successful.
     */
    private final AtomicLong totalUploadsSucceeded;

    /**
     * Total number of byte uploads to Remote Translog Store that have been started.
     */
    private final AtomicLong uploadBytesStarted;

    /**
     * Total number of byte uploads to Remote Translog Store that have failed.
     */
    private final AtomicLong uploadBytesFailed;

    /**
     * Total number of byte uploads to Remote Translog Store that have been successful.
     */
    private final AtomicLong uploadBytesSucceeded;

    /**
     * Total time spent on Remote Translog Store uploads.
     */
    private final AtomicLong totalUploadTimeInMillis;

    /**
     * Provides moving average over the last N total size in bytes of translog files uploaded as part of Remote Translog Store upload.
     * N is window size. Wrapped with {@code AtomicReference} for dynamic changes in window size.
     */
    private final AtomicReference<MovingAverage> uploadBytesMovingAverageReference;

    /**
     * This lock object is used for making sure we do not miss any data.
     */
    private final Object uploadBytesMutex;

    /**
     * Provides moving average over the last N upload speed (in bytes/s) of translog files uploaded as part of Remote Translog Store upload.
     * N is window size. Wrapped with {@code AtomicReference} for dynamic changes in window size.
     */
    private final AtomicReference<MovingAverage> uploadBytesPerSecMovingAverageReference;

    /**
     * This lock object is used for making sure we do not miss any data.
     */
    private final Object uploadBytesPerSecMutex;

    /**
     * Provides moving average over the last N overall upload time (in nanos) as part of Remote Translog Store upload. N is window size.
     * Wrapped with {@code AtomicReference} for dynamic changes in window size.
     */
    private final AtomicReference<MovingAverage> uploadTimeMsMovingAverageReference;

    /**
     * This lock object is used for making sure we do not miss any data.
     */
    private final Object uploadTimeMsMutex;

    public RemoteTranslogTracker(
        ShardId shardId,
        int uploadBytesMovingAverageWindowSize,
        int uploadBytesPerSecMovingAverageWindowSize,
        int uploadTimeMsMovingAverageWindowSize
    ) {
        this.shardId = shardId;
        this.lastUploadTimestamp = new AtomicLong(System.currentTimeMillis());
        this.totalUploadsStarted = new AtomicLong(0);
        this.totalUploadsFailed = new AtomicLong(0);
        this.totalUploadsSucceeded = new AtomicLong(0);
        this.uploadBytesStarted = new AtomicLong(0);
        this.uploadBytesFailed = new AtomicLong(0);
        this.uploadBytesSucceeded = new AtomicLong(0);
        this.totalUploadTimeInMillis = new AtomicLong(0);
        uploadBytesMutex = new Object();
        uploadBytesMovingAverageReference = new AtomicReference<>(new MovingAverage(uploadBytesMovingAverageWindowSize));
        uploadBytesPerSecMutex = new Object();
        uploadBytesPerSecMovingAverageReference = new AtomicReference<>(new MovingAverage(uploadBytesPerSecMovingAverageWindowSize));
        uploadTimeMsMutex = new Object();
        uploadTimeMsMovingAverageReference = new AtomicReference<>(new MovingAverage(uploadTimeMsMovingAverageWindowSize));
    }

    public long getTotalUploadsStarted() {
        return totalUploadsStarted.get();
    }

    public long getTotalUploadsFailed() {
        return totalUploadsFailed.get();
    }

    public long getTotalUploadsSucceeded() {
        return totalUploadsSucceeded.get();
    }

    public long getUploadBytesStarted() {
        return uploadBytesStarted.get();
    }

    public long getUploadBytesFailed() {
        return uploadBytesFailed.get();
    }

    public long getUploadBytesSucceeded() {
        return uploadBytesSucceeded.get();
    }

    public long getTotalUploadTimeInMillis() {
        return totalUploadTimeInMillis.get();
    }

    ShardId getShardId() {
        return shardId;
    }

    public void incrementUploadsStarted() {
        totalUploadsStarted.incrementAndGet();
    }

    public void incrementUploadsFailed() {
        checkTotal(totalUploadsStarted.get(), totalUploadsFailed.get(), totalUploadsSucceeded.get(), 1);
        totalUploadsFailed.incrementAndGet();
    }

    public void incrementUploadsSucceeded() {
        checkTotal(totalUploadsStarted.get(), totalUploadsFailed.get(), totalUploadsSucceeded.get(), 1);
        totalUploadsSucceeded.incrementAndGet();
    }

    public void addUploadBytesStarted(long count) {
        uploadBytesStarted.addAndGet(count);
    }

    public void addUploadBytesFailed(long count) {
        checkTotal(uploadBytesStarted.get(), uploadBytesFailed.get(), uploadBytesSucceeded.get(), count);
        uploadBytesFailed.addAndGet(count);
    }

    public void addUploadBytesSucceeded(long count) {
        checkTotal(uploadBytesStarted.get(), uploadBytesFailed.get(), uploadBytesSucceeded.get(), count);
        uploadBytesSucceeded.addAndGet(count);
    }

    public void addUploadTimeInMillis(long duration) {
        totalUploadTimeInMillis.addAndGet(duration);
    }

    public long getLastUploadTimestamp() {
        return lastUploadTimestamp.get();
    }

    public void setLastUploadTimestamp(long lastUploadTimestamp) {
        this.lastUploadTimestamp.set(lastUploadTimestamp);
    }

    boolean isUploadBytesMovingAverageReady() {
        return uploadBytesMovingAverageReference.get().isReady();
    }

    public double getUploadBytesMovingAverage() {
        return uploadBytesMovingAverageReference.get().getAverage();
    }

    public void updateUploadBytesMovingAverage(long count) {
        synchronized (uploadBytesMutex) {
            this.uploadBytesMovingAverageReference.get().record(count);
        }
    }

    boolean isUploadBytesPerSecMovingAverageReady() {
        return uploadBytesPerSecMovingAverageReference.get().isReady();
    }

    public double getUploadBytesPerSecMovingAverage() {
        return uploadBytesPerSecMovingAverageReference.get().getAverage();
    }

    public void updateUploadBytesPerSecMovingAverage(long speed) {
        synchronized (uploadBytesPerSecMutex) {
            this.uploadBytesPerSecMovingAverageReference.get().record(speed);
        }
    }

    boolean isUploadTimeMovingAverageReady() {
        return uploadTimeMsMovingAverageReference.get().isReady();
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

    /**
     * Gets the tracker's state as seen in the stats API
     * @return Stats object with the tracker's stats
     */
    public RemoteTranslogTracker.Stats stats() {
        return new RemoteTranslogTracker.Stats(
            shardId,
            lastUploadTimestamp.get(),
            totalUploadsStarted.get(),
            totalUploadsSucceeded.get(),
            totalUploadsFailed.get(),
            uploadBytesStarted.get(),
            uploadBytesSucceeded.get(),
            uploadBytesFailed.get(),
            totalUploadTimeInMillis.get(),
            uploadBytesMovingAverageReference.get().getAverage(),
            uploadBytesPerSecMovingAverageReference.get().getAverage(),
            uploadTimeMsMovingAverageReference.get().getAverage()
        );
    }

    /**
     * Validates that the sum of successful operations, failed operations, and the number of operations to add (irrespective of failed/successful) does not exceed the number of operations originally started
     * @param startedCount Number of operations started
     * @param failedCount Number of operations failed
     * @param succeededCount Number of operations successful
     * @param countToAdd Number of operations to add
     */
    private void checkTotal(long startedCount, long failedCount, long succeededCount, long countToAdd) {
        long delta = startedCount - (failedCount + succeededCount + countToAdd);
        assert delta >= 0 : "Sum of failure count ("
            + failedCount
            + "), success count ("
            + succeededCount
            + "), and count to add ("
            + countToAdd
            + ") cannot exceed started count ("
            + startedCount
            + ")";
    }

    /**
     * Represents the tracker's state as seen in the stats API.
     *
     * @opensearch.internal
     */
    public static class Stats implements Writeable {

        public final ShardId shardId;

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

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            RemoteTranslogTracker.Stats other = (RemoteTranslogTracker.Stats) obj;

            return this.shardId.toString().equals(other.shardId.toString())
                && this.lastUploadTimestamp == other.lastUploadTimestamp
                && this.totalUploadsStarted == other.totalUploadsStarted
                && this.totalUploadsFailed == other.totalUploadsFailed
                && this.totalUploadsSucceeded == other.totalUploadsSucceeded
                && this.uploadBytesStarted == other.uploadBytesStarted
                && this.uploadBytesFailed == other.uploadBytesFailed
                && this.uploadBytesSucceeded == other.uploadBytesSucceeded
                && this.totalUploadTimeInMillis == other.totalUploadTimeInMillis
                && Double.compare(this.uploadBytesMovingAverage, other.uploadBytesMovingAverage) == 0
                && Double.compare(this.uploadBytesPerSecMovingAverage, other.uploadBytesPerSecMovingAverage) == 0
                && Double.compare(this.uploadTimeMovingAverage, other.uploadTimeMovingAverage) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                shardId.toString(),
                lastUploadTimestamp,
                totalUploadsStarted,
                totalUploadsFailed,
                totalUploadsSucceeded,
                uploadBytesStarted,
                uploadBytesFailed,
                uploadBytesSucceeded,
                totalUploadTimeInMillis,
                uploadBytesMovingAverage,
                uploadBytesPerSecMovingAverage,
                uploadTimeMovingAverage
            );
        }
    }

    /**
     * Validates if the stats in this tracker and the stats contained in the given stats object are same or not
     * @param other Stats object to compare this tracker against
     * @return true if stats are same and false otherwise
     */
    boolean hasSameStatsAs(RemoteTranslogTracker.Stats other) {
        return this.getShardId().toString().equals(other.shardId.toString())
            && this.getLastUploadTimestamp() == other.lastUploadTimestamp
            && this.getTotalUploadsStarted() == other.totalUploadsStarted
            && this.getTotalUploadsFailed() == other.totalUploadsFailed
            && this.getTotalUploadsSucceeded() == other.totalUploadsSucceeded
            && this.getUploadBytesStarted() == other.uploadBytesStarted
            && this.getUploadBytesFailed() == other.uploadBytesFailed
            && this.getUploadBytesSucceeded() == other.uploadBytesSucceeded
            && this.getTotalUploadTimeInMillis() == other.totalUploadTimeInMillis
            && Double.compare(this.getUploadBytesMovingAverage(), other.uploadBytesMovingAverage) == 0
            && Double.compare(this.getUploadBytesPerSecMovingAverage(), other.uploadBytesPerSecMovingAverage) == 0
            && Double.compare(this.getUploadTimeMovingAverage(), other.uploadTimeMovingAverage) == 0;
    }
}
