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

    /**
     * Epoch timestamp of the last successful Remote Translog Store upload.
     */
    private final AtomicLong lastDownloadTimestamp;

    /**
     * Total number of Remote Translog Store uploads that have been started.
     */
    private final AtomicLong totalDownloadsStarted;

    /**
     * Total number of Remote Translog Store uploads that have failed.
     */
    private final AtomicLong totalDownloadsFailed;

    /**
     * Total number of Remote Translog Store that have been successful.
     */
    private final AtomicLong totalDownloadsSucceeded;

    /**
     * Total number of byte uploads to Remote Translog Store that have been started.
     */
    private final AtomicLong downloadBytesStarted;

    /**
     * Total number of byte uploads to Remote Translog Store that have failed.
     */
    private final AtomicLong downloadBytesFailed;

    /**
     * Total number of byte uploads to Remote Translog Store that have been successful.
     */
    private final AtomicLong downloadBytesSucceeded;

    /**
     * Total time spent on Remote Translog Store uploads.
     */
    private final AtomicLong totalDownloadTimeInMillis;

    /**
     * Provides moving average over the last N total size in bytes of translog files uploaded as part of Remote Translog Store upload.
     * N is window size. Wrapped with {@code AtomicReference} for dynamic changes in window size.
     */
    private final AtomicReference<MovingAverage> downloadBytesMovingAverageReference;

    /**
     * This lock object is used for making sure we do not miss any data.
     */
    private final Object downloadBytesMutex;

    /**
     * Provides moving average over the last N upload speed (in bytes/s) of translog files uploaded as part of Remote Translog Store upload.
     * N is window size. Wrapped with {@code AtomicReference} for dynamic changes in window size.
     */
    private final AtomicReference<MovingAverage> downloadBytesPerSecMovingAverageReference;

    /**
     * This lock object is used for making sure we do not miss any data.
     */
    private final Object downloadBytesPerSecMutex;

    /**
     * Provides moving average over the last N overall upload time (in nanos) as part of Remote Translog Store upload. N is window size.
     * Wrapped with {@code AtomicReference} for dynamic changes in window size.
     */
    private final AtomicReference<MovingAverage> downloadTimeMsMovingAverageReference;

    /**
     * This lock object is used for making sure we do not miss any data.
     */
    private final Object downloadTimeMsMutex;

    public RemoteTranslogTracker(
        ShardId shardId,
        int uploadBytesMovingAverageWindowSize,
        int uploadBytesPerSecMovingAverageWindowSize,
        int uploadTimeMsMovingAverageWindowSize,
        int downloadBytesMovingAverageWindowSize,
        int downloadBytesPerSecMovingAverageWindowSize,
        int downloadTimeMsMovingAverageWindowSize
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

        this.lastDownloadTimestamp = new AtomicLong(System.currentTimeMillis());
        this.totalDownloadsStarted = new AtomicLong(0);
        this.totalDownloadsFailed = new AtomicLong(0);
        this.totalDownloadsSucceeded = new AtomicLong(0);
        this.downloadBytesStarted = new AtomicLong(0);
        this.downloadBytesFailed = new AtomicLong(0);
        this.downloadBytesSucceeded = new AtomicLong(0);
        this.totalDownloadTimeInMillis = new AtomicLong(0);
        downloadBytesMutex = new Object();
        downloadBytesMovingAverageReference = new AtomicReference<>(new MovingAverage(downloadBytesMovingAverageWindowSize));
        downloadBytesPerSecMutex = new Object();
        downloadBytesPerSecMovingAverageReference = new AtomicReference<>(new MovingAverage(downloadBytesPerSecMovingAverageWindowSize));
        downloadTimeMsMutex = new Object();
        downloadTimeMsMovingAverageReference = new AtomicReference<>(new MovingAverage(downloadTimeMsMovingAverageWindowSize));
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

    public long getTotalDownloadsStarted() {
        return totalDownloadsStarted.get();
    }

    public long getTotalDownloadsFailed() {
        return totalDownloadsFailed.get();
    }

    public long getTotalDownloadsSucceeded() {
        return totalDownloadsSucceeded.get();
    }

    public long getDownloadBytesStarted() {
        return downloadBytesStarted.get();
    }

    public long getDownloadBytesFailed() {
        return downloadBytesFailed.get();
    }

    public long getDownloadBytesSucceeded() {
        return downloadBytesSucceeded.get();
    }

    public long getTotalDownloadTimeInMillis() {
        return totalDownloadTimeInMillis.get();
    }

    public void incrementDownloadsStarted() {
        totalDownloadsStarted.incrementAndGet();
    }

    public void incrementDownloadsFailed() {
        checkTotal(totalDownloadsStarted.get(), totalDownloadsFailed.get(), totalDownloadsSucceeded.get(), 1);
        totalDownloadsFailed.incrementAndGet();
    }

    public void incrementDownloadsSucceeded() {
        checkTotal(totalDownloadsStarted.get(), totalDownloadsFailed.get(), totalDownloadsSucceeded.get(), 1);
        totalDownloadsSucceeded.incrementAndGet();
    }

    public void addDownloadBytesStarted(long count) {
        downloadBytesStarted.addAndGet(count);
    }

    public void addDownloadBytesFailed(long count) {
        checkTotal(downloadBytesStarted.get(), downloadBytesFailed.get(), downloadBytesSucceeded.get(), count);
        downloadBytesFailed.addAndGet(count);
    }

    public void addDownloadBytesSucceeded(long count) {
        checkTotal(downloadBytesStarted.get(), downloadBytesFailed.get(), downloadBytesSucceeded.get(), count);
        downloadBytesSucceeded.addAndGet(count);
    }

    public void addDownloadTimeInMillis(long duration) {
        totalDownloadTimeInMillis.addAndGet(duration);
    }

    public long getLastDownloadTimestamp() {
        return lastDownloadTimestamp.get();
    }

    public void setLastDownloadTimestamp(long lastDownloadTimestamp) {
        this.lastDownloadTimestamp.set(lastDownloadTimestamp);
    }

    boolean isDownloadBytesMovingAverageReady() {
        return downloadBytesMovingAverageReference.get().isReady();
    }

    public double getDownloadBytesMovingAverage() {
        return downloadBytesMovingAverageReference.get().getAverage();
    }

    public void updateDownloadBytesMovingAverage(long count) {
        synchronized (downloadBytesMutex) {
            this.downloadBytesMovingAverageReference.get().record(count);
        }
    }

    boolean isDownloadBytesPerSecMovingAverageReady() {
        return downloadBytesPerSecMovingAverageReference.get().isReady();
    }

    public double getDownloadBytesPerSecMovingAverage() {
        return downloadBytesPerSecMovingAverageReference.get().getAverage();
    }

    public void updateDownloadBytesPerSecMovingAverage(long speed) {
        synchronized (downloadBytesPerSecMutex) {
            this.downloadBytesPerSecMovingAverageReference.get().record(speed);
        }
    }

    boolean isDownloadTimeMovingAverageReady() {
        return downloadTimeMsMovingAverageReference.get().isReady();
    }

    public double getDownloadTimeMovingAverage() {
        return downloadTimeMsMovingAverageReference.get().getAverage();
    }

    public void updateDownloadTimeMovingAverage(long duration) {
        synchronized (downloadTimeMsMutex) {
            this.downloadTimeMsMovingAverageReference.get().record(duration);
        }
    }

    /**
     * Updates the window size for data collection of upload bytes. This also resets any data collected so far.
     *
     * @param updatedSize the updated size
     */
    void updateDownloadBytesMovingAverageWindowSize(int updatedSize) {
        synchronized (downloadBytesMutex) {
            this.downloadBytesMovingAverageReference.set(this.downloadBytesMovingAverageReference.get().copyWithSize(updatedSize));
        }
    }

    /**
     * Updates the window size for data collection of upload bytes per second. This also resets any data collected so far.
     *
     * @param updatedSize the updated size
     */
    void updateDownloadBytesPerSecMovingAverageWindowSize(int updatedSize) {
        synchronized (downloadBytesPerSecMutex) {
            this.downloadBytesPerSecMovingAverageReference.set(this.downloadBytesPerSecMovingAverageReference.get().copyWithSize(updatedSize));
        }
    }

    /**
     * Updates the window size for data collection of upload time (ms). This also resets any data collected so far.
     *
     * @param updatedSize the updated size
     */
    void updateDownloadTimeMsMovingAverageWindowSize(int updatedSize) {
        synchronized (downloadTimeMsMutex) {
            this.downloadTimeMsMovingAverageReference.set(this.downloadTimeMsMovingAverageReference.get().copyWithSize(updatedSize));
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
            uploadTimeMsMovingAverageReference.get().getAverage(),
            lastDownloadTimestamp.get(),
            totalDownloadsStarted.get(),
            totalDownloadsSucceeded.get(),
            totalDownloadsFailed.get(),
            downloadBytesStarted.get(),
            downloadBytesSucceeded.get(),
            downloadBytesFailed.get(),
            totalDownloadTimeInMillis.get(),
            downloadBytesMovingAverageReference.get().getAverage(),
            downloadBytesPerSecMovingAverageReference.get().getAverage(),
            downloadTimeMsMovingAverageReference.get().getAverage()
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

        /**
         * Epoch timestamp of the last successful Remote Translog Store upload.
         */
        public final long lastDownloadTimestamp;

        /**
         * Total number of Remote Translog Store uploads that have been started.
         */
        public final long totalDownloadsStarted;

        /**
         * Total number of Remote Translog Store uploads that have failed.
         */
        public final long totalDownloadsFailed;

        /**
         * Total number of Remote Translog Store that have been successful.
         */
        public final long totalDownloadsSucceeded;

        /**
         * Total number of byte uploads to Remote Translog Store that have been started.
         */
        public final long downloadBytesStarted;

        /**
         * Total number of byte uploads to Remote Translog Store that have failed.
         */
        public final long downloadBytesFailed;

        /**
         * Total number of byte uploads to Remote Translog Store that have been successful.
         */
        public final long downloadBytesSucceeded;

        /**
         * Total time spent on Remote Translog Store uploads.
         */
        public final long totalDownloadTimeInMillis;

        /**
         * Size of a Remote Translog Store upload in bytes.
         */
        public final double downloadBytesMovingAverage;

        /**
         * Speed of a Remote Translog Store upload in bytes-per-second.
         */
        public final double downloadBytesPerSecMovingAverage;

        /**
         *  Time taken by a Remote Translog Store upload.
         */
        public final double downloadTimeMovingAverage;

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

                this.lastDownloadTimestamp = in.readLong();
                this.totalDownloadsStarted = in.readLong();
                this.totalDownloadsFailed = in.readLong();
                this.totalDownloadsSucceeded = in.readLong();
                this.downloadBytesStarted = in.readLong();
                this.downloadBytesFailed = in.readLong();
                this.downloadBytesSucceeded = in.readLong();
                this.totalDownloadTimeInMillis = in.readLong();
                this.downloadBytesMovingAverage = in.readDouble();
                this.downloadBytesPerSecMovingAverage = in.readDouble();
                this.downloadTimeMovingAverage = in.readDouble();
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

            out.writeLong(lastDownloadTimestamp);
            out.writeLong(totalDownloadsStarted);
            out.writeLong(totalDownloadsFailed);
            out.writeLong(totalDownloadsSucceeded);
            out.writeLong(downloadBytesStarted);
            out.writeLong(downloadBytesFailed);
            out.writeLong(downloadBytesSucceeded);
            out.writeLong(totalDownloadTimeInMillis);
            out.writeDouble(downloadBytesMovingAverage);
            out.writeDouble(downloadBytesPerSecMovingAverage);
            out.writeDouble(downloadTimeMovingAverage);
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
                && Double.compare(this.uploadTimeMovingAverage, other.uploadTimeMovingAverage) == 0
                && this.lastDownloadTimestamp == other.lastDownloadTimestamp
                && this.totalDownloadsStarted == other.totalDownloadsStarted
                && this.totalDownloadsFailed == other.totalDownloadsFailed
                && this.totalDownloadsSucceeded == other.totalDownloadsSucceeded
                && this.downloadBytesStarted == other.downloadBytesStarted
                && this.downloadBytesFailed == other.downloadBytesFailed
                && this.downloadBytesSucceeded == other.downloadBytesSucceeded
                && this.totalDownloadTimeInMillis == other.totalDownloadTimeInMillis
                && Double.compare(this.downloadBytesMovingAverage, other.downloadBytesMovingAverage) == 0
                && Double.compare(this.downloadBytesPerSecMovingAverage, other.downloadBytesPerSecMovingAverage) == 0
                && Double.compare(this.downloadTimeMovingAverage, other.downloadTimeMovingAverage) == 0;
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
                uploadTimeMovingAverage,
                lastDownloadTimestamp,
                totalDownloadsStarted,
                totalDownloadsFailed,
                totalDownloadsSucceeded,
                downloadBytesStarted,
                downloadBytesFailed,
                downloadBytesSucceeded,
                totalDownloadTimeInMillis,
                downloadBytesMovingAverage,
                downloadBytesPerSecMovingAverage,
                downloadTimeMovingAverage
            );
        }
    }

    /**
     * Validates if the stats in this tracker and the stats contained in the given stats object are same or not
     * @param other Stats object to compare this tracker against
     * @return true if stats are same and false otherwise
     */
    boolean hasSameStatsAs(RemoteTranslogTracker.Stats other) {
        return this.stats().equals(other);
    }
}
