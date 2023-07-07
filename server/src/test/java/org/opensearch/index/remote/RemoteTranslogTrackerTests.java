/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.junit.Before;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;

import static org.mockito.Mockito.mock;

public class RemoteTranslogTrackerTests extends OpenSearchTestCase {
    private RemoteStorePressureSettings pressureSettings;

    private ClusterService clusterService;

    private ThreadPool threadPool;

    private ShardId shardId;

    private RemoteTranslogTracker tracker;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("remote_store_pressure_settings_test");
        clusterService = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool
        );
        pressureSettings = new RemoteStorePressureSettings(clusterService, Settings.EMPTY, mock(RemoteStorePressureService.class));
        shardId = new ShardId("index", "uuid", 0);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    @Before
    public void initTracker() {
        tracker = new RemoteTranslogTracker(
            shardId,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
    }

    public void testGetShardId() {
        assertEquals(shardId, tracker.getShardId());
    }

    public void testIncrementUploadsStarted() {
        populateUploadsStarted();
    }

    public void testIncrementUploadsFailed() {
        populateUploadsStarted();
        populateUploadsFailed();
    }

    public void testInvalidIncrementUploadsFailed() {
        populateUploadsStarted();
        populateUploadsSucceeded();
        AssertionError error = assertThrows(AssertionError.class, () -> tracker.incrementUploadsFailed());
        assertTrue(error.getMessage().contains("Sum of failure count ("));
    }

    public void testIncrementUploadsSucceeded() {
        populateUploadsStarted();
        populateUploadsSucceeded();
    }

    public void testInvalidIncrementUploadsSucceeded() {
        populateUploadsStarted();
        populateUploadsFailed();
        AssertionError error = assertThrows(AssertionError.class, this::populateUploadsSucceeded);
        assertTrue(error.getMessage().contains("Sum of failure count ("));
    }

    public void testSetUploadBytesStarted() {
        populateUploadBytesStarted();
    }

    public void testSetUploadBytesFailed() {
        populateUploadBytesStarted();
        assertEquals(0L, tracker.getUploadBytesFailed());
        long count1 = randomIntBetween(1, (int) tracker.getUploadBytesStarted() / 4);
        tracker.addUploadBytesFailed(count1);
        assertEquals(count1, tracker.getUploadBytesFailed());
        long count2 = randomIntBetween(1, (int) tracker.getUploadBytesStarted() / 4);
        tracker.addUploadBytesFailed(count2);
        assertEquals(count1 + count2, tracker.getUploadBytesFailed());
    }

    public void testInvalidSetUploadBytesFailed() {
        populateUploadBytesStarted();
        tracker.addUploadBytesSucceeded(tracker.getUploadBytesStarted());
        AssertionError error = assertThrows(AssertionError.class, () -> tracker.addUploadBytesFailed(1L));
        assertTrue(error.getMessage().contains("Sum of failure count ("));
    }

    public void testSetUploadBytesSucceeded() {
        populateUploadBytesStarted();
        assertEquals(0L, tracker.getUploadBytesSucceeded());
        long count1 = randomIntBetween(1, (int) tracker.getUploadBytesStarted() / 4);
        tracker.addUploadBytesSucceeded(count1);
        assertEquals(count1, tracker.getUploadBytesSucceeded());
        long count2 = randomIntBetween(1, (int) tracker.getUploadBytesStarted() / 4);
        tracker.addUploadBytesSucceeded(count2);
        assertEquals(count1 + count2, tracker.getUploadBytesSucceeded());
    }

    public void testInvalidSetUploadBytesSucceeded() {
        populateUploadBytesStarted();
        tracker.addUploadBytesFailed(tracker.getUploadBytesStarted());
        AssertionError error = assertThrows(AssertionError.class, () -> tracker.addUploadBytesSucceeded(1L));
        assertTrue(error.getMessage().contains("Sum of failure count ("));
    }

    public void testAddUploadTimeInMillis() {
        assertEquals(0L, tracker.getTotalUploadTimeInMillis());
        int duration1 = randomIntBetween(10, 50);
        tracker.addUploadTimeInMillis(duration1);
        assertEquals(duration1, tracker.getTotalUploadTimeInMillis());
        int duration2 = randomIntBetween(10, 50);
        tracker.addUploadTimeInMillis(duration2);
        assertEquals(duration1 + duration2, tracker.getTotalUploadTimeInMillis());
    }

    public void testSetLastUploadTimestamp() {
        long lastUploadTimestamp = System.nanoTime() / 1_000_000L + randomIntBetween(10, 100);
        tracker.setLastUploadTimestamp(lastUploadTimestamp);
        assertEquals(lastUploadTimestamp, tracker.getLastUploadTimestamp());
    }

    public void testUpdateUploadBytesMovingAverage() {
        int uploadBytesMovingAverageWindowSize = 20;
        tracker = new RemoteTranslogTracker(
            shardId,
            uploadBytesMovingAverageWindowSize,
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        assertFalse(tracker.isUploadBytesMovingAverageReady());

        long sum = 0;
        for (int i = 1; i < uploadBytesMovingAverageWindowSize; i++) {
            tracker.updateUploadBytesMovingAverage(i);
            sum += i;
            assertFalse(tracker.isUploadBytesMovingAverageReady());
            assertEquals((double) sum / i, tracker.getUploadBytesMovingAverage(), 0.0d);
        }

        tracker.updateUploadBytesMovingAverage(uploadBytesMovingAverageWindowSize);
        sum += uploadBytesMovingAverageWindowSize;
        assertTrue(tracker.isUploadBytesMovingAverageReady());
        assertEquals((double) sum / uploadBytesMovingAverageWindowSize, tracker.getUploadBytesMovingAverage(), 0.0d);

        tracker.updateUploadBytesMovingAverage(100);
        sum = sum + 100 - 1;
        assertEquals((double) sum / uploadBytesMovingAverageWindowSize, tracker.getUploadBytesMovingAverage(), 0.0d);
    }

    public void testUpdateUploadBytesPerSecMovingAverage() {
        int uploadBytesPerSecMovingAverageWindowSize = 20;
        tracker = new RemoteTranslogTracker(
            shardId,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            uploadBytesPerSecMovingAverageWindowSize,
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        assertFalse(tracker.isUploadBytesPerSecMovingAverageReady());

        long sum = 0;
        for (int i = 1; i < uploadBytesPerSecMovingAverageWindowSize; i++) {
            tracker.updateUploadBytesPerSecMovingAverage(i);
            sum += i;
            assertFalse(tracker.isUploadBytesPerSecMovingAverageReady());
            assertEquals((double) sum / i, tracker.getUploadBytesPerSecMovingAverage(), 0.0d);
        }

        tracker.updateUploadBytesPerSecMovingAverage(uploadBytesPerSecMovingAverageWindowSize);
        sum += uploadBytesPerSecMovingAverageWindowSize;
        assertTrue(tracker.isUploadBytesPerSecMovingAverageReady());
        assertEquals((double) sum / uploadBytesPerSecMovingAverageWindowSize, tracker.getUploadBytesPerSecMovingAverage(), 0.0d);

        tracker.updateUploadBytesPerSecMovingAverage(100);
        sum = sum + 100 - 1;
        assertEquals((double) sum / uploadBytesPerSecMovingAverageWindowSize, tracker.getUploadBytesPerSecMovingAverage(), 0.0d);
    }

    public void testUpdateUploadTimeMovingAverage() {
        int uploadTimeMovingAverageWindowSize = 20;
        tracker = new RemoteTranslogTracker(
            shardId,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            uploadTimeMovingAverageWindowSize
        );
        assertFalse(tracker.isUploadTimeMovingAverageReady());

        long sum = 0;
        for (int i = 1; i < uploadTimeMovingAverageWindowSize; i++) {
            tracker.updateUploadTimeMovingAverage(i);
            sum += i;
            assertFalse(tracker.isUploadTimeMovingAverageReady());
            assertEquals((double) sum / i, tracker.getUploadTimeMovingAverage(), 0.0d);
        }

        tracker.updateUploadTimeMovingAverage(uploadTimeMovingAverageWindowSize);
        sum += uploadTimeMovingAverageWindowSize;
        assertTrue(tracker.isUploadTimeMovingAverageReady());
        assertEquals((double) sum / uploadTimeMovingAverageWindowSize, tracker.getUploadTimeMovingAverage(), 0.0d);

        tracker.updateUploadTimeMovingAverage(100);
        sum = sum + 100 - 1;
        assertEquals((double) sum / uploadTimeMovingAverageWindowSize, tracker.getUploadTimeMovingAverage(), 0.0d);
    }

    public void testStatsObjectCreation() {
        populateDummyStats();
        RemoteTranslogTracker.Stats actualStats = tracker.stats();
        assertTrue(tracker.hasSameStatsAs(actualStats));
    }

    public void testStatsObjectCreationViaStream() throws IOException {
        populateDummyStats();
        RemoteTranslogTracker.Stats expectedStats = tracker.stats();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            expectedStats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                RemoteTranslogTracker.Stats deserializedStats = new RemoteTranslogTracker.Stats(in);
                assertTrue(tracker.hasSameStatsAs(deserializedStats));
            }
        }
    }

    private void populateUploadsStarted() {
        assertEquals(0L, tracker.getTotalUploadsStarted());
        tracker.incrementUploadsStarted();
        assertEquals(1L, tracker.getTotalUploadsStarted());
        tracker.incrementUploadsStarted();
        assertEquals(2L, tracker.getTotalUploadsStarted());
    }

    private void populateUploadsFailed() {
        assertEquals(0L, tracker.getTotalUploadsFailed());
        tracker.incrementUploadsFailed();
        assertEquals(1L, tracker.getTotalUploadsFailed());
        tracker.incrementUploadsFailed();
        assertEquals(2L, tracker.getTotalUploadsFailed());
    }

    private void populateUploadsSucceeded() {
        assertEquals(0L, tracker.getTotalUploadsSucceeded());
        tracker.incrementUploadsSucceeded();
        assertEquals(1L, tracker.getTotalUploadsSucceeded());
        tracker.incrementUploadsSucceeded();
        assertEquals(2L, tracker.getTotalUploadsSucceeded());
    }

    private void populateUploadBytesStarted() {
        assertEquals(0L, tracker.getUploadBytesStarted());
        long count1 = randomIntBetween(500, 1000);
        tracker.addUploadBytesStarted(count1);
        assertEquals(count1, tracker.getUploadBytesStarted());
        long count2 = randomIntBetween(500, 1000);
        tracker.addUploadBytesStarted(count2);
        assertEquals(count1 + count2, tracker.getUploadBytesStarted());
    }

    private void populateDummyStats() {
        tracker.setLastUploadTimestamp(System.nanoTime() / 1_000_000L + randomIntBetween(10, 100));
        tracker.incrementUploadsStarted();
        tracker.incrementUploadsStarted();
        tracker.incrementUploadsFailed();
        tracker.incrementUploadsSucceeded();
        int startedBytes = randomIntBetween(10, 100);
        int failedBytes = randomIntBetween(1, startedBytes / 2);
        int succeededBytes = randomIntBetween(1, startedBytes / 2);
        tracker.addUploadBytesStarted(startedBytes);
        tracker.addUploadBytesFailed(failedBytes);
        tracker.addUploadBytesSucceeded(succeededBytes);
        tracker.addUploadTimeInMillis(randomIntBetween(10, 100));
    }
}
