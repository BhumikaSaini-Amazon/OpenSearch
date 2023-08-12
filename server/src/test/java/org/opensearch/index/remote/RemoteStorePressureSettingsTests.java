/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class RemoteStorePressureSettingsTests extends OpenSearchTestCase {

    private ClusterService clusterService;

    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("remote_refresh_segment_pressure_settings_test");
        clusterService = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool
        );
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testGetDefaultSettings() {
        RemoteStorePressureSettings pressureSettings = new RemoteStorePressureSettings(
            clusterService,
            Settings.EMPTY,
            mock(RemoteStorePressureService.class)
        );

        // Check remote refresh segment pressure enabled is false
        assertFalse(pressureSettings.isRemoteRefreshSegmentPressureEnabled());

        // Check bytes lag variance threshold default value
        assertEquals(10.0, pressureSettings.getBytesLagVarianceFactor(), 0.0d);

        // Check time lag variance threshold default value
        assertEquals(10.0, pressureSettings.getUploadTimeLagVarianceFactor(), 0.0d);

        // Check minimum consecutive failures limit default value
        assertEquals(5, pressureSettings.getMinConsecutiveFailuresLimit());

        // Check upload bytes moving average window size default value
        assertEquals(20, pressureSettings.getUploadBytesMovingAverageWindowSize());

        // Check upload bytes per sec moving average window size default value
        assertEquals(20, pressureSettings.getUploadBytesPerSecMovingAverageWindowSize());

        // Check upload time moving average window size default value
        assertEquals(20, pressureSettings.getUploadTimeMovingAverageWindowSize());

        // Check download bytes moving average window size default value
        assertEquals(20, pressureSettings.getDownloadBytesMovingAverageWindowSize());

        // Check download bytes per sec moving average window size default value
        assertEquals(20, pressureSettings.getDownloadBytesPerSecMovingAverageWindowSize());

        // Check download time moving average window size default value
        assertEquals(20, pressureSettings.getDownloadTimeMovingAverageWindowSize());
    }

    public void testGetConfiguredSettings() {
        Settings settings = Settings.builder()
            .put(RemoteStorePressureSettings.REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED.getKey(), true)
            .put(RemoteStorePressureSettings.BYTES_LAG_VARIANCE_FACTOR.getKey(), 50.0)
            .put(RemoteStorePressureSettings.UPLOAD_TIME_LAG_VARIANCE_FACTOR.getKey(), 60.0)
            .put(RemoteStorePressureSettings.MIN_CONSECUTIVE_FAILURES_LIMIT.getKey(), 121)
            .put(RemoteStorePressureSettings.UPLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 102)
            .put(RemoteStorePressureSettings.UPLOAD_BYTES_PER_SEC_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 103)
            .put(RemoteStorePressureSettings.UPLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 104)
            .put(RemoteStorePressureSettings.DOWNLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 111)
            .put(RemoteStorePressureSettings.DOWNLOAD_BYTES_PER_SEC_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 222)
            .put(RemoteStorePressureSettings.DOWNLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 333)
            .build();
        RemoteStorePressureSettings pressureSettings = new RemoteStorePressureSettings(
            clusterService,
            settings,
            mock(RemoteStorePressureService.class)
        );

        // Check remote refresh segment pressure enabled is true
        assertTrue(pressureSettings.isRemoteRefreshSegmentPressureEnabled());

        // Check bytes lag variance threshold configured value
        assertEquals(50.0, pressureSettings.getBytesLagVarianceFactor(), 0.0d);

        // Check time lag variance threshold configured value
        assertEquals(60.0, pressureSettings.getUploadTimeLagVarianceFactor(), 0.0d);

        // Check minimum consecutive failures limit configured value
        assertEquals(121, pressureSettings.getMinConsecutiveFailuresLimit());

        // Check upload bytes moving average window size configured value
        assertEquals(102, pressureSettings.getUploadBytesMovingAverageWindowSize());

        // Check upload bytes per sec moving average window size configured value
        assertEquals(103, pressureSettings.getUploadBytesPerSecMovingAverageWindowSize());

        // Check upload time moving average window size configured value
        assertEquals(104, pressureSettings.getUploadTimeMovingAverageWindowSize());

        // Check download bytes moving average window size configured value
        assertEquals(111, pressureSettings.getDownloadBytesMovingAverageWindowSize());

        // Check download bytes per sec moving average window size configured value
        assertEquals(222, pressureSettings.getDownloadBytesPerSecMovingAverageWindowSize());

        // Check download time moving average window size configured value
        assertEquals(333, pressureSettings.getDownloadTimeMovingAverageWindowSize());
    }

    public void testUpdateAfterGetDefaultSettings() {
        RemoteStorePressureSettings pressureSettings = new RemoteStorePressureSettings(
            clusterService,
            Settings.EMPTY,
            mock(RemoteStorePressureService.class)
        );

        Settings newSettings = Settings.builder()
            .put(RemoteStorePressureSettings.REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED.getKey(), true)
            .put(RemoteStorePressureSettings.BYTES_LAG_VARIANCE_FACTOR.getKey(), 50.0)
            .put(RemoteStorePressureSettings.UPLOAD_TIME_LAG_VARIANCE_FACTOR.getKey(), 60.0)
            .put(RemoteStorePressureSettings.MIN_CONSECUTIVE_FAILURES_LIMIT.getKey(), 121)
            .put(RemoteStorePressureSettings.UPLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 102)
            .put(RemoteStorePressureSettings.UPLOAD_BYTES_PER_SEC_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 103)
            .put(RemoteStorePressureSettings.UPLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 104)
            .put(RemoteStorePressureSettings.DOWNLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 111)
            .put(RemoteStorePressureSettings.DOWNLOAD_BYTES_PER_SEC_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 222)
            .put(RemoteStorePressureSettings.DOWNLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 333)
            .build();
        clusterService.getClusterSettings().applySettings(newSettings);

        // Check updated remote refresh segment pressure enabled is false
        assertTrue(pressureSettings.isRemoteRefreshSegmentPressureEnabled());

        // Check bytes lag variance threshold updated
        assertEquals(50.0, pressureSettings.getBytesLagVarianceFactor(), 0.0d);

        // Check time lag variance threshold updated
        assertEquals(60.0, pressureSettings.getUploadTimeLagVarianceFactor(), 0.0d);

        // Check minimum consecutive failures limit updated
        assertEquals(121, pressureSettings.getMinConsecutiveFailuresLimit());

        // Check upload bytes moving average window size updated
        assertEquals(102, pressureSettings.getUploadBytesMovingAverageWindowSize());

        // Check upload bytes per sec moving average window size updated
        assertEquals(103, pressureSettings.getUploadBytesPerSecMovingAverageWindowSize());

        // Check upload time moving average window size updated
        assertEquals(104, pressureSettings.getUploadTimeMovingAverageWindowSize());

        // Check download bytes moving average window size configured value
        assertEquals(111, pressureSettings.getDownloadBytesMovingAverageWindowSize());

        // Check download bytes per sec moving average window size configured value
        assertEquals(222, pressureSettings.getDownloadBytesPerSecMovingAverageWindowSize());

        // Check download time moving average window size configured value
        assertEquals(333, pressureSettings.getDownloadTimeMovingAverageWindowSize());
    }

    public void testUpdateAfterGetConfiguredSettings() {
        Settings settings = Settings.builder()
            .put(RemoteStorePressureSettings.REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED.getKey(), true)
            .put(RemoteStorePressureSettings.BYTES_LAG_VARIANCE_FACTOR.getKey(), 50.0)
            .put(RemoteStorePressureSettings.UPLOAD_TIME_LAG_VARIANCE_FACTOR.getKey(), 60.0)
            .put(RemoteStorePressureSettings.MIN_CONSECUTIVE_FAILURES_LIMIT.getKey(), 121)
            .put(RemoteStorePressureSettings.UPLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 102)
            .put(RemoteStorePressureSettings.UPLOAD_BYTES_PER_SEC_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 103)
            .put(RemoteStorePressureSettings.UPLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 104)
            .put(RemoteStorePressureSettings.DOWNLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 111)
            .put(RemoteStorePressureSettings.DOWNLOAD_BYTES_PER_SEC_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 222)
            .put(RemoteStorePressureSettings.DOWNLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 333)
            .build();
        RemoteStorePressureSettings pressureSettings = new RemoteStorePressureSettings(
            clusterService,
            settings,
            mock(RemoteStorePressureService.class)
        );

        Settings newSettings = Settings.builder()
            .put(RemoteStorePressureSettings.BYTES_LAG_VARIANCE_FACTOR.getKey(), 40.0)
            .put(RemoteStorePressureSettings.UPLOAD_TIME_LAG_VARIANCE_FACTOR.getKey(), 50.0)
            .put(RemoteStorePressureSettings.MIN_CONSECUTIVE_FAILURES_LIMIT.getKey(), 111)
            .put(RemoteStorePressureSettings.UPLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 112)
            .put(RemoteStorePressureSettings.UPLOAD_BYTES_PER_SEC_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 113)
            .put(RemoteStorePressureSettings.UPLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 114)
            .put(RemoteStorePressureSettings.DOWNLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 222)
            .put(RemoteStorePressureSettings.DOWNLOAD_BYTES_PER_SEC_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 444)
            .put(RemoteStorePressureSettings.DOWNLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 666)
            .build();

        clusterService.getClusterSettings().applySettings(newSettings);

        // Check updated remote refresh segment pressure enabled is true
        assertTrue(pressureSettings.isRemoteRefreshSegmentPressureEnabled());

        // Check bytes lag variance threshold updated
        assertEquals(40.0, pressureSettings.getBytesLagVarianceFactor(), 0.0d);

        // Check time lag variance threshold updated
        assertEquals(50.0, pressureSettings.getUploadTimeLagVarianceFactor(), 0.0d);

        // Check minimum consecutive failures limit updated
        assertEquals(111, pressureSettings.getMinConsecutiveFailuresLimit());

        // Check upload bytes moving average window size updated
        assertEquals(112, pressureSettings.getUploadBytesMovingAverageWindowSize());

        // Check upload bytes per sec moving average window size updated
        assertEquals(113, pressureSettings.getUploadBytesPerSecMovingAverageWindowSize());

        // Check upload time moving average window size updated
        assertEquals(114, pressureSettings.getUploadTimeMovingAverageWindowSize());

        // Check download bytes moving average window size configured value
        assertEquals(222, pressureSettings.getDownloadBytesMovingAverageWindowSize());

        // Check download bytes per sec moving average window size configured value
        assertEquals(444, pressureSettings.getDownloadBytesPerSecMovingAverageWindowSize());

        // Check download time moving average window size configured value
        assertEquals(666, pressureSettings.getDownloadTimeMovingAverageWindowSize());
    }

    public void testUpdateTriggeredInRemotePressureServiceOnUpdateSettings() {

        int toUpdateVal1 = 1121, toUpdateVal2 = 1123, toUpdateVal3 = 1125;
        int toUpdateVal4 = 1127, toUpdateVal5 = 1129, toUpdateVal6 = 1131;

        AtomicInteger updatedUploadBytesWindowSize = new AtomicInteger();
        AtomicInteger updatedUploadBytesPerSecWindowSize = new AtomicInteger();
        AtomicInteger updatedUploadTimeWindowSize = new AtomicInteger();
        AtomicInteger updatedDownloadBytesWindowSize = new AtomicInteger();
        AtomicInteger updatedDownloadBytesPerSecWindowSize = new AtomicInteger();
        AtomicInteger updatedDownloadTimeWindowSize = new AtomicInteger();

        RemoteStorePressureService pressureService = mock(RemoteStorePressureService.class);

        // Upload bytes
        doAnswer(invocation -> {
            updatedUploadBytesWindowSize.set(invocation.getArgument(0));
            return null;
        }).when(pressureService).updateUploadBytesMovingAverageWindowSize(anyInt());

        // Upload bytes per sec
        doAnswer(invocation -> {
            updatedUploadBytesPerSecWindowSize.set(invocation.getArgument(0));
            return null;
        }).when(pressureService).updateUploadBytesPerSecMovingAverageWindowSize(anyInt());

        // Upload time
        doAnswer(invocation -> {
            updatedUploadTimeWindowSize.set(invocation.getArgument(0));
            return null;
        }).when(pressureService).updateUploadTimeMsMovingAverageWindowSize(anyInt());

        // Download bytes
        doAnswer(invocation -> {
            updatedDownloadBytesWindowSize.set(invocation.getArgument(0));
            return null;
        }).when(pressureService).updateDownloadBytesMovingAverageWindowSize(anyInt());

        // Download bytes per sec
        doAnswer(invocation -> {
            updatedDownloadBytesPerSecWindowSize.set(invocation.getArgument(0));
            return null;
        }).when(pressureService).updateDownloadBytesPerSecMovingAverageWindowSize(anyInt());

        // Download time
        doAnswer(invocation -> {
            updatedDownloadTimeWindowSize.set(invocation.getArgument(0));
            return null;
        }).when(pressureService).updateDownloadTimeMsMovingAverageWindowSize(anyInt());

        RemoteStorePressureSettings pressureSettings = new RemoteStorePressureSettings(clusterService, Settings.EMPTY, pressureService);
        Settings newSettings = Settings.builder()
            .put(RemoteStorePressureSettings.UPLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE.getKey(), toUpdateVal1)
            .put(RemoteStorePressureSettings.UPLOAD_BYTES_PER_SEC_MOVING_AVERAGE_WINDOW_SIZE.getKey(), toUpdateVal2)
            .put(RemoteStorePressureSettings.UPLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE.getKey(), toUpdateVal3)
            .put(RemoteStorePressureSettings.DOWNLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE.getKey(), toUpdateVal4)
            .put(RemoteStorePressureSettings.DOWNLOAD_BYTES_PER_SEC_MOVING_AVERAGE_WINDOW_SIZE.getKey(), toUpdateVal5)
            .put(RemoteStorePressureSettings.DOWNLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE.getKey(), toUpdateVal6)
            .build();
        clusterService.getClusterSettings().applySettings(newSettings);

        // Assertions
        assertEquals(toUpdateVal1, pressureSettings.getUploadBytesMovingAverageWindowSize());
        assertEquals(toUpdateVal1, updatedUploadBytesWindowSize.get());
        assertEquals(toUpdateVal2, pressureSettings.getUploadBytesPerSecMovingAverageWindowSize());
        assertEquals(toUpdateVal2, updatedUploadBytesPerSecWindowSize.get());
        assertEquals(toUpdateVal3, pressureSettings.getUploadTimeMovingAverageWindowSize());
        assertEquals(toUpdateVal3, updatedUploadTimeWindowSize.get());
        assertEquals(toUpdateVal4, pressureSettings.getDownloadBytesMovingAverageWindowSize());
        assertEquals(toUpdateVal4, updatedDownloadBytesWindowSize.get());
        assertEquals(toUpdateVal5, pressureSettings.getDownloadBytesPerSecMovingAverageWindowSize());
        assertEquals(toUpdateVal5, updatedDownloadBytesPerSecWindowSize.get());
        assertEquals(toUpdateVal6, pressureSettings.getDownloadTimeMovingAverageWindowSize());
        assertEquals(toUpdateVal6, updatedDownloadTimeWindowSize.get());
    }
}
