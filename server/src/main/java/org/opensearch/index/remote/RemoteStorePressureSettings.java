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
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

/**
 * Settings related to back pressure on account of segments upload failures / lags.
 *
 * @opensearch.internal
 */
public class RemoteStorePressureSettings {

    private static class Defaults {
        private static final double BYTES_LAG_VARIANCE_FACTOR = 10.0;
        private static final double UPLOAD_TIME_LAG_VARIANCE_FACTOR = 10.0;
        private static final double VARIANCE_FACTOR_MIN_VALUE = 1.0;
        private static final int MIN_CONSECUTIVE_FAILURES_LIMIT = 5;
        private static final int MIN_CONSECUTIVE_FAILURES_LIMIT_MIN_VALUE = 1;
        private static final int UPLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE = 20;
        private static final int UPLOAD_BYTES_PER_SEC_MOVING_AVERAGE_WINDOW_SIZE = 20;
        private static final int UPLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE = 20;
        private static final int DOWNLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE = 20;
        private static final int DOWNLOAD_BYTES_PER_SEC_MOVING_AVERAGE_WINDOW_SIZE = 20;
        private static final int DOWNLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE = 20;
        private static final int MOVING_AVERAGE_WINDOW_SIZE_MIN_VALUE = 5;
    }

    public static final Setting<Boolean> REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED = Setting.boolSetting(
        "remote_store.segment.pressure.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Double> BYTES_LAG_VARIANCE_FACTOR = Setting.doubleSetting(
        "remote_store.segment.pressure.bytes_lag.variance_factor",
        Defaults.BYTES_LAG_VARIANCE_FACTOR,
        Defaults.VARIANCE_FACTOR_MIN_VALUE,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Double> UPLOAD_TIME_LAG_VARIANCE_FACTOR = Setting.doubleSetting(
        "remote_store.segment.pressure.time_lag.variance_factor",
        Defaults.UPLOAD_TIME_LAG_VARIANCE_FACTOR,
        Defaults.VARIANCE_FACTOR_MIN_VALUE,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> MIN_CONSECUTIVE_FAILURES_LIMIT = Setting.intSetting(
        "remote_store.segment.pressure.consecutive_failures.limit",
        Defaults.MIN_CONSECUTIVE_FAILURES_LIMIT,
        Defaults.MIN_CONSECUTIVE_FAILURES_LIMIT_MIN_VALUE,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> UPLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE = Setting.intSetting(
        "remote_store.pressure.upload_bytes_moving_average_window_size",
        Defaults.UPLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE,
        Defaults.MOVING_AVERAGE_WINDOW_SIZE_MIN_VALUE,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> UPLOAD_BYTES_PER_SEC_MOVING_AVERAGE_WINDOW_SIZE = Setting.intSetting(
        "remote_store.pressure.upload_bytes_per_sec_moving_average_window_size",
        Defaults.UPLOAD_BYTES_PER_SEC_MOVING_AVERAGE_WINDOW_SIZE,
        Defaults.MOVING_AVERAGE_WINDOW_SIZE_MIN_VALUE,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> UPLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE = Setting.intSetting(
        "remote_store.pressure.upload_time_moving_average_window_size",
        Defaults.UPLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE,
        Defaults.MOVING_AVERAGE_WINDOW_SIZE_MIN_VALUE,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> DOWNLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE = Setting.intSetting(
        "remote_store.pressure.download_bytes_moving_average_window_size",
        Defaults.DOWNLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE,
        Defaults.MOVING_AVERAGE_WINDOW_SIZE_MIN_VALUE,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> DOWNLOAD_BYTES_PER_SEC_MOVING_AVERAGE_WINDOW_SIZE = Setting.intSetting(
        "remote_store.pressure.download_bytes_per_sec_moving_average_window_size",
        Defaults.DOWNLOAD_BYTES_PER_SEC_MOVING_AVERAGE_WINDOW_SIZE,
        Defaults.MOVING_AVERAGE_WINDOW_SIZE_MIN_VALUE,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> DOWNLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE = Setting.intSetting(
        "remote_store.pressure.download_time_moving_average_window_size",
        Defaults.DOWNLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE,
        Defaults.MOVING_AVERAGE_WINDOW_SIZE_MIN_VALUE,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile boolean remoteRefreshSegmentPressureEnabled;

    private volatile long minRefreshSeqNoLagLimit;

    private volatile double bytesLagVarianceFactor;

    private volatile double uploadTimeLagVarianceFactor;

    private volatile int minConsecutiveFailuresLimit;

    private volatile int uploadBytesMovingAverageWindowSize;

    private volatile int uploadBytesPerSecMovingAverageWindowSize;

    private volatile int uploadTimeMovingAverageWindowSize;

    private volatile int downloadBytesMovingAverageWindowSize;

    private volatile int downloadBytesPerSecMovingAverageWindowSize;

    private volatile int downloadTimeMovingAverageWindowSize;

    public RemoteStorePressureSettings(
        ClusterService clusterService,
        Settings settings,
        RemoteStorePressureService remoteStorePressureService
    ) {
        ClusterSettings clusterSettings = clusterService.getClusterSettings();

        this.remoteRefreshSegmentPressureEnabled = REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED.get(settings);
        clusterSettings.addSettingsUpdateConsumer(REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED, this::setRemoteRefreshSegmentPressureEnabled);

        this.bytesLagVarianceFactor = BYTES_LAG_VARIANCE_FACTOR.get(settings);
        clusterSettings.addSettingsUpdateConsumer(BYTES_LAG_VARIANCE_FACTOR, this::setBytesLagVarianceFactor);

        this.uploadTimeLagVarianceFactor = UPLOAD_TIME_LAG_VARIANCE_FACTOR.get(settings);
        clusterSettings.addSettingsUpdateConsumer(UPLOAD_TIME_LAG_VARIANCE_FACTOR, this::setUploadTimeLagVarianceFactor);

        this.minConsecutiveFailuresLimit = MIN_CONSECUTIVE_FAILURES_LIMIT.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MIN_CONSECUTIVE_FAILURES_LIMIT, this::setMinConsecutiveFailuresLimit);

        this.uploadBytesMovingAverageWindowSize = UPLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            UPLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE,
            remoteStorePressureService::updateUploadBytesMovingAverageWindowSize
        );
        clusterSettings.addSettingsUpdateConsumer(UPLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE, this::setUploadBytesMovingAverageWindowSize);

        this.uploadBytesPerSecMovingAverageWindowSize = UPLOAD_BYTES_PER_SEC_MOVING_AVERAGE_WINDOW_SIZE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            UPLOAD_BYTES_PER_SEC_MOVING_AVERAGE_WINDOW_SIZE,
            remoteStorePressureService::updateUploadBytesPerSecMovingAverageWindowSize
        );
        clusterSettings.addSettingsUpdateConsumer(
            UPLOAD_BYTES_PER_SEC_MOVING_AVERAGE_WINDOW_SIZE,
            this::setUploadBytesPerSecMovingAverageWindowSize
        );

        this.uploadTimeMovingAverageWindowSize = UPLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            UPLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE,
            remoteStorePressureService::updateUploadTimeMsMovingAverageWindowSize
        );
        clusterSettings.addSettingsUpdateConsumer(UPLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE, this::setUploadTimeMovingAverageWindowSize);

        this.downloadBytesMovingAverageWindowSize = DOWNLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            DOWNLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE,
            remoteStorePressureService::updateDownloadBytesMovingAverageWindowSize
        );
        clusterSettings.addSettingsUpdateConsumer(DOWNLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE, this::setDownloadBytesMovingAverageWindowSize);

        this.downloadBytesPerSecMovingAverageWindowSize = DOWNLOAD_BYTES_PER_SEC_MOVING_AVERAGE_WINDOW_SIZE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            DOWNLOAD_BYTES_PER_SEC_MOVING_AVERAGE_WINDOW_SIZE,
            remoteStorePressureService::updateDownloadBytesPerSecMovingAverageWindowSize
        );
        clusterSettings.addSettingsUpdateConsumer(
            DOWNLOAD_BYTES_PER_SEC_MOVING_AVERAGE_WINDOW_SIZE,
            this::setDownloadBytesPerSecMovingAverageWindowSize
        );

        this.downloadTimeMovingAverageWindowSize = DOWNLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            DOWNLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE,
            remoteStorePressureService::updateDownloadTimeMsMovingAverageWindowSize
        );
        clusterSettings.addSettingsUpdateConsumer(DOWNLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE, this::setDownloadTimeMovingAverageWindowSize);
    }

    public boolean isRemoteRefreshSegmentPressureEnabled() {
        return remoteRefreshSegmentPressureEnabled;
    }

    public void setRemoteRefreshSegmentPressureEnabled(boolean remoteRefreshSegmentPressureEnabled) {
        this.remoteRefreshSegmentPressureEnabled = remoteRefreshSegmentPressureEnabled;
    }

    public long getMinRefreshSeqNoLagLimit() {
        return minRefreshSeqNoLagLimit;
    }

    public void setMinRefreshSeqNoLagLimit(long minRefreshSeqNoLagLimit) {
        this.minRefreshSeqNoLagLimit = minRefreshSeqNoLagLimit;
    }

    public double getBytesLagVarianceFactor() {
        return bytesLagVarianceFactor;
    }

    public void setBytesLagVarianceFactor(double bytesLagVarianceFactor) {
        this.bytesLagVarianceFactor = bytesLagVarianceFactor;
    }

    public double getUploadTimeLagVarianceFactor() {
        return uploadTimeLagVarianceFactor;
    }

    public void setUploadTimeLagVarianceFactor(double uploadTimeLagVarianceFactor) {
        this.uploadTimeLagVarianceFactor = uploadTimeLagVarianceFactor;
    }

    public int getMinConsecutiveFailuresLimit() {
        return minConsecutiveFailuresLimit;
    }

    public void setMinConsecutiveFailuresLimit(int minConsecutiveFailuresLimit) {
        this.minConsecutiveFailuresLimit = minConsecutiveFailuresLimit;
    }

    public int getUploadBytesMovingAverageWindowSize() {
        return uploadBytesMovingAverageWindowSize;
    }

    public void setUploadBytesMovingAverageWindowSize(int uploadBytesMovingAverageWindowSize) {
        this.uploadBytesMovingAverageWindowSize = uploadBytesMovingAverageWindowSize;
    }

    public int getUploadBytesPerSecMovingAverageWindowSize() {
        return uploadBytesPerSecMovingAverageWindowSize;
    }

    public void setUploadBytesPerSecMovingAverageWindowSize(int uploadBytesPerSecMovingAverageWindowSize) {
        this.uploadBytesPerSecMovingAverageWindowSize = uploadBytesPerSecMovingAverageWindowSize;
    }

    public int getUploadTimeMovingAverageWindowSize() {
        return uploadTimeMovingAverageWindowSize;
    }

    public void setUploadTimeMovingAverageWindowSize(int uploadTimeMovingAverageWindowSize) {
        this.uploadTimeMovingAverageWindowSize = uploadTimeMovingAverageWindowSize;
    }

    public int getDownloadBytesMovingAverageWindowSize() {
        return downloadBytesMovingAverageWindowSize;
    }

    public void setDownloadBytesMovingAverageWindowSize(int downloadBytesMovingAverageWindowSize) {
        this.downloadBytesMovingAverageWindowSize = downloadBytesMovingAverageWindowSize;
    }

    public int getDownloadBytesPerSecMovingAverageWindowSize() {
        return downloadBytesPerSecMovingAverageWindowSize;
    }

    public void setDownloadBytesPerSecMovingAverageWindowSize(int downloadBytesPerSecMovingAverageWindowSize) {
        this.downloadBytesPerSecMovingAverageWindowSize = downloadBytesPerSecMovingAverageWindowSize;
    }

    public int getDownloadTimeMovingAverageWindowSize() {
        return downloadTimeMovingAverageWindowSize;
    }

    public void setDownloadTimeMovingAverageWindowSize(int downloadTimeMovingAverageWindowSize) {
        this.downloadTimeMovingAverageWindowSize = downloadTimeMovingAverageWindowSize;
    }
}
