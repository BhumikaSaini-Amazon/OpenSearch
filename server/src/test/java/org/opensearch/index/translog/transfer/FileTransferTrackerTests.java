/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

public class FileTransferTrackerTests extends OpenSearchTestCase {

    protected final ShardId shardId = new ShardId("index", "_na_", 1);
    FileTransferTracker fileTransferTracker;

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    public void testOnSuccess() throws IOException {
        RemoteTranslogTransferTracker remoteTranslogTransferTracker = new RemoteTranslogTransferTracker(shardId, 20);
        fileTransferTracker = new FileTransferTracker(shardId, remoteTranslogTransferTracker);
        Path testFile = createTempFile();
        int testFileSize = 128;
        Files.write(testFile, randomByteArrayOfLength(testFileSize), StandardOpenOption.APPEND);
        try (
            FileSnapshot.TransferFileSnapshot transferFileSnapshot = new FileSnapshot.TransferFileSnapshot(
                testFile,
                randomNonNegativeLong(),
                null
            )
        ) {
            remoteTranslogTransferTracker.addUploadBytesStarted(testFileSize);
            fileTransferTracker.onSuccess(transferFileSnapshot);
            // idempotent
            remoteTranslogTransferTracker.addUploadBytesStarted(testFileSize);
            fileTransferTracker.onSuccess(transferFileSnapshot);
            assertEquals(fileTransferTracker.allUploaded().size(), 1);

            // assert translog stats
            assertEquals(testFileSize * 2, remoteTranslogTransferTracker.getUploadBytesSucceeded());
            assertEquals(0, remoteTranslogTransferTracker.getUploadBytesFailed());
            assertTrue(remoteTranslogTransferTracker.getTotalUploadTimeInMillis() > 0);

            try {
                fileTransferTracker.onFailure(transferFileSnapshot, new IOException("random exception"));
                fail("failure after succcess invalid");
            } catch (IllegalStateException ex) {
                // all good
            }
        }
    }

    public void testOnFailure() throws IOException {
        RemoteTranslogTransferTracker remoteTranslogTransferTracker = new RemoteTranslogTransferTracker(shardId, 20);
        fileTransferTracker = new FileTransferTracker(shardId, remoteTranslogTransferTracker);
        Path testFile = createTempFile();
        Path testFile2 = createTempFile();
        int testFileSize = 128;
        Files.write(testFile, randomByteArrayOfLength(testFileSize), StandardOpenOption.APPEND);
        try (
            FileSnapshot.TransferFileSnapshot transferFileSnapshot = new FileSnapshot.TransferFileSnapshot(
                testFile,
                randomNonNegativeLong(),
                null
            );
            FileSnapshot.TransferFileSnapshot transferFileSnapshot2 = new FileSnapshot.TransferFileSnapshot(
                testFile2,
                randomNonNegativeLong(),
                null
            )
        ) {
            remoteTranslogTransferTracker.addUploadBytesStarted(testFileSize);
            fileTransferTracker.onFailure(transferFileSnapshot, new IOException("random exception"));
            fileTransferTracker.onSuccess(transferFileSnapshot2);
            assertEquals(fileTransferTracker.allUploaded().size(), 1);

            remoteTranslogTransferTracker.addUploadBytesStarted(testFileSize);
            fileTransferTracker.onSuccess(transferFileSnapshot);
            assertEquals(fileTransferTracker.allUploaded().size(), 2);

            // assert translog stats
            assertEquals(testFileSize, remoteTranslogTransferTracker.getUploadBytesSucceeded());
            assertEquals(testFileSize, remoteTranslogTransferTracker.getUploadBytesFailed());
            assertTrue(remoteTranslogTransferTracker.getTotalUploadTimeInMillis() > 0);
        }
    }

    public void testUploaded() throws IOException {
        RemoteTranslogTransferTracker remoteTranslogTransferTracker = new RemoteTranslogTransferTracker(shardId, 20);
        fileTransferTracker = new FileTransferTracker(shardId, remoteTranslogTransferTracker);
        Path testFile = createTempFile();
        int testFileSize = 128;
        Files.write(testFile, randomByteArrayOfLength(testFileSize), StandardOpenOption.APPEND);
        try (
            FileSnapshot.TransferFileSnapshot transferFileSnapshot = new FileSnapshot.TransferFileSnapshot(
                testFile,
                randomNonNegativeLong(),
                null
            );

        ) {
            remoteTranslogTransferTracker.addUploadBytesStarted(testFileSize);
            fileTransferTracker.onSuccess(transferFileSnapshot);
            String fileName = String.valueOf(testFile.getFileName());
            assertTrue(fileTransferTracker.uploaded(fileName));
            assertFalse(fileTransferTracker.uploaded("random-name"));

            // assert translog stats
            assertEquals(testFileSize, remoteTranslogTransferTracker.getUploadBytesSucceeded());
            assertEquals(0, remoteTranslogTransferTracker.getUploadBytesFailed());
            assertTrue(remoteTranslogTransferTracker.getTotalUploadTimeInMillis() > 0);

            fileTransferTracker.delete(List.of(fileName));
            assertFalse(fileTransferTracker.uploaded(fileName));
        }
    }

}
