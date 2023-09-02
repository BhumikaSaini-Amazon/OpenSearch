/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer.listener;

import org.opensearch.index.translog.transfer.FileSnapshot;
import org.opensearch.index.translog.transfer.TransferSnapshot;

import java.io.IOException;
import java.util.Set;

/**
 * The listener to be invoked on the completion or failure of a {@link TransferSnapshot}
 *
 * @opensearch.internal
 */
public interface TranslogTransferListener {
    /**
     * Invoked before the transfer of {@link TransferSnapshot}
     * @param toUpload the set of files to upload
     */
    void beforeUpload(Set<FileSnapshot.TransferFileSnapshot> toUpload) throws IOException;

    /**
     * Invoked when the transfer of {@link TransferSnapshot} succeeds
     * @param transferSnapshot the transfer snapshot
     * @throws IOException the exception during the transfer of data
     */
    void onUploadComplete(TransferSnapshot transferSnapshot) throws IOException;

    /**
     * Invoked when the transfer of {@link TransferSnapshot} fails
     * @param transferSnapshot the transfer snapshot
     * @param ex the exception while processing the {@link TransferSnapshot}
     * @throws IOException the exception during the transfer of data
     */
    void onUploadFailed(TransferSnapshot transferSnapshot, Exception ex) throws IOException;
}
