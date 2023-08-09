/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.apache.logging.log4j.Logger;
import org.opensearch.common.SetOnce;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.util.FileSystemUtils;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.remote.RemoteTranslogTracker;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.index.translog.transfer.FileTransferTracker;
import org.opensearch.index.translog.transfer.TransferSnapshot;
import org.opensearch.index.translog.transfer.TranslogCheckpointTransferSnapshot;
import org.opensearch.index.translog.transfer.TranslogTransferManager;
import org.opensearch.index.translog.transfer.TranslogTransferMetadata;
import org.opensearch.index.translog.transfer.FileSnapshot;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

/**
 * A Translog implementation which syncs local FS with a remote store
 * The current impl uploads translog , ckp and metadata to remote store
 * for every sync, post syncing to disk. Post that, a new generation is
 * created.
 *
 * @opensearch.internal
 */
public class RemoteFsTranslog extends Translog {

    private final Logger logger;
    private final BlobStoreRepository blobStoreRepository;
    private final TranslogTransferManager translogTransferManager;
    private final FileTransferTracker fileTransferTracker;
    private final BooleanSupplier primaryModeSupplier;
    private final RemoteTranslogTracker remoteTranslogTracker;
    private volatile long maxRemoteTranslogGenerationUploaded;

    private volatile long minSeqNoToKeep;

    // min generation referred by last uploaded translog
    private volatile long minRemoteGenReferenced;

    // clean up translog folder uploaded by previous primaries once
    private final SetOnce<Boolean> olderPrimaryCleaned = new SetOnce<>();

    private static final int REMOTE_DELETION_PERMITS = 2;
    public static final String TRANSLOG = "translog";

    // Semaphore used to allow only single remote generation to happen at a time
    private final Semaphore remoteGenerationDeletionPermits = new Semaphore(REMOTE_DELETION_PERMITS);

    public RemoteFsTranslog(
        TranslogConfig config,
        String translogUUID,
        TranslogDeletionPolicy deletionPolicy,
        LongSupplier globalCheckpointSupplier,
        LongSupplier primaryTermSupplier,
        LongConsumer persistedSequenceNumberConsumer,
        BlobStoreRepository blobStoreRepository,
        ThreadPool threadPool,
        BooleanSupplier primaryModeSupplier,
        RemoteTranslogTracker remoteTranslogTracker
    ) throws IOException {
        super(config, translogUUID, deletionPolicy, globalCheckpointSupplier, primaryTermSupplier, persistedSequenceNumberConsumer);
        logger = Loggers.getLogger(getClass(), shardId);
        this.blobStoreRepository = blobStoreRepository;
        this.primaryModeSupplier = primaryModeSupplier;
        fileTransferTracker = new FileTransferTracker(shardId);
        this.translogTransferManager = buildTranslogTransferManager(blobStoreRepository, threadPool, shardId, fileTransferTracker);
        this.remoteTranslogTracker = remoteTranslogTracker;
        try {
            download(translogTransferManager, location, logger);
            Checkpoint checkpoint = readCheckpoint(location);
            this.readers.addAll(recoverFromFiles(checkpoint));
            if (readers.isEmpty()) {
                String errorMsg = String.format(Locale.ROOT, "%s at least one reader must be recovered", shardId);
                logger.error(errorMsg);
                throw new IllegalStateException(errorMsg);
            }
            boolean success = false;
            current = null;
            try {
                current = createWriter(
                    checkpoint.generation + 1,
                    getMinFileGeneration(),
                    checkpoint.globalCheckpoint,
                    persistedSequenceNumberConsumer
                );
                success = true;
            } finally {
                // we have to close all the recovered ones otherwise we leak file handles here
                // for instance if we have a lot of tlog and we can't create the writer we keep
                // on holding
                // on to all the uncommitted tlog files if we don't close
                if (success == false) {
                    IOUtils.closeWhileHandlingException(readers);
                }
            }
        } catch (Exception e) {
            // close the opened translog files if we fail to create a new translog...
            IOUtils.closeWhileHandlingException(current);
            IOUtils.closeWhileHandlingException(readers);
            throw e;
        }
    }

    public static void download(Repository repository, ShardId shardId, ThreadPool threadPool, Path location, Logger logger)
        throws IOException {
        assert repository instanceof BlobStoreRepository : String.format(
            Locale.ROOT,
            "%s repository should be instance of BlobStoreRepository",
            shardId
        );
        BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
        FileTransferTracker fileTransferTracker = new FileTransferTracker(shardId);
        TranslogTransferManager translogTransferManager = buildTranslogTransferManager(
            blobStoreRepository,
            threadPool,
            shardId,
            fileTransferTracker
        );
        RemoteFsTranslog.download(translogTransferManager, location, logger);
    }

    public static void download(TranslogTransferManager translogTransferManager, Path location, Logger logger) throws IOException {
        logger.trace("Downloading translog files from remote");
        TranslogTransferMetadata translogMetadata = translogTransferManager.readMetadata();
        if (translogMetadata != null) {
            if (Files.notExists(location)) {
                Files.createDirectories(location);
            }
            // Delete translog files on local before downloading from remote
            for (Path file : FileSystemUtils.files(location)) {
                Files.delete(file);
            }
            Map<String, String> generationToPrimaryTermMapper = translogMetadata.getGenerationToPrimaryTermMapper();
            for (long i = translogMetadata.getGeneration(); i >= translogMetadata.getMinTranslogGeneration(); i--) {
                String generation = Long.toString(i);
                translogTransferManager.downloadTranslog(generationToPrimaryTermMapper.get(generation), generation, location);
            }
            // We copy the latest generation .ckp file to translog.ckp so that flows that depend on
            // existence of translog.ckp file work in the same way
            Files.copy(
                location.resolve(Translog.getCommitCheckpointFileName(translogMetadata.getGeneration())),
                location.resolve(Translog.CHECKPOINT_FILE_NAME)
            );
        }
        logger.trace("Downloaded translog files from remote");
    }

    public static TranslogTransferManager buildTranslogTransferManager(
        BlobStoreRepository blobStoreRepository,
        ThreadPool threadPool,
        ShardId shardId,
        FileTransferTracker fileTransferTracker
    ) {
        return new TranslogTransferManager(
            shardId,
            new BlobStoreTransferService(blobStoreRepository.blobStore(), threadPool),
            blobStoreRepository.basePath().add(shardId.getIndex().getUUID()).add(String.valueOf(shardId.id())).add(TRANSLOG),
            fileTransferTracker
        );
    }

    @Override
    public boolean ensureSynced(Location location) throws IOException {
        try (ReleasableLock ignored = writeLock.acquire()) {
            assert location.generation <= current.getGeneration();
            if (location.generation == current.getGeneration()) {
                ensureOpen();
                return prepareAndUpload(primaryTermSupplier.getAsLong(), location.generation);
            }
        } catch (final Exception ex) {
            closeOnTragicEvent(ex);
            throw ex;
        }
        return false;
    }

    @Override
    public void rollGeneration() throws IOException {
        syncBeforeRollGeneration();
        if (current.totalOperations() == 0 && primaryTermSupplier.getAsLong() == current.getPrimaryTerm()) {
            return;
        }
        prepareAndUpload(primaryTermSupplier.getAsLong(), null);
    }

    private boolean prepareAndUpload(Long primaryTerm, Long generation) throws IOException {
        try (Releasable ignored = writeLock.acquire()) {
            if (generation == null || generation == current.getGeneration()) {
                try {
                    final TranslogReader reader = current.closeIntoReader();
                    readers.add(reader);
                    copyCheckpointTo(location.resolve(getCommitCheckpointFileName(current.getGeneration())));
                    if (closed.get() == false) {
                        logger.trace("Creating new writer for gen: [{}]", current.getGeneration() + 1);
                        current = createWriter(current.getGeneration() + 1);
                    }
                } catch (final Exception e) {
                    tragedy.setTragicException(e);
                    closeOnTragicEvent(e);
                    throw e;
                }
            } else if (generation < current.getGeneration()) {
                return false;
            }

            // Do we need remote writes in sync fashion ?
            // If we don't , we should swallow FileAlreadyExistsException while writing to remote store
            // and also verify for same during primary-primary relocation
            // Writing remote in sync fashion doesn't hurt as global ckp update
            // is not updated in remote translog except in primary to primary recovery.
            if (generation == null) {
                if (closed.get() == false) {
                    return upload(primaryTerm, current.getGeneration() - 1);
                } else {
                    return upload(primaryTerm, current.getGeneration());
                }
            } else {
                return upload(primaryTerm, generation);
            }
        }
    }

    private boolean upload(Long primaryTerm, Long generation) throws IOException {
        // During primary relocation (primary-primary peer recovery), both the old and the new primary have engine
        // created with the RemoteFsTranslog. Both primaries are equipped to upload the translogs. The primary mode check
        // below ensures that the real primary only is uploading. Before the primary mode is set as true for the new
        // primary, the engine is reset to InternalEngine which also initialises the RemoteFsTranslog which in turns
        // downloads all the translogs from remote store and does a flush before the relocation finishes.
        if (primaryModeSupplier.getAsBoolean() == false) {
            logger.trace("skipped uploading translog for {} {}", primaryTerm, generation);
            // NO-OP
            return true;
        }
        logger.trace("uploading translog for {} {}", primaryTerm, generation);
        try (
            TranslogCheckpointTransferSnapshot transferSnapshotProvider = new TranslogCheckpointTransferSnapshot.Builder(
                primaryTerm,
                generation,
                location,
                readers,
                Translog::getCommitCheckpointFileName
            ).build()
        ) {
            Releasable transferReleasable = Releasables.wrap(deletionPolicy.acquireTranslogGen(getMinFileGeneration()));
            return translogTransferManager.transferSnapshot(
                transferSnapshotProvider,
                new RemoteFsTranslogTransferListener(transferReleasable, generation, primaryTerm)
            );
        }

    }

    // Visible for testing
    public Set<String> allUploaded() {
        return fileTransferTracker.allUploaded();
    }

    private boolean syncToDisk() throws IOException {
        try (ReleasableLock lock = readLock.acquire()) {
            return current.sync();
        } catch (final Exception ex) {
            closeOnTragicEvent(ex);
            throw ex;
        }
    }

    @Override
    public void sync() throws IOException {
        try {
            if (syncToDisk() || syncNeeded()) {
                prepareAndUpload(primaryTermSupplier.getAsLong(), null);
            }
        } catch (final Exception e) {
            tragedy.setTragicException(e);
            closeOnTragicEvent(e);
            throw e;
        }
    }

    /**
     * Returns <code>true</code> if an fsync and/or remote transfer is required to ensure durability of the translogs operations or it's metadata.
     */
    public boolean syncNeeded() {
        try (ReleasableLock lock = readLock.acquire()) {
            return current.syncNeeded()
                || (maxRemoteTranslogGenerationUploaded + 1 < this.currentFileGeneration() && current.totalOperations() == 0);
        }
    }

    @Override
    public TranslogStats stats() {
        // acquire lock to make the two numbers roughly consistent (no file change half way)
        try (ReleasableLock lock = readLock.acquire()) {
            long uncommittedGen = getMinGenerationForSeqNo(deletionPolicy.getLocalCheckpointOfSafeCommit() + 1).translogFileGeneration;
            return new TranslogStats(
                totalOperations(),
                sizeInBytes(),
                totalOperationsByMinGen(uncommittedGen),
                sizeInBytesByMinGen(uncommittedGen),
                earliestLastModifiedAge(),
                new RemoteTranslogStats(
                    remoteTranslogTracker.getLastUploadTimestamp(),
                    remoteTranslogTracker.getTotalUploadsStarted(),
                    remoteTranslogTracker.getTotalUploadsSucceeded(),
                    remoteTranslogTracker.getTotalUploadsFailed(),
                    remoteTranslogTracker.getUploadBytesStarted(),
                    remoteTranslogTracker.getUploadBytesSucceeded(),
                    remoteTranslogTracker.getUploadBytesFailed(),
                    remoteTranslogTracker.getTotalUploadTimeInMillis(),
                    remoteTranslogTracker.getUploadBytesMovingAverage(),
                    remoteTranslogTracker.getUploadBytesPerSecMovingAverage(),
                    remoteTranslogTracker.getUploadTimeMovingAverage()
                )
            );
        }
    }

    @Override
    public void close() throws IOException {
        assert Translog.calledFromOutsideOrViaTragedyClose() : shardId
            + "Translog.close method is called from inside Translog, but not via closeOnTragicEvent method";
        if (closed.compareAndSet(false, true)) {
            try (ReleasableLock lock = writeLock.acquire()) {
                sync();
            } finally {
                logger.debug("translog closed");
                closeFilesIfNoPendingRetentionLocks();
            }
        }
    }

    protected long getMinReferencedGen() throws IOException {
        assert readLock.isHeldByCurrentThread() || writeLock.isHeldByCurrentThread();
        long minReferencedGen = Math.min(
            deletionPolicy.minTranslogGenRequired(readers, current),
            minGenerationForSeqNo(minSeqNoToKeep, current, readers)
        );

        assert minReferencedGen >= getMinFileGeneration() : shardId
            + " deletion policy requires a minReferenceGen of ["
            + minReferencedGen
            + "] but the lowest gen available is ["
            + getMinFileGeneration()
            + "]";
        assert minReferencedGen <= currentFileGeneration() : shardId
            + " deletion policy requires a minReferenceGen of ["
            + minReferencedGen
            + "] which is higher than the current generation ["
            + currentFileGeneration()
            + "]";
        return minReferencedGen;
    }

    protected void setMinSeqNoToKeep(long seqNo) {
        if (seqNo < this.minSeqNoToKeep) {
            throw new IllegalArgumentException(
                shardId + " min seq number required can't go backwards: " + "current [" + this.minSeqNoToKeep + "] new [" + seqNo + "]"
            );
        }
        this.minSeqNoToKeep = seqNo;
    }

    @Override
    public void trimUnreferencedReaders() throws IOException {
        // clean up local translog files and updates readers
        super.trimUnreferencedReaders();

        // Since remote generation deletion is async, this ensures that only one generation deletion happens at a time.
        // Remote generations involves 2 async operations - 1) Delete translog generation files 2) Delete metadata files
        // We try to acquire 2 permits and if we can not, we return from here itself.
        if (remoteGenerationDeletionPermits.tryAcquire(REMOTE_DELETION_PERMITS) == false) {
            return;
        }

        // cleans up remote translog files not referenced in latest uploaded metadata.
        // This enables us to restore translog from the metadata in case of failover or relocation.
        Set<Long> generationsToDelete = new HashSet<>();
        for (long generation = minRemoteGenReferenced - 1; generation >= 0; generation--) {
            if (fileTransferTracker.uploaded(Translog.getFilename(generation)) == false) {
                break;
            }
            generationsToDelete.add(generation);
        }
        if (generationsToDelete.isEmpty() == false) {
            deleteRemoteGeneration(generationsToDelete);
            translogTransferManager.deleteStaleTranslogMetadataFilesAsync(remoteGenerationDeletionPermits::release);
            deleteStaleRemotePrimaryTerms();
        } else {
            remoteGenerationDeletionPermits.release(REMOTE_DELETION_PERMITS);
        }
    }

    /**
     * Deletes remote translog and metadata files asynchronously corresponding to the generations.
     * @param generations generations to be deleted.
     */
    private void deleteRemoteGeneration(Set<Long> generations) {
        translogTransferManager.deleteGenerationAsync(
            primaryTermSupplier.getAsLong(),
            generations,
            remoteGenerationDeletionPermits::release
        );
    }

    /**
     * This method must be called only after there are valid generations to delete in trimUnreferencedReaders as it ensures
     * implicitly that minimum primary term in latest translog metadata in remote store is the current primary term.
     * <br>
     * This will also delete all stale translog metadata files from remote except the latest basis the metadata file comparator.
     */
    private void deleteStaleRemotePrimaryTerms() {
        // The deletion of older translog files in remote store is on best-effort basis, there is a possibility that there
        // are older files that are no longer needed and should be cleaned up. In here, we delete all files that are part
        // of older primary term.
        if (olderPrimaryCleaned.trySet(Boolean.TRUE)) {
            // First we delete all stale primary terms folders from remote store
            assert readers.isEmpty() == false : shardId + " Expected non-empty readers";
            long minimumReferencedPrimaryTerm = readers.stream().map(BaseTranslogReader::getPrimaryTerm).min(Long::compare).get();
            translogTransferManager.deletePrimaryTermsAsync(minimumReferencedPrimaryTerm);
        }
    }

    public static void cleanup(Repository repository, ShardId shardId, ThreadPool threadPool) throws IOException {
        assert repository instanceof BlobStoreRepository : "repository should be instance of BlobStoreRepository";
        BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
        FileTransferTracker fileTransferTracker = new FileTransferTracker(shardId);
        TranslogTransferManager translogTransferManager = buildTranslogTransferManager(
            blobStoreRepository,
            threadPool,
            shardId,
            fileTransferTracker
        );
        // clean up all remote translog files
        translogTransferManager.deleteTranslogFiles();
    }

    protected void onDelete() {
        if (primaryModeSupplier.getAsBoolean() == false) {
            logger.trace("skipped delete translog");
            // NO-OP
            return;
        }
        // clean up all remote translog files
        translogTransferManager.delete();
    }

    private class RemoteFsTranslogTransferListener implements TranslogTransferListener {
        /**
         * Releasable instance for the translog
         */
        Releasable transferReleasable;

        /**
         * Generation for the translog
         */
        Long generation;

        /**
         * Primary Term for the translog
         */
        Long primaryTerm;

        /**
         * Tracker holding stats related to Remote Translog Store operations
         */
        final RemoteTranslogTracker remoteTranslogTracker = RemoteFsTranslog.this.remoteTranslogTracker;

        /**
         * Files (.tlog, .ckp, metadata) to upload to Remote Translog Store
         */
        Set<FileSnapshot.TransferFileSnapshot> toUpload;

        /**
         * Total bytes to be uploaded to Remote Translog Store
         */
        long uploadBytes;

        /**
         * System nano time when the Remote Translog Store upload is started
         */
        long uploadStartTime;

        /**
         * System nano time when the Remote Translog Store upload is completed
         */
        long uploadEndTime;

        public RemoteFsTranslogTransferListener(Releasable transferReleasable, Long generation, Long primaryTerm) {
            this.transferReleasable = transferReleasable;
            this.generation = generation;
            this.primaryTerm = primaryTerm;
        }

        @Override
        public void beforeUpload(TransferSnapshot transferSnapshot) throws IOException {
            toUpload = RemoteStoreUtils.getUploadBlobsFromSnapshot(transferSnapshot, fileTransferTracker);
            uploadBytes = RemoteStoreUtils.getTotalBytes(toUpload);
            uploadStartTime = RemoteStoreUtils.getCurrentSystemNanoTime();

            captureStatsBeforeUpload();
        }

        @Override
        public void onUploadComplete(TransferSnapshot transferSnapshot) throws IOException {
            uploadEndTime = RemoteStoreUtils.getCurrentSystemNanoTime();
            captureStatsOnUploadSuccess();

            transferReleasable.close();
            closeFilesIfNoPendingRetentionLocks();
            maxRemoteTranslogGenerationUploaded = generation;
            minRemoteGenReferenced = getMinFileGeneration();
            logger.trace("uploaded translog for {} {} ", primaryTerm, generation);
        }

        @Override
        public void onUploadFailed(TransferSnapshot transferSnapshot, Exception ex) throws IOException {
            uploadEndTime = RemoteStoreUtils.getCurrentSystemNanoTime();
            captureStatsOnUploadFailure();

            transferReleasable.close();
            closeFilesIfNoPendingRetentionLocks();

            if (ex instanceof IOException) {
                throw (IOException) ex;
            } else {
                throw (RuntimeException) ex;
            }
        }

        /**
         * Adds relevant stats to the tracker when an upload is started
         */
        private void captureStatsBeforeUpload() {
            remoteTranslogTracker.incrementUploadsStarted();
            remoteTranslogTracker.addUploadBytesStarted(uploadBytes);
        }

        /**
         * Adds relevant stats to the tracker when an upload is successfully completed
         */
        private void captureStatsOnUploadSuccess() {
            long uploadDurationInMillis = (uploadEndTime - uploadStartTime) / 1_000_000L;
            remoteTranslogTracker.incrementUploadsSucceeded();
            remoteTranslogTracker.addUploadBytesSucceeded(uploadBytes);
            remoteTranslogTracker.addUploadTimeInMillis(uploadDurationInMillis);
            remoteTranslogTracker.setLastUploadTimestamp(System.currentTimeMillis());

            remoteTranslogTracker.updateUploadBytesMovingAverage(uploadBytes);
            if (uploadDurationInMillis > 0) {
                remoteTranslogTracker.updateUploadBytesPerSecMovingAverage((uploadBytes * 1_000L) / uploadDurationInMillis);
            }

            remoteTranslogTracker.updateUploadTimeMovingAverage(uploadDurationInMillis);
        }

        /**
         * Adds relevant stats to the tracker when an upload has failed
         */
        private void captureStatsOnUploadFailure() {
            remoteTranslogTracker.incrementUploadsFailed();
            remoteTranslogTracker.addUploadTimeInMillis((uploadEndTime - uploadStartTime) / 1_000_000L);

            Set<String> uploadedFiles = fileTransferTracker.allUploaded();
            Set<FileSnapshot.TransferFileSnapshot> successfulUploads = new HashSet<>();
            Set<FileSnapshot.TransferFileSnapshot> failedUploads = new HashSet<>();
            for (FileSnapshot.TransferFileSnapshot file : toUpload) {
                if (uploadedFiles.contains(file.getName())) {
                    successfulUploads.add(file);
                } else {
                    failedUploads.add(file);
                }
            }

            remoteTranslogTracker.addUploadBytesSucceeded(RemoteStoreUtils.getTotalBytes(successfulUploads));
            remoteTranslogTracker.addUploadBytesFailed(RemoteStoreUtils.getTotalBytes(failedUploads));
        }
    }
}
