package com.hieunn.filereceiver.services.impls;

import com.hieunn.filereceiver.dtos.ChunkFile;
import com.hieunn.filereceiver.dtos.FileChunkState;
import com.hieunn.filereceiver.dtos.FileMetadata;
import com.hieunn.filereceiver.enums.CacheName;
import com.hieunn.filereceiver.services.FileService;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.*;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.stream.Stream;

@Slf4j
@Service
@RequiredArgsConstructor
public class FileServiceImpl implements FileService {
    private final CacheManager cacheManager;

    @Value("${app.folder-name}")
    private String folderName;

    @Override
    public void handleFileMetadata(FileMetadata fileMetadata) {
        Cache cache = cacheManager.getCache(CacheName.FILE_META_DATA.getValue());
        if (cache == null) {
            throw new RuntimeException("Cache not found");
        }
        cache.put(fileMetadata.getId(), fileMetadata);
    }

    @Override
    public void handleChunkFile(ChunkFile chunkFile) {
        try {
            // Save chunk file to folder
            Path fileDir = Paths.get(folderName, chunkFile.getFileId());
            Files.createDirectories(fileDir);

            String partFileName = chunkFile.getIndex() + ".part";
            Path partFilePath = fileDir.resolve(partFileName);

            Files.write(
                    partFilePath,
                    chunkFile.getData(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING
            );

            // Get metadata from cache
            Cache metadataCache = cacheManager.getCache(CacheName.FILE_META_DATA.getValue());
            assert metadataCache != null;
            FileMetadata metadata = metadataCache.get(chunkFile.getFileId(), FileMetadata.class);
            if (metadata == null) {
                throw new RuntimeException("Metadata not found");
            }

            // Get or create chunk state from cache
            Cache chunkStateCache = cacheManager.getCache(CacheName.FILE_CHUNK_STATE.getValue());
            assert chunkStateCache != null;
            FileChunkState state = chunkStateCache.get(chunkFile.getFileId(), FileChunkState.class);
            if (state == null) {
                state = new FileChunkState(metadata.getTotalChunks());
                chunkStateCache.put(chunkFile.getFileId(), state);
            }

            // Mark that chunk already received
            state.getReceivedChunks().add(chunkFile.getIndex());

            if (state.getReceivedChunks().size() == metadata.getTotalChunks()
                    && state.getMerged().compareAndSet(false, true)
            ) {
                this.mergeChunksAndVerify(metadata);
                this.cleanupFolder(chunkFile.getFileId());
                this.cleanupCache(chunkFile.getFileId());
            }
        } catch (IOException e) {
            throw new RuntimeException("Cannot create file folder", e);
        }
    }

    @PostConstruct
    protected void createFolderForStoringFiles() {
        try {
            Path receivedDir = Paths.get(folderName);

            if (Files.notExists(receivedDir)) {
                Files.createDirectories(receivedDir);
            }
        } catch (IOException e) {
            throw new RuntimeException("Cannot create folder", e);
        }
    }

    private void mergeChunksAndVerify(FileMetadata metadata) throws IOException {
        Path fileDir = Paths.get(folderName, metadata.getId());
        Path tempFile = fileDir.resolve(metadata.getFileName() + ".tmp");
        Path finalFile = fileDir.resolve(metadata.getFileName());

        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        try (
                OutputStream fos = Files.newOutputStream(
                        tempFile,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING
                );
                DigestOutputStream dos = new DigestOutputStream(fos, digest)
        ) {
            for (int i = 0; i < metadata.getTotalChunks(); i++) {
                Path part = fileDir.resolve(i + ".part");
                Files.copy(part, dos);
            }
        }

        String calculatedChecksum = HexFormat.of().formatHex(digest.digest());
        if (!calculatedChecksum.equalsIgnoreCase(metadata.getChecksum())) {
            log.error("Checksum mismatch for fileId={}, fileName={}", metadata.getId(), metadata.getFileName());

            this.cleanupCache(metadata.getId());

            Files.deleteIfExists(tempFile);
            this.cleanupFolder(metadata.getId());
            Files.deleteIfExists(fileDir);

            return;
        }

        Files.move(
                tempFile,
                finalFile,
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE
        );
    }

    private void cleanupFolder(String fileId) throws IOException {
        Path fileDir = Paths.get(folderName, fileId);

        if (Files.notExists(fileDir)) {
            return;
        }

        try (Stream<Path> paths = Files.list(fileDir)) {
            paths
                    .filter(p -> p.toString().endsWith(".part"))
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                        } catch (IOException e) {
                            log.warn("Cannot delete file {}", p, e);
                        }
                    });
        }
    }

    private void cleanupCache(String fileId) {
        Cache metadataCache = cacheManager.getCache(CacheName.FILE_META_DATA.getValue());
        Cache chunkStateCache = cacheManager.getCache(CacheName.FILE_CHUNK_STATE.getValue());

        if (metadataCache != null) {
            metadataCache.evict(fileId);
        }

        if (chunkStateCache != null) {
            chunkStateCache.evict(fileId);
        }
    }
}
