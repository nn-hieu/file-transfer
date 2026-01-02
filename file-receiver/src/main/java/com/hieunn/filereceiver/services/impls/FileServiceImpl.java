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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

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
        assert cache != null;
        cache.put(fileMetadata.getFileId(), fileMetadata);
    }

    @Override
    public void handleChunkFile(ChunkFile chunkFile) {
        try {
            // Get metadata from cache
            Cache metadataCache = cacheManager.getCache(CacheName.FILE_META_DATA.getValue());
            assert metadataCache != null;
            FileMetadata metadata = metadataCache.get(chunkFile.getFileId(), FileMetadata.class);
            if (metadata == null) {
                throw new RuntimeException("Metadata not found for fileId: " + chunkFile.getFileId());
            }

            Path fileDir = Paths.get(folderName, chunkFile.getFileId());
            if (Files.notExists(fileDir)) {
                Files.createDirectories(fileDir);
            }

            Path targetFilePath = fileDir.resolve(metadata.getFileName() + ".temp");

            // Write data of chunk file into temp file
            try (RandomAccessFile raf = new RandomAccessFile(targetFilePath.toFile(), "rw")) {
                long offset = (long) chunkFile.getIndex() * metadata.getChunkSizeInBytes();
                raf.seek(offset);
                raf.write(chunkFile.getData());
            }

            // Update chunk state into cache
            Cache chunkStateCache = cacheManager.getCache(CacheName.FILE_CHUNK_STATE.getValue());
            assert chunkStateCache != null;
            FileChunkState state = chunkStateCache.get(chunkFile.getFileId(), FileChunkState.class);
            if (state == null) {
                state = new FileChunkState(metadata.getTotalChunks());
                chunkStateCache.put(chunkFile.getFileId(), state);
            }

            // Mark chunk already received
            state.getReceivedChunks().add(chunkFile.getIndex());

            if (state.getReceivedChunks().size() == metadata.getTotalChunks()
                    && state.getMerged().compareAndSet(false, true)
            ) {
                this.finalizeFile(targetFilePath, metadata);
                this.cleanupCache(chunkFile.getFileId());
            }
        } catch (IOException e) {
            log.error("Error writing chunk file for fileId={}", chunkFile.getFileId(), e);
            throw new RuntimeException("Cannot process chunk file", e);
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

    private void finalizeFile(Path tempFilePath, FileMetadata metadata) throws IOException {
        Path finalFilePath = tempFilePath.getParent().resolve(metadata.getFileName());

        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not found", e);
        }

        try (
                InputStream is = new FileInputStream(tempFilePath.toFile());
                DigestInputStream dis = new DigestInputStream(is, digest)
        ) {
            byte[] buffer = new byte[32 * 1024];
            while (dis.read(buffer) != -1) {
                // Just let the DigestInputStream read data to buffer and feed data into MessageDigest
            }
        }

        String calculatedChecksum = HexFormat.of().formatHex(digest.digest());

        if (!calculatedChecksum.equalsIgnoreCase(metadata.getChecksum())) {
            log.error(
                    "Checksum mismatch for fileId={}, fileName={}. Expected={}, Actual={}",
                    metadata.getFileId(), metadata.getFileName(), metadata.getChecksum(), calculatedChecksum
            );

            Files.deleteIfExists(tempFilePath);
            this.cleanupCache(metadata.getFileId());
            this.cleanupFolder(metadata.getFileId());

            return;
        }

        Files.move(
                tempFilePath,
                finalFilePath,
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE
        );

        log.info("Received successfully new file: {}", finalFilePath);
    }

    private void cleanupFolder(String fileId) throws IOException {
        Path fileDir = Paths.get(folderName, fileId);

        if (Files.notExists(fileDir)) {
            return;
        }

        Files.deleteIfExists(fileDir);
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
