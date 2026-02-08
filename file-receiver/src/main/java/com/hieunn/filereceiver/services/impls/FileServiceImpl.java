package com.hieunn.filereceiver.services.impls;

import com.hieunn.commonlib.dtos.ChunkFile;
import com.hieunn.commonlib.dtos.FileMetadata;
import com.hieunn.commonlib.dtos.MqttEnvelope;
import com.hieunn.commonlib.dtos.ResendChunkRequest;
import com.hieunn.commonlib.utils.ObjectUtils;
import com.hieunn.filereceiver.dtos.FileChunkState;
import com.hieunn.filereceiver.enums.CacheName;
import com.hieunn.filereceiver.services.FileService;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

@Slf4j
@Service
@RequiredArgsConstructor
public class FileServiceImpl implements FileService {
    private final CacheManager cacheManager;
    private final MqttClient mqttClient;
    private final ObjectUtils objectUtils;

    @Value("${mqtt.topic.file.transfer-completed}")
    private String fileTransferCompletedTopic;

    @Value("${mqtt.topic.file.resend-file}")
    private String fileResendTopic;

    @Value("${mqtt.topic.file.resend-chunk}")
    private String chunkResendTopic;

    @Value("${app.folder-name}")
    private String folderName;

    @Value("${spring.application.name}")
    private String serviceName;

    @Value("${file.chunk.max-resend-times}")
    private int maxResendChunkTimes;

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

    @Override
    public void handleFileMetadata(MqttEnvelope<FileMetadata> envelope) {
        FileMetadata metadata = envelope.getPayload();

        Cache cache = cacheManager.getCache(CacheName.FILE_META_DATA.getValue());
        assert cache != null;
        cache.put(metadata.getFileId(), metadata);
    }

    @Override
    public void handleChunkFile(MqttEnvelope<ChunkFile> envelope) {
        ChunkFile chunkFile = envelope.getPayload();

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
                long offset = chunkFile.getIndex() * metadata.getChunkSizeInBytes();
                raf.seek(offset);
                raf.write(chunkFile.getData());
            }

            // Update chunk state into cache
            Cache chunkStateCache = cacheManager.getCache(CacheName.FILE_CHUNK_STATE.getValue());
            assert chunkStateCache != null;
            FileChunkState state = chunkStateCache.get(chunkFile.getFileId(), FileChunkState.class);
            if (state == null) {
                state = new FileChunkState(metadata.getTotalChunks());
                state.setSourceService(envelope.getSourceService());
                state.setTargetService(envelope.getTargetService());
                chunkStateCache.put(chunkFile.getFileId(), state);
            }

            // Mark chunk already received
            state.getReceivedChunks().add(chunkFile.getIndex());
            chunkStateCache.put(chunkFile.getFileId(), state);

            if (state.getReceivedChunks().size() == metadata.getTotalChunks()
                    && state.getMerged().compareAndSet(false, true)
            ) {
                this.finalizeFile(targetFilePath, metadata, envelope);
                this.cleanupCache(chunkFile.getFileId());
            }
        } catch (IOException e) {
            throw new RuntimeException("Cannot process chunk file", e);
        }
    }

    @Override
    public void sendEventResendChunk(FileMetadata metadata, int[] indexes, String targetService) {
        log.info(
                "Sending event resend chunk...: indexes={}, fileId={}, fileName={}",
                indexes, metadata.getFileId(), metadata.getFileName()
        );
        try {
            Cache chunkStateCache = cacheManager.getCache(CacheName.FILE_CHUNK_STATE.getValue());
            FileChunkState chunkState = chunkStateCache.get(metadata.getFileId(), FileChunkState.class);
            if (chunkState.getRetryTimes() >= maxResendChunkTimes) {
                log.error("Reach max resend chunk times");
                this.cleanupCache(metadata.getFileId());
                this.cleanupFolder(metadata.getFileId());
                return;
            }

            ResendChunkRequest request = new ResendChunkRequest(metadata, indexes);
            MqttEnvelope<ResendChunkRequest> envelope = new MqttEnvelope<>(serviceName, targetService, request);

            MqttMessage message = new MqttMessage();
            message.setPayload(objectUtils.convertObjectToBytes(envelope));
            message.setQos(1);

            mqttClient.publish(chunkResendTopic, message);

            chunkState.setRetryTimes(chunkState.getRetryTimes() + 1);
            chunkStateCache.put(metadata.getFileId(), chunkState);

            log.info(
                    "Sent event resend chunk successfully: indexes={}, fileId={}, fileName={}",
                    indexes, metadata.getFileId(), metadata.getFileName()
            );
        } catch (MqttException e) {
            throw new RuntimeException("Cannot publish chunk to MQTT", e);
        } catch (IOException e) {
            throw new RuntimeException("Cannot convert data to bytes", e);
        }
    }

    private void sendEventFileCompleted(FileMetadata metadata, String targetService) {
        log.info("Sending event file completed...: fileId={}, fileName={}", metadata.getFileId(), metadata.getFileName());
        try {
            MqttEnvelope<FileMetadata> envelope = new MqttEnvelope<>(serviceName, targetService, metadata);

            MqttMessage message = new MqttMessage();
            message.setPayload(objectUtils.convertObjectToBytes(envelope));
            message.setQos(1);

            mqttClient.publish(fileTransferCompletedTopic, message);

            log.info("Sent event file completed successfully: fileId={}", metadata.getFileId());
        } catch (MqttException e) {
            throw new RuntimeException("Cannot publish chunk to MQTT", e);
        } catch (IOException e) {
            throw new RuntimeException("Cannot convert data to bytes", e);
        }
    }

    private void finalizeFile(Path tempFilePath, FileMetadata metadata, MqttEnvelope<?> envelope) throws IOException {
        log.info("Merging chunk and verifying checksum...: fileId={}", metadata.getFileId());

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

        // Verify checksum
        String calculatedChecksum = HexFormat.of().formatHex(digest.digest());
        if (!calculatedChecksum.equalsIgnoreCase(metadata.getChecksum())) {
            log.error(
                    "Checksum mismatch for fileId={}, fileName={}. Expected={}, Actual={}",
                    metadata.getFileId(), metadata.getFileName(), metadata.getChecksum(), calculatedChecksum
            );

            Files.deleteIfExists(tempFilePath);
            this.cleanupCache(metadata.getFileId());
            this.cleanupFolder(metadata.getFileId());

            this.sendEventResendFile(metadata, envelope.getSourceService());

            return;
        }

        // Rename temp file with actual name
        Files.move(
                tempFilePath,
                finalFilePath,
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE
        );

        log.info("Merged new file successfully: path={}", finalFilePath);

        this.sendEventFileCompleted(metadata, envelope.getSourceService());
    }

    public void cleanupFolder(String fileId) throws IOException {
        Path fileDir = Paths.get(folderName, fileId);

        if (Files.exists(fileDir) && Files.isDirectory(fileDir)) {
            Files.walkFileTree(fileDir, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
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

    private void sendEventResendFile(FileMetadata metadata, String targetService) {
        log.info("Sent event resend file: fileId={}", metadata.getFileId());
        try {
            MqttEnvelope<FileMetadata> envelope = new MqttEnvelope<>(serviceName, targetService, metadata);

            MqttMessage message = new MqttMessage();
            message.setPayload(objectUtils.convertObjectToBytes(envelope));
            message.setQos(1);

            mqttClient.publish(fileResendTopic, message);
        } catch (IOException e) {
            throw new RuntimeException("Cannot convert data to bytes", e);
        } catch (MqttException e) {
            throw new RuntimeException("Cannot publish message to MQTT", e);
        }
    }
}
