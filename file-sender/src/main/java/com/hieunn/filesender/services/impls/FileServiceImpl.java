package com.hieunn.filesender.services.impls;

import com.hieunn.commonlib.dtos.ChunkFile;
import com.hieunn.commonlib.dtos.FileMetadata;
import com.hieunn.commonlib.dtos.MqttEnvelope;
import com.hieunn.commonlib.utils.ObjectUtils;
import com.hieunn.filesender.dtos.FileState;
import com.hieunn.filesender.enums.CacheName;
import com.hieunn.filesender.services.FileService;
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
import org.springframework.util.unit.DataSize;
import org.springframework.web.multipart.MultipartFile;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class FileServiceImpl implements FileService {
    private final MqttClient mqttClient;
    private final ObjectUtils objectUtils;
    private final CacheManager cacheManager;

    @Value("${mqtt.topic.file.meta-data}")
    private String fileMetaTopic;

    @Value("${mqtt.topic.file.chunk}")
    private String chunkFileTopic;

    @Value("${file.chunk-size}")
    private DataSize chunkSize;

    @Value("${app.folder-name}")
    private String folderName;

    @Value("${spring.application.name}")
    private String serviceName;

    @Value("${file.max-resend-times}")
    private int maxResendFileTimes;

    @PostConstruct
    protected void createFolderForStoringFiles() {
        try {
            Path sentDir = Paths.get(folderName);
            if (Files.notExists(sentDir)) {
                Files.createDirectories(sentDir);
            }
        } catch (IOException e) {
            throw new RuntimeException("Cannot create folder", e);
        }
    }

    @Override
    public void sendFile(MultipartFile file, String targetService) {
        String fileId = UUID.randomUUID().toString();
        Path tempFilePath = Paths.get(folderName, fileId + ".tmp");

        log.info("Sending file...: fileId={}, fileName={}", fileId, file.getOriginalFilename());

        try {
            file.transferTo(tempFilePath);

            FileMetadata metaData = this.buildFileMetadata(file, fileId, tempFilePath);
            this.sendFileMetadata(metaData, targetService);

            try (RandomAccessFile raf = new RandomAccessFile(tempFilePath.toFile(), "r")) {
                int chunkSizeInBytes = (int) chunkSize.toBytes();

                ChunkFile chunkFile = new ChunkFile();
                chunkFile.setFileId(fileId);
                for (int i = 0; i < metaData.getTotalChunks(); ++i) {
//                    if (i == 1 || i == 3) continue;
                    byte[] data = this.readChunkData(raf, i, chunkSizeInBytes, metaData.getFileSize());
                    chunkFile.setIndex(i);
                    chunkFile.setData(data);

                    this.sendChunkFile(chunkFile, targetService);
                }
            }

            FileState fileState = new FileState();
            Cache fileStateCache = cacheManager.getCache(CacheName.FILE_STATE.getValue());
            fileStateCache.put(fileId, fileState);

            log.info("Sent file successfully: fileId={}, fileName={}", fileId, metaData.getFileName());
        } catch (IOException | NoSuchAlgorithmException e) {
            log.error("Error processing file send", e);
            try {
                Files.deleteIfExists(tempFilePath);
            } catch (IOException ex) {
                log.error("Error deleting temp file", ex);
            }
        }
    }

    private FileMetadata buildFileMetadata(MultipartFile file, String fileId, Path tempFilePath)
            throws IOException, NoSuchAlgorithmException {
        FileMetadata metaData = new FileMetadata();
        metaData.setFileId(fileId);
        metaData.setFileName(file.getOriginalFilename());
        metaData.setContentType(file.getContentType());
        metaData.setChunkSizeInBytes(chunkSize.toBytes());

        long fileSize = file.getSize();
        metaData.setFileSize(fileSize);

        int chunkSizeInBytes = (int) chunkSize.toBytes();
        int totalChunks = (int) Math.ceil((double) fileSize / chunkSizeInBytes);
        metaData.setTotalChunks(totalChunks);

        String checksum = this.calculateFileChecksum(tempFilePath);
        metaData.setChecksum(checksum);

        return metaData;
    }

    private String calculateFileChecksum(Path filePath) throws IOException, NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (
                InputStream is = new FileInputStream(filePath.toFile());
                DigestInputStream dis = new DigestInputStream(is, digest)
        ) {
            byte[] buffer = new byte[32 * 1024];
            while (dis.read(buffer) != -1) {

            }
        }

        return HexFormat.of().formatHex(digest.digest());
    }

    private byte[] readChunkData(RandomAccessFile raf, int index, int chunkSize, long totalFileSize) throws IOException {
        long offset = (long) index * chunkSize;
        raf.seek(offset);

        int currentChunkSize = (int) Math.min(chunkSize, totalFileSize - offset);
        byte[] buffer = new byte[currentChunkSize];

        raf.readFully(buffer);
        return buffer;
    }

    @Override
    public void sendFileMetadata(FileMetadata metadata, String targetService) {
        log.info("Sending file metadata...: fileId={}, fileName={}", metadata.getFileId(), metadata.getFileName());

        try {
            MqttEnvelope<FileMetadata> envelope = new MqttEnvelope<>(serviceName, targetService, metadata);

            MqttMessage message = new MqttMessage();
            message.setPayload(objectUtils.convertObjectToBytes(envelope));
            message.setQos(1);

            mqttClient.publish(fileMetaTopic, message);

            log.info("Sent file metadata successfully: fileId={}, fileName={}", metadata.getFileId(), metadata.getFileName());
        } catch (IOException e) {
            throw new RuntimeException("Cannot convert file meta data to bytes", e);
        } catch (MqttException e) {
            throw new RuntimeException("Cannot publish chunk to MQTT", e);
        }
    }

    @Override
    public void sendChunkFile(ChunkFile chunkFile, String targetService) {
        log.info("Sending chunk {}...: fileId={}", chunkFile.getIndex(), chunkFile.getFileId());
        try {
            MqttEnvelope<ChunkFile> envelope = new MqttEnvelope<>(serviceName, targetService, chunkFile);

            MqttMessage message = new MqttMessage();
            message.setPayload(objectUtils.convertObjectToBytes(envelope));
            message.setQos(1);

            mqttClient.publish(chunkFileTopic, message);

            log.info("Sent chunk {} successfully: fileId={}", chunkFile.getIndex(), chunkFile.getFileId());
        } catch (MqttException e) {
            throw new RuntimeException("Cannot publish chunk to MQTT", e);
        } catch (IOException e) {
            throw new RuntimeException("Cannot convert chunk file to bytes", e);
        }
    }

    @Override
    public void resendSpecificChunk(FileMetadata metadata, int index, String targetService) {
//        if (index == 3) return;
        log.info(
                "Resending chunk {}...: fileId={}, fileName={}",
                index, metadata.getFileId(), metadata.getFileName()
        );

        Path filePath = Paths.get(folderName, metadata.getFileId() + ".tmp");
        if (Files.notExists(filePath)) {
            log.error("File cache not found for fileId: {}", metadata.getFileId());
            return;
        }

        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r")) {
            int chunkSizeInBytes = (int) chunkSize.toBytes();
            long fileSize = Files.size(filePath);

            byte[] data = this.readChunkData(raf, index, chunkSizeInBytes, fileSize);

            ChunkFile chunkFile = new ChunkFile();
            chunkFile.setFileId(metadata.getFileId());
            chunkFile.setIndex(index);
            chunkFile.setData(data);

            this.sendChunkFile(chunkFile, targetService);
        } catch (IOException e) {
            throw new RuntimeException("Cannot resend chunk file", e);
        }
    }

    @Override
    public void resendFile(FileMetadata metadata, String targetService) {
        log.info("Resending file...: fileId={}, fileName={}", metadata.getFileId(), metadata.getFileName());

        Cache fileStateCache = cacheManager.getCache(CacheName.FILE_STATE.getValue());
        FileState fileState = fileStateCache.get(metadata.getFileId(), FileState.class);
        if (fileState.getRetryTimes() >= maxResendFileTimes) {
            this.cleanupCache(metadata.getFileId());

            log.error("Reach max resend times");
            return;
        }

        Path tempFilePath = Paths.get(folderName, metadata.getFileId() + ".tmp");
        if (Files.notExists(tempFilePath)) {
            log.error(
                    "Cannot resend file. Temp file not found: fileId={}, fileName={}",
                    metadata.getFileId(), metadata.getFileName()
            );
            return;
        }

        try {
            this.sendFileMetadata(metadata, targetService);

            try (RandomAccessFile raf = new RandomAccessFile(tempFilePath.toFile(), "r")) {
                int chunkSizeInBytes = (int) chunkSize.toBytes();
                long fileSize = Files.size(tempFilePath);

                ChunkFile chunkFile = new ChunkFile();
                chunkFile.setFileId(metadata.getFileId());

                for (int i = 0; i < metadata.getTotalChunks(); i++) {
                    byte[] data = this.readChunkData(raf, i, chunkSizeInBytes, fileSize);
                    chunkFile.setIndex(i);
                    chunkFile.setData(data);

                    this.sendChunkFile(chunkFile, targetService);
                }
            }

            fileState.setRetryTimes(fileState.getRetryTimes() + 1);
            fileStateCache.put(metadata.getFileId(), fileState);

            log.info(
                    "Resent file successfully: fileId={}, fileName={}",
                    metadata.getFileId(),
                    metadata.getFileName()
            );
        } catch (IOException e) {
            log.error("Error while resending file: fileId={}", metadata.getFileId());
        }
    }

    @Override
    public void handleFileCompleted(MqttEnvelope<FileMetadata> envelope) {
        FileMetadata fileMetadata = envelope.getPayload();

        this.cleanupCache(fileMetadata.getFileId());

        Path tempFilePath = Paths.get(folderName, fileMetadata.getFileId() + ".tmp");

        try {
            Files.deleteIfExists(tempFilePath);
        } catch (IOException e) {
            log.error("Cannot delete temp file: {}", tempFilePath);
        }
    }

    private void cleanupCache(String fileId) {
        Cache fileStateCache = cacheManager.getCache(CacheName.FILE_STATE.getValue());
        if (fileStateCache != null) {
            fileStateCache.evict(fileId);
        }
    }
}
