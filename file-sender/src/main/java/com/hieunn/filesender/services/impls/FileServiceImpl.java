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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

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

    @Value("${app.max-allowed-virtual-threads}")
    private int maxAllowedVirtualThreads;

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

        log.info(
                "Sending file...: fileId={}, fileName={}, targetService={}",
                fileId, file.getOriginalFilename(), targetService
        );

        Semaphore semaphore = new Semaphore(maxAllowedVirtualThreads);
        try {
            file.transferTo(tempFilePath);

            FileMetadata metaData = this.buildFileMetadata(file, fileId, tempFilePath);
            this.sendFileMetadata(metaData, targetService);

            int chunkSizeInBytes = (int) chunkSize.toBytes();
            int totalChunks = metaData.getTotalChunks();

            try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
                List<Future<?>> futures = new ArrayList<>();
                for (int i = 0; i < totalChunks; ++i) {
                    if (i == 1 || i == 3 || i == 5) continue;
                    final int chunkIndex = i;
                    futures.add(executor.submit(() -> {
                        try {
                            semaphore.acquire();
                            try (
                                    FileChannel fileChannel =
                                            FileChannel.open(tempFilePath, StandardOpenOption.READ)
                            ) {
                                long offset = (long) chunkIndex * chunkSizeInBytes;
                                long remainingSize = metaData.getFileSize() - offset;
                                int currentChunkSize =
                                        (int) Math.min(chunkSizeInBytes, remainingSize);

                                ByteBuffer buffer = ByteBuffer.allocate(currentChunkSize);
                                while (buffer.hasRemaining()) {
                                    fileChannel.read(buffer, offset + buffer.position());
                                }

                                ChunkFile chunkFile = new ChunkFile();
                                chunkFile.setFileId(fileId);
                                chunkFile.setIndex(chunkIndex);
                                chunkFile.setData(buffer.array());

                                this.sendChunkFile(chunkFile, targetService);
                            }
                        } catch (Exception e) {
                            log.error("Error sending chunk {}", chunkIndex, e);
                        } finally {
                            semaphore.release();
                        }
                    }));
                }

                for (Future<?> f : futures) {
                    f.get();
                }
            }

            FileState fileState = new FileState();
            Cache fileStateCache = cacheManager.getCache(CacheName.FILE_STATE.getValue());
            fileStateCache.put(fileId, fileState);

            log.info(
                    "Sent file successfully: fileId={}, fileName={}, targetService={}",
                    fileId, metaData.getFileName(), targetService
            );
        } catch (Exception e) {
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
        log.info(
                "Sending file metadata...: fileId={}, fileName={}, targetService={}",
                metadata.getFileId(), metadata.getFileName(), targetService
        );

        try {
            MqttEnvelope<FileMetadata> envelope = new MqttEnvelope<>(serviceName, targetService, metadata);

            MqttMessage message = new MqttMessage();
            message.setPayload(objectUtils.convertObjectToBytes(envelope));
            message.setQos(1);

            mqttClient.publish(fileMetaTopic, message);

            log.info(
                    "Sent file metadata successfully: fileId={}, fileName={}, targetService={}",
                    metadata.getFileId(), metadata.getFileName(), targetService
            );
        } catch (IOException e) {
            throw new RuntimeException("Cannot convert file meta data to bytes", e);
        } catch (MqttException e) {
            throw new RuntimeException("Cannot publish chunk to MQTT", e);
        }
    }

    @Override
    public void sendChunkFile(ChunkFile chunkFile, String targetService) {
        log.info(
                "Sending chunk {}...: fileId={}, targetService={}",
                chunkFile.getIndex(), chunkFile.getFileId(), targetService
        );
        try {
            MqttEnvelope<ChunkFile> envelope = new MqttEnvelope<>(serviceName, targetService, chunkFile);

            MqttMessage message = new MqttMessage();
            message.setPayload(objectUtils.convertObjectToBytes(envelope));
            message.setQos(1);

            mqttClient.publish(chunkFileTopic, message);

            log.info(
                    "Sent chunk {} successfully: fileId={}, targetService={}",
                    chunkFile.getIndex(), chunkFile.getFileId(), targetService
            );
        } catch (MqttException e) {
            throw new RuntimeException("Cannot publish chunk to MQTT", e);
        } catch (IOException e) {
            throw new RuntimeException("Cannot convert chunk file to bytes", e);
        }
    }

    @Override
    public void resendSpecificChunk(FileMetadata metadata, int index, String targetService) {
        Path filePath = Paths.get(folderName, metadata.getFileId() + ".tmp");
        if (Files.notExists(filePath)) {
            log.error("File cache not found: fileId={}", metadata.getFileId());
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
    public void resendChunks(FileMetadata metadata, int[] indexes, String targetService) {
        Semaphore semaphore = new Semaphore(maxAllowedVirtualThreads);
        List<Future<?>> futures = new ArrayList<>();

        Path filePath = Paths.get(folderName, metadata.getFileId() + ".tmp");
        if (Files.notExists(filePath)) {
            log.error("File cache not found: fileId={}", metadata.getFileId());
            return;
        }

        indexes = Arrays.stream(indexes).distinct().toArray();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (Integer index : indexes) {
                int totalChunks = metadata.getTotalChunks();
                if (index < 0 || index >= totalChunks) {
                    log.warn("Invalid chunk: index={}, fileId={}", index, metadata.getFileId());
                    continue;
                }
                futures.add(executor.submit(() -> {
                    try {
                        semaphore.acquire();
                        this.resendSpecificChunk(metadata, index, targetService);
                    } catch (Exception e) {
                        log.error("Failed to resend chunk {}", index, e);
                    } finally {
                        semaphore.release();
                    }
                }));
            }

            for (Future<?> f : futures) {
                f.get();
            }
        } catch (Exception e) {
            log.error("Resent chunks failed: fileId={}", metadata.getFileId(), e);
        }
    }

    @Override
    public void resendFile(FileMetadata metadata, String targetService) {
        log.info(
                "Resending file...: fileId={}, fileName={}, targetService={}",
                metadata.getFileId(), metadata.getFileName(), targetService
        );

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
                    "Resent file successfully: fileId={}, fileName={}, targetService={}",
                    metadata.getFileId(), metadata.getFileName(), targetService
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
