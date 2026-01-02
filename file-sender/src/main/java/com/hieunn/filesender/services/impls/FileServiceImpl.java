package com.hieunn.filesender.services.impls;

import com.hieunn.filesender.dtos.ChunkFile;
import com.hieunn.filesender.dtos.FileMetadata;
import com.hieunn.filesender.services.FileService;
import com.hieunn.filesender.utils.ObjectUtils;
import lombok.RequiredArgsConstructor;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.unit.DataSize;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class FileServiceImpl implements FileService {
    private final MqttClient mqttClient;
    private final ObjectUtils objectUtils;

    @Value("${mqtt.topic.file.meta-data}")
    private String fileMetaTopic;

    @Value("${mqtt.topic.file.chunk}")
    private String chunkFileTopic;

    @Value("${file.chunk-size}")
    private DataSize chunkSize;

    @Override
    public void sendFile(MultipartFile file, String targetService) {
        byte[] fileBytes;
        MessageDigest digest;
        try {
            fileBytes = file.getBytes();
            digest = MessageDigest.getInstance("SHA-256");
        } catch (IOException e) {
            throw new RuntimeException("Cannot get file bytes", e);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not supported", e);
        }

        FileMetadata metaData = new FileMetadata();
        metaData.setId(UUID.randomUUID().toString());
        metaData.setFileName(file.getOriginalFilename());
        metaData.setContentType(file.getContentType());

        long fileSize = file.getSize();
        int chunkSizeInBytes = (int) chunkSize.toBytes();
        int totalChunks = (int) ((fileSize + chunkSizeInBytes - 1) / chunkSizeInBytes);
        metaData.setTotalChunks(totalChunks);

        byte[] hash = digest.digest(fileBytes);
        String checksum = HexFormat.of().formatHex(hash);
        metaData.setChecksum(checksum);

        this.sendFileMetadata(metaData, targetService);

        ChunkFile chunkFile = new ChunkFile();
        chunkFile.setFileId(metaData.getId());
        for (int i = 0; i < totalChunks; i++) {
            int start = i * chunkSizeInBytes;
            int end = Math.min(fileBytes.length, (i + 1) * chunkSizeInBytes);
            byte[] chunkFileData = Arrays.copyOfRange(fileBytes, start, end);
            chunkFile.setIndex(i);
            chunkFile.setData(chunkFileData);

//            if (chunkFileData.length > 10) {
//                chunkFileData[0] = (byte) 0x55;
//                chunkFileData[10] = (byte) 0x00;
//                chunkFileData[9] = (byte) 0x00;
//                chunkFileData[8] = (byte) 0x00;
//            }

            this.sendChunkFile(chunkFile, targetService);
        }
    }

    @Override
    public void sendFileMetadata(FileMetadata fileMetadata, String targetService) {
        try {
            MqttMessage message = new MqttMessage();
            message.setPayload(objectUtils.convertObjectToBytes(fileMetadata));
            message.setQos(1);

            mqttClient.publish(fileMetaTopic + "/" + targetService, message);
        } catch (IOException e) {
            throw new RuntimeException("Cannot convert file meta data to bytes", e);
        } catch (MqttException e) {
            throw new RuntimeException("Cannot publish chunk to MQTT", e);
        }
    }

    @Override
    public void sendChunkFile(ChunkFile chunkFile, String targetService) {
        try {
            MqttMessage message = new MqttMessage();
            message.setPayload(objectUtils.convertObjectToBytes(chunkFile));
            message.setQos(1);

            mqttClient.publish(
                    chunkFileTopic + "/" + targetService,
                    message
            );
        } catch (MqttException e) {
            throw new RuntimeException("Cannot publish chunk to MQTT", e);
        } catch (IOException e) {
            throw new RuntimeException("Cannot convert chunk file to bytes", e);
        }
    }
}
