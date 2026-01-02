package com.hieunn.filesender.listners;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hieunn.filesender.dtos.FileMetadata;
import com.hieunn.filesender.services.FileService;
import lombok.RequiredArgsConstructor;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttMessage;

@RequiredArgsConstructor
public class FileTransferCompletedListener implements IMqttMessageListener {
    private final ObjectMapper objectMapper;
    private final FileService fileService;

    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
        byte[] payload = mqttMessage.getPayload();

        FileMetadata fileMetaData = objectMapper.readValue(payload, FileMetadata.class);

        fileService.handleFileCompleted(fileMetaData);
    }
}
