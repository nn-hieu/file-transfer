package com.hieunn.filereceiver.listeners;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hieunn.filereceiver.dtos.FileMetadata;
import com.hieunn.filereceiver.services.FileService;
import lombok.RequiredArgsConstructor;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttMessage;


@RequiredArgsConstructor
public class FileMetaListener implements IMqttMessageListener {
    private final ObjectMapper objectMapper;
    private final FileService fileService;

    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
        byte[] payload = mqttMessage.getPayload();

        FileMetadata fileMetaData = objectMapper.readValue(payload, FileMetadata.class);

        fileService.handleFileMetadata(fileMetaData);
    }
}
