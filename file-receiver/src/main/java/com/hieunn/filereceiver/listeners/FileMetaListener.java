package com.hieunn.filereceiver.listeners;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hieunn.commonlib.dtos.FileMetadata;
import com.hieunn.commonlib.dtos.MqttEnvelope;
import com.hieunn.filereceiver.services.FileService;
import lombok.RequiredArgsConstructor;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttMessage;

@RequiredArgsConstructor
public class FileMetaListener implements IMqttMessageListener {
    private final ObjectMapper objectMapper;
    private final FileService fileService;
    private final String serviceName;

    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
        byte[] payload = mqttMessage.getPayload();

        MqttEnvelope<FileMetadata> envelope = objectMapper.readValue(payload, new TypeReference<>() {});
        if (!serviceName.equals(envelope.getTargetService())) {
            return;
        }

        fileService.handleFileMetadata(envelope);
    }
}
