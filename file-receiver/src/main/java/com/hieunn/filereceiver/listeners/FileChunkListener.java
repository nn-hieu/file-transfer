package com.hieunn.filereceiver.listeners;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hieunn.commonlib.dtos.ChunkFile;
import com.hieunn.commonlib.dtos.MqttEnvelope;
import com.hieunn.filereceiver.services.FileService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.beans.factory.annotation.Value;

@RequiredArgsConstructor
@Slf4j
public class FileChunkListener implements IMqttMessageListener {
    private final ObjectMapper objectMapper;
    private final FileService fileService;
    private final String serviceName;

    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
        byte[] payload = mqttMessage.getPayload();

        MqttEnvelope<ChunkFile> envelope = objectMapper.readValue(payload, new TypeReference<>() {});
        if (!serviceName.equals(envelope.getTargetService())) {
            return;
        }

        log.info(
                "Received chunk file: index={}, fileId={}, sourceService={}",
                envelope.getPayload().getIndex(), envelope.getPayload().getFileId(), envelope.getSourceService()
        );

        fileService.handleChunkFile(envelope);
    }
}
