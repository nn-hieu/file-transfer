package com.hieunn.filereceiver.listeners;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hieunn.commonlib.dtos.FileMetadata;
import com.hieunn.commonlib.dtos.MqttEnvelope;
import com.hieunn.filereceiver.services.FileService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttMessage;

@RequiredArgsConstructor
@Slf4j
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

        log.info(
                "Received file metadata: fileId={}, fileName={}, sourceService={}",
                envelope.getPayload().getFileId(), envelope.getPayload().getFileName(), envelope.getSourceService()
        );

        fileService.handleFileMetadata(envelope);
    }
}
