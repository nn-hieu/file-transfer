package com.hieunn.filesender.listeners;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hieunn.commonlib.dtos.MqttEnvelope;
import com.hieunn.commonlib.dtos.ResendChunkRequest;
import com.hieunn.filesender.services.FileService;
import lombok.RequiredArgsConstructor;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttMessage;

@RequiredArgsConstructor
public class ResendChunkListener implements IMqttMessageListener {
    private final ObjectMapper objectMapper;
    private final FileService fileService;
    private final String serviceName;

    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
        byte[] payload = mqttMessage.getPayload();

        MqttEnvelope<ResendChunkRequest> envelope = objectMapper.readValue(payload, new TypeReference<>() {});
        if (!serviceName.equals(envelope.getTargetService())) {
            return;
        }

        ResendChunkRequest request = envelope.getPayload();

        for (int i : request.getIndexes()) {
            fileService.resendSpecificChunk(request.getMetadata(), i, envelope.getSourceService());
        }
    }
}
