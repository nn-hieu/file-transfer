package com.hieunn.filesender.subscribers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hieunn.commonlib.events.MqttConnectedEvent;
import com.hieunn.filesender.listeners.FileTransferCompletedListener;
import com.hieunn.filesender.listeners.ResendChunkListener;
import com.hieunn.filesender.listeners.ResendFileListener;
import com.hieunn.filesender.services.FileService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class MqttSubscriber {
    @Value("${spring.application.name}")
    private String serviceName;

    @Value("${mqtt.topic.file.transfer-completed}")
    private String fileTransferCompletedTopic;

    @Value("${mqtt.topic.file.resend-file}")
    private String resendFileTopic;

    @Value("${mqtt.topic.file.resend-chunk}")
    private String resendChunkTopic;

    private final MqttClient mqttClient;
    private final ObjectMapper objectMapper;
    private final FileService fileService;

    @EventListener
    public void onMqttConnected(MqttConnectedEvent ignored) {
        try {
            log.info("Subscribing MQTT topics...");

            this.subscribe();
        } catch (MqttException e) {
            log.error("MQTT topics subscribed failed", e);
        }
    }

    private void subscribe() throws MqttException {
        if (!this.mqttClient.isConnected()) return;

        mqttClient.subscribe(
                fileTransferCompletedTopic,
                1,
                new FileTransferCompletedListener(objectMapper, fileService, serviceName)
        );

        mqttClient.subscribe(
                resendFileTopic,
                1,
                new ResendFileListener(objectMapper, fileService, serviceName)
        );

        mqttClient.subscribe(
                resendChunkTopic,
                1,
                new ResendChunkListener(objectMapper, fileService, serviceName)
        );

        log.info("MQTT topics subscribed successfully");
    }
}
