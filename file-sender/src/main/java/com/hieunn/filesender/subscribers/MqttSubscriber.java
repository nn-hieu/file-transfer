package com.hieunn.filesender.subscribers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hieunn.filesender.events.MqttConnectedEvent;
import com.hieunn.filesender.listners.FileTransferCompletedListener;
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
    private String serverName;

    @Value("${mqtt.topic.file.transfer-completed}")
    private String fileTransferCompletedTopic;

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
                fileTransferCompletedTopic + "/" + serverName,
                1,
                new FileTransferCompletedListener(objectMapper, fileService)
        );

        log.info("MQTT topics subscribed successfully");
    }
}
