package com.hieunn.filereceiver.subscribers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hieunn.commonlib.events.MqttConnectedEvent;
import com.hieunn.filereceiver.listeners.FileChunkListener;
import com.hieunn.filereceiver.listeners.FileMetaListener;
import com.hieunn.filereceiver.services.FileService;
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
    @Value("${mqtt.topic.file.meta-data}")
    private String fileMetaTopic;

    @Value("${mqtt.topic.file.chunk}")
    private String fileChunkTopic;

    @Value("${spring.application.name}")
    private String serviceName;

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
                fileMetaTopic,
                1,
                new FileMetaListener(objectMapper, fileService, serviceName)
        );

        mqttClient.subscribe(
                fileChunkTopic,
                1,
                new FileChunkListener(objectMapper, fileService, serviceName)
        );

        log.info("MQTT topics subscribed successfully");
    }
}
