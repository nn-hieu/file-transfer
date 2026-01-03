package com.hieunn.filesender.configs;

import com.hieunn.commonlib.events.MqttConnectedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@RequiredArgsConstructor
@Slf4j
public class MqttConfig {
    @Value("${mqtt.broker}")
    private String broker;

    @Value("${mqtt.client-id}")
    private String clientId;

    private final ApplicationEventPublisher appPublisher;

    @Bean
    public MqttClient mqttClient() throws MqttException {
        MqttClient mqttClient = new MqttClient(broker, clientId, new MemoryPersistence());
        mqttClient.setCallback(new MqttCallbackExtended() {
            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                appPublisher.publishEvent(new MqttConnectedEvent());
            }

            @Override
            public void connectionLost(Throwable cause) {
                log.error("MQTT connection lost: {}", cause.getMessage());
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {}

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {}
        });

        return mqttClient;
    }
}
