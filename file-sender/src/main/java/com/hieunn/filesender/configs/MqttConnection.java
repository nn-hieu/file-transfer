package com.hieunn.filesender.configs;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class MqttConnection implements SmartLifecycle {
    private final MqttClient mqttClient;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private volatile Boolean isRunning = false;

    @Override
    public void start() {
        scheduler.scheduleWithFixedDelay(
                this::tryConnect,
                0,
                10,
                TimeUnit.SECONDS
        );
    }

    @Override
    public void stop() {
        log.info("Disconnecting to MQTT broker...");

        try {
            if (mqttClient.isConnected()) {
                mqttClient.disconnect();
            }
            this.isRunning = false;

            log.info("MQTT disconnected");
        } catch (MqttException e) {
            log.warn("Error while disconnecting MQTT: {}", e.getMessage());
        }
    }

    @Override
    public boolean isRunning() {
        return this.isRunning;
    }

    private void tryConnect() {
        if (this.isRunning || mqttClient.isConnected()) {
            return;
        }

        try {
            log.info("Connecting to MQTT broker...");

            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(false);
            options.setAutomaticReconnect(true);
            options.setConnectionTimeout(10);

            mqttClient.connect(options);

            this.isRunning = true;

            log.info("MQTT connected successfully");
        } catch (Exception e) {
            log.warn("MQTT connect failed, retry in 5s: {}", e.getMessage());
        }
    }
}
