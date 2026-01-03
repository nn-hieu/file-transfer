package com.hieunn.commonlib.dtos;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class MqttEnvelope<T> {
    private String sourceService;
    private String targetService;
    private T payload;
}
