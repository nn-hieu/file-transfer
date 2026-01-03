package com.hieunn.filereceiver.configs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hieunn.commonlib.utils.ObjectUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@RequiredArgsConstructor
public class UtilsBean {
    private final ObjectMapper objectMapper;

    @Bean
    public ObjectUtils objectUtils() {
        return new ObjectUtils(objectMapper);
    }
}
