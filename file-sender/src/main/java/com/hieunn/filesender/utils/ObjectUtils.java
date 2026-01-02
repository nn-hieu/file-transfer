package com.hieunn.filesender.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@RequiredArgsConstructor
public class ObjectUtils {
    private final ObjectMapper objectMapper;

    public byte[] convertObjectToBytes(Object object) throws IOException {
        return objectMapper.writeValueAsBytes(object);
    }
}
