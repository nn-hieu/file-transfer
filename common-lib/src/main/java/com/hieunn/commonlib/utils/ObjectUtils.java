package com.hieunn.commonlib.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;

import java.io.IOException;

@RequiredArgsConstructor
public class ObjectUtils {
    private final ObjectMapper objectMapper;

    public byte[] convertObjectToBytes(Object object) throws IOException {
        return objectMapper.writeValueAsBytes(object);
    }
}
