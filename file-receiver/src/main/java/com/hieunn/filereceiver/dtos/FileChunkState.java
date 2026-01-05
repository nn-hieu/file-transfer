package com.hieunn.filereceiver.dtos;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class FileChunkState {
    private int totalChunks;
    private Set<Integer> receivedChunks = ConcurrentHashMap.newKeySet();
    private AtomicBoolean merged = new AtomicBoolean(false);
    private String sourceService;
    private String targetService;
    private int retryTimes = 0;

    public FileChunkState(int totalChunks) {
        this.totalChunks = totalChunks;
    }
}
