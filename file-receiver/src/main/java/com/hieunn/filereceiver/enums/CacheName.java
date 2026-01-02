package com.hieunn.filereceiver.enums;

import lombok.Getter;

@Getter
public enum CacheName {
    FILE_META_DATA("fileMetaCache"),
    FILE_CHUNK_STATE("fileChunkStateCache");

    private final String value;

    CacheName(String value) {
        this.value = value;
    }
}
