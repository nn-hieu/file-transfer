package com.hieunn.filesender.enums;

import lombok.Getter;

@Getter
public enum CacheName {
    FILE_STATE("fileStateCache");

    private final String value;

    CacheName(String value) {
        this.value = value;
    }
}
