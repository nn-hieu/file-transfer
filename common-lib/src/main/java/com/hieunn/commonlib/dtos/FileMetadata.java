package com.hieunn.commonlib.dtos;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class FileMetadata {
    private String fileId;
    private String fileName;
    private long fileSize;
    private long chunkSizeInBytes;
    private int totalChunks;
    private String contentType;
    private String checksum;
}
