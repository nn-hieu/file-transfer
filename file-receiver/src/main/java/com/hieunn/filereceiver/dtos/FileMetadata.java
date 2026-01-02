package com.hieunn.filereceiver.dtos;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class FileMetadata {
    private String fileId;
    private String fileName;
    private int chunkSizeInBytes;
    private int totalChunks;
    private String contentType;
    private String checksum;
}
