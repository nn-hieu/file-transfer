package com.hieunn.filesender.dtos;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class FileMetadata {
    private String id;
    private String fileName;
    private int totalChunks;
    private String contentType;
    private String checksum;
}
