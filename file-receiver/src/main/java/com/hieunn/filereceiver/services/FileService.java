package com.hieunn.filereceiver.services;

import com.hieunn.filereceiver.dtos.ChunkFile;
import com.hieunn.filereceiver.dtos.FileMetadata;

public interface FileService {
    void handleFileMetadata(FileMetadata fileMetadata);

    void handleChunkFile(ChunkFile chunkFile);
}
