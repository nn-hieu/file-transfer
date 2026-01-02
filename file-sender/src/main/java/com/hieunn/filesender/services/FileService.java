package com.hieunn.filesender.services;

import com.hieunn.filesender.dtos.ChunkFile;
import com.hieunn.filesender.dtos.FileMetadata;
import org.springframework.web.multipart.MultipartFile;

public interface FileService {
    void sendFile(MultipartFile file, String targetService);

    void sendFileMetadata(FileMetadata fileMetadata, String targetService);

    void sendChunkFile(ChunkFile chunkFile, String targetService);

    void resendSpecificChunk(FileMetadata metadata, int index, String targetService);

    void handleFileCompleted(FileMetadata fileMetadata);
}
