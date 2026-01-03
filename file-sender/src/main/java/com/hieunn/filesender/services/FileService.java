package com.hieunn.filesender.services;

import com.hieunn.commonlib.dtos.ChunkFile;
import com.hieunn.commonlib.dtos.FileMetadata;
import com.hieunn.commonlib.dtos.MqttEnvelope;
import org.springframework.web.multipart.MultipartFile;

public interface FileService {
    void sendFile(MultipartFile file, String targetService);

    void sendFileMetadata(FileMetadata fileMetadata, String targetService);

    void sendChunkFile(ChunkFile chunkFile, String targetService);

    void resendSpecificChunk(FileMetadata metadata, int index, String targetService);

    void resendFile(FileMetadata metadata, String targetService);

    void handleFileCompleted(MqttEnvelope<FileMetadata> envelope);
}
