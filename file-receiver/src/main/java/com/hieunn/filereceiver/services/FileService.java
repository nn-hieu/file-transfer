package com.hieunn.filereceiver.services;

import com.hieunn.commonlib.dtos.ChunkFile;
import com.hieunn.commonlib.dtos.FileMetadata;
import com.hieunn.commonlib.dtos.MqttEnvelope;

public interface FileService {
    void handleFileMetadata(MqttEnvelope<FileMetadata> envelope);

    void handleChunkFile(MqttEnvelope<ChunkFile> envelope);
}
