package com.hieunn.filereceiver.schedulers;

import com.hieunn.commonlib.dtos.FileMetadata;
import com.hieunn.filereceiver.dtos.FileChunkState;
import com.hieunn.filereceiver.enums.CacheName;
import com.hieunn.filereceiver.services.FileService;
import lombok.RequiredArgsConstructor;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.IntStream;

@Component
@RequiredArgsConstructor
public class ChunkCheckingScheduler {
    private final CacheManager cacheManager;
    private final FileService fileService;

    @Scheduled(cron = "0 */3 * * * *")
    public void checkMissingChunks() {
        Cache chunkStateCache = cacheManager.getCache(CacheName.FILE_CHUNK_STATE.getValue());
        Cache metadataCache = cacheManager.getCache(CacheName.FILE_META_DATA.getValue());

        @SuppressWarnings("unchecked")
        ConcurrentMap<String, FileChunkState> allChunkStates =
                (ConcurrentMap<String, FileChunkState>) chunkStateCache.getNativeCache();

        @SuppressWarnings("unchecked")
        ConcurrentMap<String, FileMetadata> allMetadata =
                (ConcurrentMap<String, FileMetadata>) metadataCache.getNativeCache();

        if (allChunkStates.isEmpty()) {
            return;
        }

        allChunkStates.forEach((fileId, state) -> {
            int totalChunks = state.getTotalChunks();
            Set<Integer> receivedChunks = state.getReceivedChunks();

            int[] missingChunks = IntStream.range(0, totalChunks)
                    .filter(i -> !receivedChunks.contains(i))
                    .toArray();

            fileService.sendEventResendChunk(allMetadata.get(fileId), missingChunks, state.getSourceService());
        });
    }
}
