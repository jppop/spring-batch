package hello;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.annotation.AfterChunk;
import org.springframework.batch.core.annotation.AfterChunkError;
import org.springframework.batch.core.annotation.BeforeChunk;
import org.springframework.batch.core.scope.context.ChunkContext;

public class ChunkListener  {

    private static final Logger log = LoggerFactory.getLogger(ChunkListener.class);

    @BeforeChunk
    public void beforeChunk(ChunkContext chunkContext) {
        log.info("beforeChunk -- chunk context: {}", chunkContext);
    }

    @AfterChunk
    public void afterChunk(ChunkContext chunkContext) {
        log.info("afterChunk -- chunk context: {}", chunkContext);
    }

    @AfterChunkError
    public void afterChunkError(ChunkContext chunkContext) {
        log.error("afterChunkError -- chunk context: {}", chunkContext);
        log.error("afterChunkError -- step context: {}", chunkContext.getStepContext());
    }

}