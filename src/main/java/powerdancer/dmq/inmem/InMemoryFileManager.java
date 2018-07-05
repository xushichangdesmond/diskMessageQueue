package powerdancer.dmq.inmem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import powerdancer.dmq.FileManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Clock;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class InMemoryFileManager extends FileManager {
    static final Logger logger = LoggerFactory.getLogger(InMemoryFileManager.class);


    public final ConcurrentHashMap<Path, Long> offsets = new ConcurrentHashMap<>();

    public InMemoryFileManager(Path dataPath, Clock clock) throws IOException {
        super(dataPath, clock);
    }

    @Override
    protected void registerNewFile(String topic, Path p) {

    }

    @Override
    protected void closeWriter(String topic, Path p) {

    }

    @Override
    protected Stream<Path> getReadableFiles(String topic) {
        try {
            return Files.list(dataPath.resolve(topic));
        } catch (NoSuchFileException e) {
            return Stream.empty();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void delete(Path p) {
        if(logger.isDebugEnabled()) {
            logger.debug("deleting file {}", p);
        }
        offsets.remove(p);
        try {
            Files.delete(p);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    protected void commitOffset(Path p, long offset) {
        offsets.put(p, offset);
        if (logger.isDebugEnabled())
            logger.debug("committing offset {} for {}", offset, p);
    }

    @Override
    protected long getOffset(Path p) {
        return offsets.getOrDefault(p, 0L);
    }
}
