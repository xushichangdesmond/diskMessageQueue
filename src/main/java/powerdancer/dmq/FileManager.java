package powerdancer.dmq;

import powerdancer.dmq.internal.Pool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import java.util.zip.CRC32;

public abstract class FileManager {

    public final Path dataPath;
    public final String nodeId;
    public final Clock clock;

    protected abstract void registerNewFile(String topic, Path p);
    protected abstract void closeWriter(String topic, Path p);

    // recommend some randomness in getReadableFiles to ensure all files are proessed more fairly and avoid a big growing file from DOS other files
    protected abstract Stream<Path> getReadableFiles(String topic);
    protected abstract void delete(Path p);
    protected abstract void commitOffset(Path p, long offset);
    protected abstract long getOffset(Path p);

    public FileManager(Path dataPath, Clock clock) throws IOException {
        this.dataPath = Objects.requireNonNull(dataPath);
        this.clock = Objects.requireNonNull(clock);

        Path nodeIdFile = dataPath.resolve("dmq.node.id");
        String generatedId = UUID.randomUUID().toString();
        try {
            Files.write(nodeIdFile, generatedId.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE_NEW);
        } catch (FileAlreadyExistsException e) {
            generatedId = new String(Files.readAllBytes(nodeIdFile), StandardCharsets.UTF_8);
        }
        nodeId = generatedId;
    }

    final WritableFile newFile(String topic) throws IOException {
        Path p = dataPath.resolve(topic);
        Files.createDirectories(p);
        p = p.resolve(UUID.randomUUID().toString());
        FileChannel c = FileChannel.open(p, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);

        try {
            registerNewFile(topic, p);
            return new WritableFile(c, p, topic);
        } catch(RuntimeException e) {
            c.close();
            throw e;
        }
    }

    class WritableFile implements AutoCloseable {
        final FileChannel c;
        public final Path p;
        public final String topic;
        final AtomicInteger numberOfMessages = new AtomicInteger(0);
        final Instant rotateTime;

        WritableFile(FileChannel c, Path p, String topic) {
            this.c = c;
            this.p = p;
            this.topic = topic;
            rotateTime = clock.instant().plus(Duration.ofMinutes(15));
        }

        @Override
        public void close() throws IOException {
            closeWriter(topic, p);
            c.close();
        }

        int write(ByteBuffer b) throws IOException {
            return c.write(b);
        }
    }

    public <R> Producer<R> newProducer(String topic, int maxMessageSize, int messagesPerFile, Class<R> returnType) throws IOException {

        AtomicReference<WritableFile> f = new AtomicReference<>(newFile(topic));
        ThreadLocal<CRC32> crc32 = ThreadLocal.withInitial(CRC32::new);

        Pool<ByteBuffer> bufferPool = new Pool<ByteBuffer>() {
            @Override
            protected ByteBuffer newObject() {
                return ByteBuffer.allocate(maxMessageSize + 8);
            }

            @Override
            protected void prepareObject(ByteBuffer byteBuffer) {
                byteBuffer.clear();
            }
        };

        return new Producer<R>() {
            @Override
            public R send(Writable<R> w) throws ProductionException, InterruptedException {
                // file rotation does not need to be so precise and we can afford to overshoot so
                // race conditions should be acceptable to allow for less synchronizations
                WritableFile wf = f.get();

                if (wf.numberOfMessages.incrementAndGet() == 0) {
                    try {
                        f.getAndSet(newFile(topic)).close();
                    } catch (IOException e) {
                        wf.numberOfMessages.set(0);
                        throw new ProductionException(e.getMessage(), e);
                    }
                } else if (clock.instant().isAfter(wf.rotateTime)){
                    try {
                        f.getAndSet(newFile(topic)).close();
                    } catch (IOException e) {
                        wf.numberOfMessages.set(0);
                        throw new ProductionException(e.getMessage(), e);
                    }
                }

                ByteBuffer b = bufferPool.borrow();
                try {
                    b.position(8);
                    ByteBuffer sliced = b.slice();
                    R rv = w.writeTo(sliced);
                    int length = sliced.position();
                    b.putInt(0, length);
                    CRC32 crc = crc32.get();
                    sliced.flip();
                    crc.reset();
                    crc.update(sliced);
                    long cs = crc.getValue();
                    b.put(4, (byte)((cs>>24) & 0xFF));
                    b.put(5, (byte)((cs>>16) & 0xFF));
                    b.put(6, (byte)((cs>>8) & 0xFF));
                    b.put(7, (byte)((cs) & 0xFF));
                    b.position(0);
                    b.limit(8 + length);
                    try {
                        f.get().write(b);
                    } catch (AsynchronousCloseException e) {
                        return send(w);
                    }
                    return rv;
                } catch (IOException e) {
                    throw new ProductionException(e.getMessage(), e);
                } finally {
                    bufferPool.release(b);
                }
            }

            @Override
            public void close() throws IOException {
                f.get().close();
            }
        };
    }

    public void consume(String topic, Consumer c, int maxMessageSize, int maxBatchSize) throws InterruptedException {
        ByteBuffer headerB = ByteBuffer.allocate(8);
        ByteBuffer mB = ByteBuffer.allocate(maxMessageSize);

        CRC32 crc32 = new CRC32();
        while (true) {
            try {
                getReadableFiles(topic).forEach(p->{
                    if (Thread.currentThread().isInterrupted()) {
                        throw new RuntimeInterruptedException(new InterruptedException());
                    }
                    long offset = getOffset(p);
                    try {
                        if (Files.size(p) <= offset) {
                            if (Files.getLastModifiedTime(p).toInstant().isBefore(clock.instant().minus(Duration.ofHours(1)))) {
                                delete(p);
                            }
                            return;
                        }
                        try (FileChannel fc = FileChannel.open(p, StandardOpenOption.READ)) {

                            int n = 0;
                            while (true) {
                                try {
                                    if (Thread.currentThread().isInterrupted()) {
                                        if (n > 0) commitOffset(p, offset);
                                        throw new RuntimeInterruptedException(new InterruptedException());
                                    }
                                    headerB.clear();
                                    if (fc.read(headerB) != 8) {
                                        if (n > 0) commitOffset(p, offset);
                                        return; // on to next file
                                    }
                                    mB.clear();
                                    int length = headerB.getInt(0);
                                    mB.limit(length);

                                    if (fc.read(mB) != length) {
                                        //System.out.println("skipping to next file " + p);
                                        if (n > 0) commitOffset(p, offset);
                                        return; // on to next file
                                    }
                                    mB.flip();

                                    crc32.reset();
                                    crc32.update(mB);
                                    long cs = crc32.getValue();

                                    if (((byte) ((cs >> 24) & 0xff) != headerB.get(4))
                                            || ((byte) ((cs >> 16) & 0xff) != headerB.get(5))
                                            || ((byte) ((cs >> 8) & 0xff) != headerB.get(6))
                                            || ((byte) ((cs) & 0xff) != headerB.get(7))) {

                                        if (n > 0) commitOffset(p, offset);
                                        return; // on to next file
                                    }
                                    mB.flip();
                                    try {
                                        c.onMessage(mB);
                                        n++;
                                    } catch (Consumer.SkipMessage skipMessage) {

                                    } catch (RuntimeException e) {
                                        //System.out.println(p + " - " + offset + " - " + length + " - " + fc.position());
                                        throw e;
                                    } catch (InterruptedException e) {
                                        throw new RuntimeInterruptedException(e);
                                    }
                                    offset = offset + 8 + length;
                                    if (n == maxBatchSize) {
                                        n = 0;
                                        commitOffset(p, offset);
                                    }
                                } catch (ClosedByInterruptException e) {
                                    if (n > 0) commitOffset(p, offset);
                                    throw new RuntimeInterruptedException(new InterruptedException());
                                }
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace(); // TODO
                    }
                });
            }
            catch(RuntimeInterruptedException e) {
                throw e.getCause();
            }

            Thread.sleep(100L);
        }
    }

    private static class RuntimeInterruptedException extends RuntimeException {
        public RuntimeInterruptedException(InterruptedException e) {
            super(e);
        }

        @Override
        public synchronized InterruptedException getCause() {
            return (InterruptedException) super.getCause();
        }
    }
}