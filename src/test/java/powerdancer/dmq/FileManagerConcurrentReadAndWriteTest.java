package powerdancer.dmq;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.opentest4j.AssertionFailedError;
import powerdancer.dmq.inmem.InMemoryFileManager;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

public class FileManagerConcurrentReadAndWriteTest {

    @Test
    public void test() throws Throwable {
        int numMessagesPerProducer = 10000;
        CountDownLatch[] latches = new CountDownLatch[numMessagesPerProducer];
        for (int i = 0; i < numMessagesPerProducer; i++) {
            latches[i] = new CountDownLatch(8);
        }

        try {
            Assertions.assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {

                Clock c = Mockito.mock(Clock.class);
                Instant now = Instant.now();
                Instant later = now.plus(Duration.ofHours(2));
                AtomicBoolean useNow = new AtomicBoolean(true);
                Mockito.when(c.instant()).thenAnswer(inv -> { // to simulate more file rotations more aggressively
                    useNow.set(!useNow.get());
                    return useNow.get() ? now : later;
                });

                Path p = Files.createTempDirectory("FileManagerConcurrentReadAndWriteTest");
                InMemoryFileManager m = new InMemoryFileManager(p, c) {
                    @Override
                    protected void delete(Path p) {
                        this.offsets.remove(p); // since we are simulating the clock, we cant be sure the files are deletable yet(producer still writing into)
                    }
                };

                Runnable consumerRunnable = () -> {
                    try {
                        m.consume("myTopic", b -> {
                            byte[] pl = new byte[b.remaining()];
                            b.get(pl);
                            int i = Integer.decode(new String(pl, StandardCharsets.UTF_8));
                            latches[i].countDown();
                        }, 4, 3);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                };

                Thread consumerSupervisor = new Thread(() -> {
                    while(true) {
                        Thread t = new Thread(consumerRunnable);
                        t.start();
                        try {
                            Thread.sleep(500L);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        t.interrupt();
                        try {
                            t.join();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                });
                consumerSupervisor.start();

                try (Producer<Void> producer = m.newProducer("myTopic", 4, 10, Void.class)) {
                    try (Producer<Void> producer2 = m.newProducer("myTopic", 4, 10, Void.class)) {
                        IntStream.range(0, 8).parallel().forEach(n -> {
                            IntStream.range(0, numMessagesPerProducer).parallel().forEach(
                                    i -> {
                                        try {
                                            ((i < 5) ? producer : producer2).send(b -> {
                                                b.put((i + "").getBytes(StandardCharsets.UTF_8));
                                                return null;
                                            });
                                        } catch (Producer.ProductionException e) {
                                            e.printStackTrace();
                                        } catch (InterruptedException e) {
                                            e.printStackTrace();
                                        }
                                    }
                            );
                        });
                    }
                }

                Files.list(p.resolve("myTopic")).forEach(f -> {
                    try {
                        System.out.println(f.getFileName());
                        System.out.println(new String(Files.readAllBytes(f), StandardCharsets.UTF_8));

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });

                try {
                    for (int i = 0; i < numMessagesPerProducer; i++) {
                        latches[i].await();
                    }
                } finally {
                    consumerSupervisor.interrupt();
                }

            });
        } catch (AssertionFailedError e) {
            for (int i = 0; i < numMessagesPerProducer; i++) {
                System.out.println("remaining counts for message " + i + " = " + latches[i].getCount());
            }
            throw e;
        }

    }
}
