package powerdancer.dmq;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Optional;

public interface Producer<R> extends Closeable {

    interface Writable<R> {
        R writeTo(ByteBuffer b);
    }

    R send(Writable<R> w) throws ProductionException, InterruptedException;

    default Optional<R> sendUnchecked(Writable<R> w) {
        try {
            return Optional.of(send(w));
        } catch (ProductionException e) {
            throw new RuntimeException(e.getMessage(), e);
        }  catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    class ProductionException extends Exception {
        public ProductionException() {
        }

        public ProductionException(String message) {
            super(message);
        }

        public ProductionException(String message, Throwable cause) {
            super(message, cause);
        }

        public ProductionException(Throwable cause) {
            super(cause);
        }

        public ProductionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
            super(message, cause, enableSuppression, writableStackTrace);
        }
    }


}