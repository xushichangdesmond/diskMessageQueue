package powerdancer.dmq;

import java.nio.ByteBuffer;

@FunctionalInterface
public interface Consumer {

     void onMessage(ByteBuffer b) throws SkipMessage, InterruptedException;

     class SkipMessage extends Throwable{}
}
