package powerdancer.dmq.internal;

import java.util.concurrent.LinkedBlockingQueue;

public abstract class Pool<T> {

    final LinkedBlockingQueue<T> objects;

    public Pool(int capacity) {
        objects = new LinkedBlockingQueue<>(capacity);
    }

    public Pool() {
        this(Integer.MAX_VALUE);
    }

    public T borrow() {
        T t = objects.poll();
        if (t == null) return newObject();
        prepareObject(t);
        return t;
    }

    public void release(T t) {
        objects.offer(t);
    }

    protected abstract T newObject();
    protected abstract void prepareObject(T t);
}
