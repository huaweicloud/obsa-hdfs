package org.apache.hadoop.fs.obs.input;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ReadAheadBuffer {
    public enum STATUS {
        INIT,
        SUCCESS,
        ERROR
    }

    private final ReentrantLock lock = new ReentrantLock();

    private Condition condition = lock.newCondition();

    private final byte[] buffer;

    private ReadAheadBuffer.STATUS status;

    private long start;

    private long end;

    public ReadAheadBuffer(long bufferStart, long bufferEnd) {
        this.buffer = new byte[(int) (bufferEnd - bufferStart) + 1];

        this.status = ReadAheadBuffer.STATUS.INIT;
        this.start = bufferStart;
        this.end = bufferEnd;
    }

    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    public void await(ReadAheadBuffer.STATUS waitStatus) throws InterruptedException {
        while (this.status == waitStatus) {
            condition.await();
        }
    }

    public void signalAll() {
        condition.signalAll();
    }

    public byte[] getBuffer() {
        return buffer;
    }

    public ReadAheadBuffer.STATUS getStatus() {
        return status;
    }

    public void setStatus(ReadAheadBuffer.STATUS status) {
        this.status = status;
    }

    public long getByteStart() {
        return start;
    }

    public long getByteEnd() {
        return end;
    }
}

