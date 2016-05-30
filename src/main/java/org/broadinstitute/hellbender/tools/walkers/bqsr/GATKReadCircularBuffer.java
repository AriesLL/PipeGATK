package org.broadinstitute.hellbender.tools.walkers.bqsr;

import org.broadinstitute.hellbender.utils.read.GATKRead;

/**
 * Created by peipei on 5/25/16.
 */
public class GATKReadCircularBuffer {

    private GATKRead[] buffer;
    private volatile int head, tail;
    private static final int SIZE = 100000;
    public long countConsumer;
    public long countProducer;

    public GATKReadCircularBuffer() {
        buffer = new GATKRead[SIZE];
        head = 0;
        tail = 0;
        countConsumer = 0;
        countProducer = 0;
    }

    public GATKRead take() throws InterruptedException {
        while (head == tail) { // empty

            countConsumer ++;
            Thread.sleep(5);
            //Thread.sleep(0,1000);
        }
        GATKRead e = buffer[tail];
        buffer[tail] = null;
        tail = inc(tail);
        return e;
    }

    public void put(GATKRead m) throws InterruptedException {
        while (inc(head) == tail) { // full

            countProducer++;
            Thread.sleep(5);
            //Thread.sleep(0,1000);
        }
        buffer[head] = m;
        head = inc(head);
    }

    private int inc(int a) {
        int b = a + 1;
        return (b == SIZE) ? 0 : b;
    }

}
