package com.github.zhangner.sequence;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 序列号区间段
 *
 * @author <a href="mailto:zhangner@gmail.com">ZhanGNer</a>
 * @since 2013-05-14 15:52
 */
public class SequenceRange {

    private long min;

    private long max;

    private AtomicLong value;

    public SequenceRange(long min, long max) {
        this.min = min;
        this.max = max;
        this.value = new AtomicLong(min);
    }

    public long getAndIncrement() {
        long currentValue = value.getAndIncrement();
        if (currentValue > max) {
            return -1;
        }

        return currentValue;
    }

}
