package com.emma.netty.bytebuf;

import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBufAllocator;

public class AbstractByteBufAllocatorTest<T extends AbstractByteBufAllocator> extends ByteBufAllocatorTest {
    @Override
    protected int defaultMaxCapacity() {
        return 0;
    }

    @Override
    protected int defaultMaxComponents() {
        return 0;
    }

    @Override
    protected ByteBufAllocator newAllocator(boolean preferDirect) {
        return null;
    }

    @Override
    protected boolean isDirectExpected(boolean preferDirect) {
        return false;
    }
}
