package com.emma.netty.bytebuf;

import com.emma.netty.bytebuf.constants.ByteBufConstants;
import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.internal.PlatformDependent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public abstract class AbstractByteBufAllocatorTest<T extends AbstractByteBufAllocator> extends ByteBufAllocatorTest {
    @Override
    protected abstract T newAllocator(boolean preferDirect);

    protected abstract T newUnpooledAllocator();

    @Override
    protected boolean isDirectExpected(boolean preferDirect) {
        return preferDirect && PlatformDependent.hasUnsafe();
    }

    @Override
    protected final int defaultMaxCapacity() {
        return ByteBufConstants.DEFAULT_MAX_CAPACITY;
    }

    @Override
    protected final int defaultMaxComponents() {
        return ByteBufConstants.DEFAULT_MAX_COMPONENTS;
    }

    @Test
    public void testCalculateNewCapacity() {
        testCalculateNewCapacity(true);
        testCalculateNewCapacity(false);
    }

    private void testCalculateNewCapacity(boolean preferDirect) {
        T allocator = newAllocator(preferDirect);
        Assertions.assertEquals(8, allocator.calculateNewCapacity(1, 8));
        Assertions.assertEquals(7, allocator.calculateNewCapacity(1, 7));
        Assertions.assertEquals(64, allocator.calculateNewCapacity(1, 129));


    }
}
