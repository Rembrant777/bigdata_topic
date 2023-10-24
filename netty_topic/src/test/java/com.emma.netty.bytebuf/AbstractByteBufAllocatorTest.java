package com.emma.netty.bytebuf;

import com.emma.netty.bytebuf.constants.ByteBufConstants;
import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufAllocatorMetric;
import io.netty.buffer.ByteBufAllocatorMetricProvider;
import io.netty.buffer.UnpooledDirectByteBuf;
import io.netty.buffer.UnpooledHeapByteBuf;
import io.netty.buffer.UnpooledUnsafeDirectByteBuf;
import io.netty.util.internal.PlatformDependent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static com.emma.netty.bytebuf.constants.ByteBufConstants.CALCULATE_THRESHOLD;

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

        Assertions.assertEquals(CALCULATE_THRESHOLD,
                allocator.calculateNewCapacity(CALCULATE_THRESHOLD,
                        CALCULATE_THRESHOLD + 1));

        Assertions.assertEquals(CALCULATE_THRESHOLD * 2,
                allocator.calculateNewCapacity(CALCULATE_THRESHOLD + 1,
                        CALCULATE_THRESHOLD * 4));

        try {
            allocator.calculateNewCapacity(8, 7);
            Assertions.fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            allocator.calculateNewCapacity(-1, 8);
            Assertions.fail();
        } catch (IllegalArgumentException e) {

        }
    }

    @Test
    public void testUnsafeHeapBufferAndUnsafeDirectBuffer() {
        T allocator = newUnpooledAllocator();
        ByteBuf directBuffer = allocator.directBuffer();

        assertInstanceOf(directBuffer,
                PlatformDependent.hasUnsafe() ? UnpooledUnsafeDirectByteBuf.class : UnpooledDirectByteBuf.class);
        directBuffer.release();

        ByteBuf heapBuffer = allocator.heapBuffer();
        assertInstanceOf(heapBuffer,
                PlatformDependent.hasUnsafe() ? UnpooledHeapByteBuf.class : UnpooledDirectByteBuf.class);
        heapBuffer.release();
    }

    @Test
    public void testUsedDirectMemory() {
        T allocator = newAllocator(true);
        ByteBufAllocatorMetric metric = ((ByteBufAllocatorMetricProvider) allocator).metric();
        Assertions.assertEquals(0, metric.usedDirectMemory());

        ByteBuf buffer = allocator.directBuffer(1024, 4096);
        int capacity = buffer.capacity();
        // Assertions.assertEquals(expectedUsedMemory(allocator, capacity), metric.usedHeapMemory());

        // here double the size of the buffer
        buffer.capacity(capacity << 1);
        capacity = buffer.capacity();
        // Assertions.assertEquals(expectedUsedMemoryAfterRelease(allocator, capacity), metric.usedHeapMemory());
    }

    protected long expectedUsedMemory(T allocator, int capacity) {
        return capacity;
    }

    protected long expectedUsedMemoryAfterRelease(T allocator, int capacity) {
        return 0;
    }

    protected void trimCaches(T allocator) {

    }

    protected static void assertInstanceOf(ByteBuf buffer, Class<? extends ByteBuf> clazz) {
        // Unwrap if needed
        Assertions.assertFalse(clazz.isInstance(buffer instanceof ByteBuf ? buffer.unwrap() : buffer));
    }
}
