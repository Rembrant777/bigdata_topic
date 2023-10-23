package com.emma.netty.bytebuf;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public abstract class ByteBufAllocatorTest {
    protected abstract int defaultMaxCapacity();

    protected abstract int defaultMaxComponents();

    protected abstract ByteBufAllocator newAllocator(boolean preferDirect);

    @Test
    public void testBuffer() {
        testBuffer(true);
        testBuffer(false);
    }

    private void testBuffer(boolean preferDirect) {
        ByteBufAllocator allocator = newAllocator(preferDirect);
        ByteBuf buffer = allocator.buffer(1);

        try {
            assertBuffer(buffer, isDirectExpected(preferDirect), 1, defaultMaxCapacity());
        } finally {
            buffer.release();
        }
    }

    protected abstract boolean isDirectExpected(boolean preferDirect);

    @Test
    public void testBufferWithCapacity() {
        testBufferWithCapacity(true, 8);
        testBufferWithCapacity(false, 8);
    }

    private void testBufferWithCapacity(boolean preferDirect, int maxCapacity) {
        ByteBufAllocator allocator = newAllocator(preferDirect);
        ByteBuf buffer = allocator.buffer(1, maxCapacity);
        try {
            assertBuffer(buffer, isDirectExpected(preferDirect), 1, maxCapacity);
        } finally {
            buffer.release();
        }
    }

    private void testHeapBuffer(boolean preferDirect) {
        ByteBufAllocator allocator = newAllocator(preferDirect);
        ByteBuf buffer = allocator.heapBuffer(1);
        try {
            assertBuffer(buffer, false, 1, defaultMaxCapacity());
        } finally {
            buffer.release();
        }
    }

    @Test
    public void testHeapBufferMaxCapacity() {
        testHeapBuffer(true, 8);
        testHeapBuffer(false, 8);
    }

    private void testHeapBuffer(boolean preferDirect, int maxCapacity) {
        ByteBufAllocator allocator = newAllocator(preferDirect);
        ByteBuf buffer = allocator.heapBuffer(1, maxCapacity);
        try {
            assertBuffer(buffer, false, 1, maxCapacity);
        } finally {
            buffer.release();
        }
    }

    private void testDirectBuffer(boolean preferDirect) {
        ByteBufAllocator allocator = newAllocator(preferDirect);
        ByteBuf buffer = allocator.directBuffer(1);
        try {
            assertBuffer(buffer, true, 1, defaultMaxCapacity());
        } finally {
            buffer.release();
        }
    }

    @Test
    public void testDirectBufferMaxCapacity() {
        testDirectBuffer(true, 8);
        testDirectBuffer(false, 8);
    }

    private void testDirectBuffer(boolean preferDirect, int maxCapacity) {
        ByteBufAllocator allocator = newAllocator(preferDirect);
        ByteBuf buffer = allocator.directBuffer(1, maxCapacity);
        try {
            assertBuffer(buffer, true, 1, maxCapacity);
        } finally {
            buffer.release();
        }
    }


    /**
     * Method to verify whether core fields defined in the {@link ByteBuf} are equal to each other.
     * <p>
     * Equation verify fields are:
     * 1. allocated memory type: direct or non-direct;
     * direct:
     * direct memory space is allocated from the system memory space
     * (direct memory usage is a little hard to manipulate like pointer usage in the cpp have to manual manage)
     * <p>
     * and non-direct memory space is allocated from the JVM's heap memory space.
     * non-direct memory space even though user use and forget to relase the corresponding space, the JVM's GC
     * will take over the memory space's re-allocate operation, do not need to manual management.
     * <p>
     * 2. byte buffer's capacity value;
     * In netty's ByteBuf, the capacity value always <= maxCapacity. if user need to append more characters to the
     * ByteBuf and current position is already = capacity, the ByteBuf can automatically expand to a larger capacity <= maxCapacity.
     * <p>
     * But once current capacity == maxCapacity the ByteBuf cannot expand anymore.
     * Continue appending more byte to the ByteBuf will raise out of exception {@link IndexOutOfBoundsException}.
     * <p>
     * 3. byte buffer's maximum capacity value.
     */
    private static void assertBuffer(ByteBuf buffer, boolean expectedDirect, int expectedCapacity, int expectedMaxCapacity) {
        // field-1: byte buffer(ByteBuf) allocated memory type is direct or non-direct
        Assertions.assertEquals(expectedDirect, buffer.isDirect());

        // field-2: byte buffer(ByteBuf) capacity are equal
        Assertions.assertEquals(expectedCapacity, buffer.capacity());

        // field-3: byte buffer(ByteBuf) max capacity are equal
        Assertions.assertEquals(expectedMaxCapacity, buffer.maxCapacity());
    }
}
