package com.emma.netty.bytebuf;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.internal.PlatformDependent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * refer to:
 * https://github.com/netty/netty/blob/32a025b4eb2c9fefb389ecd5c781f7cabc6ab139/buffer/src/test/java/io/netty/buffer/ByteBufAllocatorTest.java
 */
public abstract class ByteBufAllocatorTest {

    protected abstract int defaultMaxCapacity();

    protected abstract int defaultMaxComponents();

    protected abstract ByteBufAllocator newAllocator(boolean preferDirect);

    /**
     * method to detect whether user prefers direct memory and
     * at the same time current platform(os) supports the unsafe memory strategy.
     *
     * @param preferDirect
     */
    protected boolean isDirectExpected(boolean preferDirect) {
        return preferDirect && PlatformDependent.hasUnsafe();
    }

    @Test
    public void testBuffer() {
        testBuffer();
        testBuffer();
    }

    /**
     * method to verify whether direct memory or non-direct memory support.
     *
     * @param preferDirect whether user prefer use direct memory.
     */
    private void testBuffer(boolean preferDirect) {
        ByteBufAllocator allocator = newAllocator(preferDirect);
        ByteBuf buffer = allocator.buffer(1);
        try {
            assertBuffer(buffer, isDirectExpected(preferDirect), 1, defaultMaxCapacity());
        } finally {
            buffer.release();
        }
    }


    /**
     * method to test whether apply buffer space with direct memory or non-direct memory.
     * and with the expected defined maximum capacity.
     */
    @Test
    public void testBufferWithCapacity() {
        // apply byte buffer from direct memory with maximum capacity equals to 8 bytes
        testBufferWithCapacity(true, 8);

        // apply byte buffer from non-direct memory with maximum capacity equals to 8 bytes
        testBufferWithCapacity(false, 8);
    }

    /**
     * method to execute apply required maximum capacity byte buffer.
     *
     * @param preferDirect whether use direct memory or non-direct memory.
     * @param maxCapacity  maximum capacity apply from the memory.
     */
    private void testBufferWithCapacity(boolean preferDirect, int maxCapacity) {
        ByteBufAllocator allocator = newAllocator(preferDirect);
        ByteBuf buffer = allocator.buffer(1, maxCapacity);

        try {
            assertBuffer(buffer, isDirectExpected(preferDirect), 1, maxCapacity);
        } finally {
            buffer.release();
        }
    }

    @Test
    public void testHeapBuffer() {
        testHeapBuffer(true);
        testHeapBuffer(false);
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
    public void testDirectBuffer() {
        testDirectBuffer(true);
        testDirectBuffer(false);
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

    @Test
    public void testCompositeBuffer() {
        testCompositeBuffer(true);
        testCompositeBuffer(false);
    }

    private void testCompositeBuffer(boolean preferDirect) {
        ByteBufAllocator allocator = newAllocator(preferDirect);
        CompositeByteBuf buffer = allocator.compositeBuffer();
        try {
            assertCompositeByteBuf(buffer, defaultMaxComponents());
        } finally {
            buffer.release();
        }
    }

    @Test
    public void testCompositeBufferWithCapacity() {
        testCompositeHeapBufferWithCapacity(true, 8);
        testCompositeHeapBufferWithCapacity(false, 8);
    }

    @Test
    public void testCompositeHeapBuffer() {
        testCompositeHeapBuffer(true);
        testCompositeHeapBuffer(false);
    }

    private void testCompositeHeapBuffer(boolean preferDirect) {
        ByteBufAllocator allocator = newAllocator(preferDirect);
        CompositeByteBuf buffer = allocator.compositeHeapBuffer();
        try {
            assertCompositeByteBuf(buffer, defaultMaxComponents());
        } finally {
            buffer.release();
        }
    }

    @Test
    public void testCompositeHeapBufferWithCapacity() {
        testCompositeHeapBufferWithCapacity(true, 8);
        testCompositeHeapBufferWithCapacity(false, 8);
    }

    private void testCompositeHeapBufferWithCapacity(boolean preferDirect, int maxNumComponents) {
        ByteBufAllocator allocator = newAllocator(preferDirect);
        CompositeByteBuf buffer = allocator.compositeHeapBuffer(maxNumComponents);
        try {
            assertCompositeByteBuf(buffer, maxNumComponents);
        } finally {
            buffer.release();
        }
    }

    @Test
    public void testCompositeDirectBuffer() {
        testCompositeDirectBuffer(true);
        testCompositeDirectBuffer(false);
    }

    private void testCompositeDirectBuffer(boolean preferDirect) {
        ByteBufAllocator allocator = newAllocator(preferDirect);
        CompositeByteBuf buffer = allocator.compositeDirectBuffer();
        try {
            assertCompositeByteBuf(buffer, defaultMaxComponents());
        } finally {
            buffer.release();
        }
    }

    @Test
    public void testCompositeDirectBufferWithCapacity() {
        testCompositeDirectBufferWithCapacity(true, 8);
        testCompositeDirectBufferWithCapacity(false, 8);
    }

    private void testCompositeDirectBufferWithCapacity(boolean preferDirect, int maxNumComponents) {
        ByteBufAllocator allocator = newAllocator(preferDirect);
        CompositeByteBuf buffer = allocator.compositeDirectBuffer(maxNumComponents);
        try {
            assertCompositeByteBuf(buffer, maxNumComponents);
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

    private void assertCompositeByteBuf(CompositeByteBuf buffer, int expectedMaxNumComponents) {
        Assertions.assertEquals(0, buffer.numComponents());
        Assertions.assertEquals(expectedMaxNumComponents, buffer.maxNumComponents());
        assertBuffer(buffer, false, 0, defaultMaxCapacity());
    }
}
