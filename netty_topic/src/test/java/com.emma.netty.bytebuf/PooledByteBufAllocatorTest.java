package com.emma.netty.bytebuf;

import com.emma.netty.bytebuf.constants.ByteBufConstants;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.concurrent.FastThreadLocalThread;
import io.netty.util.internal.PlatformDependent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PooledByteBufAllocatorTest extends AbstractByteBufAllocatorTest<PooledByteBufAllocator> {
    @Override
    protected PooledByteBufAllocator newAllocator(boolean preferDirect) {
        return new PooledByteBufAllocator(preferDirect);
    }

    @Override
    protected PooledByteBufAllocator newUnpooledAllocator() {
        return new PooledByteBufAllocator(0, 0, 8192, 1);
    }

    @Override
    protected long expectedUsedMemory(PooledByteBufAllocator allocator, int capacity) {
        return allocator.metric().chunkSize();
    }

    @Override
    protected long expectedUsedMemoryAfterRelease(PooledByteBufAllocator allocator, int capacity) {
        // This is the case as allocations will start in qInt and chunks in qInt will never be released until
        // these are moved to q000
        // See https://www.bsdcan.org/2006/papers/jemalloc.pdf
        return allocator.metric().chunkSize();
    }


    /**
     * Trim thread local cache for the current Thread, which will give back any cached memory that was not
     * allocated frequently since the last trim operations.
     * <p>
     * Returns true if a cache for the current Thread exists and so was trimmed, false otherwise. s
     */
    @Override
    protected void trimCaches(PooledByteBufAllocator allocator) {
        allocator.trimCurrentThreadCache();
    }

    public void testTrim() {
        PooledByteBufAllocator allocator = newAllocator(true);

        // Should return false as we never allocated from this thread yet.
        Assertions.assertFalse(allocator.trimCurrentThreadCache());

        ByteBuf directBuffer = allocator.directBuffer();

        Assertions.assertTrue(directBuffer.release());

        // Should return true now a cache exists for the calling thread.
        Assertions.assertTrue(allocator.trimCurrentThreadCache());
    }

    @Test
    public void testIOBufferAreDirectWhenUnsafeAvailableOrDirectBuffersPooled() {
        PooledByteBufAllocator allocator = newAllocator(true);
        ByteBuf ioBuffer = allocator.ioBuffer();

        Assertions.assertTrue(ioBuffer.isDirect());
        ioBuffer.release();

        PooledByteBufAllocator unpooledAllocator = newUnpooledAllocator();
        ioBuffer = unpooledAllocator.ioBuffer();

        if (PlatformDependent.hasUnsafe()) {
            Assertions.assertTrue(ioBuffer.isDirect());
        } else {
            Assertions.assertFalse(ioBuffer.isDirect());
        }

        // release io buffer
        ioBuffer.release();
    }

    @Test
    public void testWithoutUseCacheForAllThreads() {
        Assertions.assertFalse(Thread.currentThread() instanceof FastThreadLocalThread);

        PooledByteBufAllocator pool = new PooledByteBufAllocator(
                /* preferDirect=*/ false,
                /* nHeapArena=*/ 1,
                /* nDirectArena*/ 1,
                /* pageSize=*/ 8192,
                /* maxOrder=*/ 9,
                /* tinyCacheSize=*/ 0,
                /* smallCacheSize=*/ 0,
                /* normalCacheSize=*/ 0,
                /* useCacheForAllThreads=*/ false);

        ByteBuf buf = pool.buffer(1);
        buf.release();
    }
}
