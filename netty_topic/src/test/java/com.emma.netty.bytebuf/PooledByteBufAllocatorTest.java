package com.emma.netty.bytebuf;

import com.emma.netty.bytebuf.constants.ByteBufConstants;
import io.netty.buffer.PooledByteBufAllocator;

public class PooledByteBufAllocatorTest extends AbstractByteBufAllocatorTest<PooledByteBufAllocator> {
    @Override
    protected PooledByteBufAllocator newAllocator(boolean preferDirect) {
        return new PooledByteBufAllocator(preferDirect);
    }


}
