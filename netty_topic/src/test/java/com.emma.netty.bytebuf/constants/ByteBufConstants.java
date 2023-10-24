package com.emma.netty.bytebuf.constants;

/**
 * netty inner class defined constants.
 *
 * https://github.com/netty/netty/blob/32a025b4eb2c9fefb389ecd5c781f7cabc6ab139/buffer/src/main/java/io/netty/buffer/AbstractByteBufAllocator.java
 */
public class ByteBufConstants {
    public static final int DEFAULT_INITIAL_CAPACITY = 256;
    public static final int DEFAULT_MAX_CAPACITY = Integer.MAX_VALUE;
    public static final int DEFAULT_MAX_COMPONENTS = 16;
    public static final int CALCULATE_THRESHOLD = 1048576 * 4; // 4 MiB page
}
