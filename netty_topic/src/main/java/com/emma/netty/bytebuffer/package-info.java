package com.emma.netty.bytebuffer;

/**
 * In this folder we plan to dive deep into the details and usage of the netty
 * frequently used data structure: ByteBuf which is an advanced data structure based on the
 * JDK 1.8's NIO ByteBuffer.
 *
 * It provides two position pointers to read and write operaitons to manipulate.
 * One is readIndex, the other one is the writeIndex.
 *
 * Most of the codes are referring from the netty ByteBuf source code's
 */