# Chapter 5 ByteBuf 

Netty's alternative to ByteBuffer is ByteBuf, a powerful implementation that addresses the limitation
of the JDK API and provides a better API for network application developers. 

Netty's API for data handling is exposed through two components -- abstract class `ByteBuf` and interface `ByteBufHolder`. 

Advantages of the `ByteBuf` API: 
1. extensible to user-defined buffer types. 
2. transparent zero-copy is achieved by a built-in composite buffer type.
3. capacity can be expanded on demand (as with the JDK StringBuilder).
4. Switching between reader and writer modes doesn't require calling `ByteBuffer#flip()` method. 
5. reading and writing employ distinct indices
6. method chaining is supported
7. reference counting is supported 
8. pooling is supported. 