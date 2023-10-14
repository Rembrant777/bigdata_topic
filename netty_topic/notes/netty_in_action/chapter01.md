# Netty asynchronous and event driven 
## Terminology 
### Channels 
A Channel is a basic construct of Java NIO. It represents an open connection to an entity such as a hardware defice, 
a file, a network socket, or a program component that is capable of performing one or more distinct I/O operations. 

### Callbacks 
A simple method, a reference to which has been provided to another method. 
This enables the latter to call the former at an appropriate time. 

Callbacks are used in a broad range of programming situations and represent one of the most common ways 
to notify an interested party that an operation has completed. 

Netty uses callbacks internally when handling events. When a callback is triggered the event can be handled
by an implementation of ChannelHandler. 


### Futures 
Future provides another way to notify an application when an operation has completed. 
This object acts as a placeholder for the result of an asynchronous operation; 
it will complete at some point in the future and provide access to the result.

`ChannelFuture` provides additional methods that allows us to register one or more `ChannelFutureListener` instances. 
The listener's callback method, `operationComplete`() is called when the operation has completed. 

And this method can then determine whether the operation completed successfully or with an error. 

If the latter, we can retrieve the `Throwable` that was produced. 

In short, the notication mechanism provided by the `ChannelFutureListener` eliminates the need for manually checking operation completion. 

All of the Netty's outbound I/O operations returns a ChannelFuture; that is, none of them block. 

As we said earlier, Netty is asynchronous and event-driven from the ground up. 

### Event and Handlers 

