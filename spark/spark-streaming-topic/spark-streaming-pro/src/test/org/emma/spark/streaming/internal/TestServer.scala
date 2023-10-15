package org.emma.spark.streaming.internal

import org.apache.hadoop.shaded.com.nimbusds.jose.util.StandardCharset
import org.junit.Assert.fail

import java.io.{BufferedWriter, OutputStreamWriter}
import java.net.{ServerSocket, Socket, SocketException}
import java.util.concurrent.{ArrayBlockingQueue, CountDownLatch, TimeUnit}

/**
 * A server implemented for testing cases. TestServer will setup server with random port and
 * passing data as input stream for spark jobs to subscribe and consume
 */
class TestServer(portToBind: Int = 0) extends Logging {
  val queueCapacity: Int = 100
  val queue = new ArrayBlockingQueue[String](queueCapacity)
  val serverSocket = new ServerSocket(portToBind)

  private val startLatch = new CountDownLatch(1)

  /**
   * method of executeSocketServer executes
   *   1. poll data from queue and wrap data into message sent message to the connected/accepted
   *      socket clients 2. also distinguish the connection times, the first connection is spark
   *      job setup signal dataset sending operaiton only happens after the spark job setup and
   *      get ready to process data. 3.
   */
  def executeSocketServer(
      serverSocket: ServerSocket,
      queue: ArrayBlockingQueue[String],
      startLatch: CountDownLatch): Unit = {
    while (true) {
      logInfo("#executeSocketServer Accepting connections on port " + port)
      val clientSocket = serverSocket.accept()
      if (startLatch.getCount == 1) {
        // just skip this connection, this connection is the signal to let TestServer
        // know that client side (spark job) is just set up
        // not get ready to subscribe data and consume it yet.
        if (!clientSocket.isClosed) {
          clientSocket.close()
        }
        startLatch.countDown()
      } else {
        logInfo("#executeSocketServer new connection coming")
        try {
          clientSocket.setTcpNoDelay(true)
          val outputStream = new BufferedWriter(
            new OutputStreamWriter(clientSocket.getOutputStream, StandardCharset.UTF_8))

          while (clientSocket.isConnected) {
            val msg = queue.poll(100, TimeUnit.MILLISECONDS)
            if (msg != null) {
              outputStream.write(msg)
              outputStream.flush()
              logInfo("Message '" + msg + " ' sent")
            }
          }
        } catch {
          case e: SocketException => logError("#executeSocketServer got exception ", e)
        } finally {
          logInfo("#executeSockeServer connection going to be closed")
          if (!clientSocket.isClosed) {
            clientSocket.close()
          }
        }
      }

    }
  }

  val servingThread = new Thread(() => {
    try {
      // invoke method and send message to accepted connected client(s)
      executeSocketServer(serverSocket, queue, startLatch);
    } catch {
      case ie: InterruptedException =>
    } finally {
      logInfo("Close Server Socket")
      serverSocket.close()
    }
  })

  def start(): Unit = {
    servingThread.start()
    // here we call the WaitFor Start this methods will return after the server socket setup successfully
    if (!waitForStart(100000)) {
      stop()

      // if 10 seconds past the client socket still cannot get access to the server via the specific port
      // it will trigger fail operation to stop the test
      fail("Timeout: TestServer cannot start in 10 seconds")
    }
  }

  /**
   * Method defined for handling wait until the server starts. This method will return true if the
   * server starts in "millis" milliesecionds. Otherwise, return false to indicate it's timeout.
   */
  def waitForStart(millis: Long): Boolean = {
    // we will create a test connection to the server so that we can make sure it has started.
    val socket = new Socket("localhost", port)

    try {
      startLatch.await(millis, TimeUnit.MILLISECONDS)
    } finally {
      if (!socket.isClosed) {
        socket.close()
      }
    }
  }

  def startState(): Boolean = {
    (startLatch.getCount == 0)
  }

  def send(msg: String): Unit = {
    queue.put(msg)
  }
  def stop(): Unit = {
    servingThread.interrupt()
  }
  def port: Int = serverSocket.getLocalPort
}
