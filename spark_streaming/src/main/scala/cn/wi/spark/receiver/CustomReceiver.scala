package cn.wi.spark.receiver

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver


/**
 * @ProjectName: Spark_Parent 
 * @ClassName: CustomReceiver
 * @Author: xianlawei
 * @Description: 自定义接收器Receiver  从TCP Socket读取数据
 *               需要如下两个参数(hostname port)
 *               表示从哪台机器的哪个端口上读取数据
 * @date: 2019/8/30 16:26
 */
class CustomReceiver(val host: String, val port: Int) extends
  Receiver[String](StorageLevel.MEMORY_AND_DISK) {
  /**
   * 开始接受数据
   */
  override def onStart(): Unit = {
    //TODO 启动线程一直从源端接受数据
    new Thread(
      new Runnable {
        override def run(): Unit = receive()
      }
    ).start()
  }

  /**
   * 停止接受数据
   */
  override def onStop(): Unit = {}

  private def receive() {
    var socket: Socket = null
    var userInput: String = null
    try {
      // Connect to host:port
      socket = new Socket(host, port)

      // 得到字节流 封装到字符流  放到缓冲流中
      val reader = new BufferedReader(
        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))

      userInput = reader.readLine()
      while (!isStopped && userInput != null) {
        // TODO: 将读取的数据进行存储
        store(userInput)

        userInput = reader.readLine()
      }
      reader.close()
      socket.close()

      // Restart in an attempt to connect again when server is active again
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException =>
        // restart if could not connect to server
        restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        // restart if there is any other error
        restart("Error receiving data", t)
    }
  }
}
