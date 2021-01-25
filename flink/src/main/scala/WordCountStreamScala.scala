import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object WordCountStreamScala {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val sourceStream:DataStream[String] = env.socketTextStream("node01",9999)

    val result:DataStream[(String,Int)] = sourceStream.flatMap(x => x.split(" ")).map(x => (x,1)).keyBy(0).sum(1)

    result.print()

    env.execute("WordCountStreamScala")
  }
}