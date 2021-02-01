package com.cyh.mapreduce.flink.dataStream

import java.{lang, util}

import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.collector.selector.OutputSelector

object Split {
  def main(args: Array[String]): Unit = {
    //todo: 构建StreamExecutionEnvironment
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)

    //todo: 构建DataStream
    val firstStream: DataStream[String] = environment.fromCollection(Array("hadoop hive","spark flink"))

    //todo: 对数据流进行split切分操作
    val selectStream: SplitStream[String] = firstStream.split(new OutputSelector[String] {
      override def select(out: String): lang.Iterable[String] = {
        var list = new util.ArrayList[String]()
        //todo: 如果包含hello字符串
        if (out.contains("hadoop")) {
          //存放到一个叫做first的stream里面去
          list.add("first")
          //todo: 不包含hello字符串
        }else{
          //否则存放到一个叫做second的stream里面去
          list.add("second")
        }
        list
      }
    })

    //todo: 获取first这个stream
    selectStream.select("first").print("contains hadoop")

    //todo: 获取second这个stream
    selectStream.select("second").print("not contains hadoop")

    //todo: 执行任务
    environment.execute("SplitAndSelect")

  }
}


