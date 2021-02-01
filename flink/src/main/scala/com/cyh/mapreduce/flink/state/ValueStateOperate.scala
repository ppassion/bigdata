package com.cyh.mapreduce.flink.state

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

object ValueStateOperate {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //准备数据集
    env.fromCollection(List(
      (1L, 3d),
      (2L, 5d),
      (2L, 1d),
      (1L, 7d),
      (1L, 4d),
      (1L, 2d),
      (1L, 3d)
    ))
      .keyBy(_._1)
      .flatMap(new CountAverageWithValue())
      .print()

    env.execute()
  }
}


class CountAverageWithValue extends RichFlatMapFunction[(Long, Double), (Long, Double)] {
  //定义ValueState类型的变量  (Long, Double):Long就是记录下相同的key出现的次数，Double就是把相同的key的value累加
  private var sum: ValueState[(Long, Double)] = _

  //todo:初始化方法
  override def open(parameters: Configuration): Unit = {
    //初始化获取历史状态的值
    val valueState = new ValueStateDescriptor[(Long, Double)]("average", classOf[(Long, Double)])
    sum = getRuntimeContext.getState(valueState)
  }

  //todo：业务处理方法
  override def flatMap(input: (Long, Double), out: Collector[(Long, Double)]): Unit = {
    // access the state value
    val tmpCurrentSum = sum.value

    // If it hasn't been used before, it will be null
    val currentSum = if (tmpCurrentSum != null) {
      tmpCurrentSum
    } else {
      (0L, 0d)
    }
    // update the count   //key出现的次数      //累加的结果
    val newSum = (currentSum._1 + 1, currentSum._2 + input._2)

    // update the state
    sum.update(newSum)

    // if the count reaches 2, emit the average and clear the state
    if (newSum._1 >= 2) {
      out.collect((input._1, newSum._2 / newSum._1))
      //将状态清除
      sum.clear()
    }
  }
}
