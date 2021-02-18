package com.cyh.flinkProject.isSpider.dataProcess.businessProcess

import com.cyh.flinkProject.isSpider.common.bean.{CoreRequestParams, ProcessedData, QueryRequestData, RequestType}
import com.cyh.flinkProject.isSpider.dataProcess.constants.TravelTypeEnum.TravelTypeEnum
import com.cyh.flinkProject.isSpider.dataProcess.constants.{BehaviorTypeEnum, FlightTypeEnum, TravelTypeEnum}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream

/**
  * 对数据进行切分，封装成样例类ProcessData
  */
object QueryDataPackage {

  /**
    * 数据数据进行分割，封装样例类ProcessData
    * @param sourceData
    * @return
    */
    def queryDataLoadAndPackage(sourceData: DataStream[String]): DataStream[ProcessedData] = {

    //将数据进行 map，一条条处理
      sourceData.map { sourceLine =>
        //分割数据
        val dataArray = sourceLine.split("#CS#", -1)
        val requestMethod = dataArray(0)
        val request = dataArray(1)
        val remoteAddr = dataArray(2)
        val httpUserAgent = dataArray(3)
        val timeIso8601 = dataArray(4)
        val serverAddr = dataArray(5)
        val isBlackFlag: Boolean = dataArray(6).equalsIgnoreCase("true")
        val requestType: RequestType = RequestType(FlightTypeEnum.withName(dataArray(7)), BehaviorTypeEnum.withName(dataArray(8)))
        val travelType: TravelTypeEnum = TravelTypeEnum.withName(dataArray(9))
        val requestParams: CoreRequestParams = CoreRequestParams(dataArray(10), dataArray(11), dataArray(12))
        val cookieValue_JSESSIONID: String = dataArray(13)
        val cookieValue_USERID: String = dataArray(14)
        //封装 query 数据
        val mapper = new ObjectMapper
        mapper.registerModule(DefaultScalaModule)
        val queryRequestData = mapper.readValue(dataArray(15), classOf[QueryRequestData])

        //分析查询请求的时候不需要 book 数据
        val bookRequestData=null

        //refer来源
        val httpReferrer = dataArray(17)
        //封装流程数据，返回
        ProcessedData(requestMethod, request, remoteAddr, httpUserAgent, timeIso8601, serverAddr, isBlackFlag, requestType, travelType, requestParams, cookieValue_JSESSIONID, cookieValue_USERID, queryRequestData, bookRequestData, httpReferrer)
      }
    }
}
