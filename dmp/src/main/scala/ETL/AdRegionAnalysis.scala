package ETL

import com.dmp.parentTrait.ProcessData
import com.dmp.tools._
import org.apache.kudu.Schema
import org.apache.kudu.spark.kudu.{KuduContext, _}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

object AdRegionAnalysis extends ProcessData{
  val KUDU_MASTER = GlobalConfigUtils.kuduMaster
  val SOURCE_TABLE = GlobalConfigUtils.ODS_PREFIX + DataUtils.NowDate()
  val SINK_TABLE = GlobalConfigUtils.AdRegionAnalysis
  val kuduOptions:Map[String , String] = Map(
    "kudu.table" -> SOURCE_TABLE ,
    "kudu.master" -> KUDU_MASTER
  )

  override def process(sparkSession: SparkSession, kuduContext: KuduContext): Unit = {
    //1 ：将ods数据从kudu中取出
    val data: DataFrame = sparkSession.read.options(kuduOptions).kudu
    data.registerTempTable("ods")
    //2：做报表
    val sqlTemp = sparkSession.sql(ContantsSQL.adRegionAnalysis_tmp)
    sqlTemp.registerTempTable("regionTemp")
    val result: DataFrame = sparkSession.sql(ContantsSQL.adRegionAnalysis)

    //3：定义schema
    val schema:Schema = ContantsSchema.AdRegionAnalysis
    val partitionID = "provincename"
    //4：数据落地
    DBUtils.process(kuduContext , result , SINK_TABLE , KUDU_MASTER , schema , partitionID)

  }
}
