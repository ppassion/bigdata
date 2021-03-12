package ETL

import com.dmp.parentTrait.ProcessData
import com.dmp.tools._
import org.apache.kudu.Schema
import org.apache.kudu.spark.kudu.{KuduContext, _}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

/**
  * Created by laowang
  */
object ProcessRegion_city extends ProcessData{
  val KUDU_MASTER = GlobalConfigUtils.kuduMaster
  val SOURCE_TABLE = GlobalConfigUtils.ODS_PREFIX + DataUtils.NowDate()
  val SINK_TABLE = GlobalConfigUtils.ProcessRegionCity
  val kuduOptions:Map[String , String] = Map(
    "kudu.table" -> SOURCE_TABLE ,
    "kudu.master" -> KUDU_MASTER
  )
  /**
    * 去查询【ODS+当天】 表 ， 统计地域[数量]分布情况
    * */
  override def process(sparkSession: SparkSession, kuduContext: KuduContext): Unit = {
    //1:读取kudu中的数据sourceTable
    val ods: DataFrame = sparkSession.read.options(kuduOptions).kudu
    ods.registerTempTable("ods")
    //2：执行SQL。查询地域[数量]分布情况
    val result: DataFrame = sparkSession.sql(ContantsSQL.region_city_sql)
    //3：数据落地
    //3.1)：构建表的schema
    val schema:Schema = ContantsSchema.ProcessRegionCity
    //3.2): 分区ID
    val partitionID = "provincename"
    //3.3）：将数据落地
    DBUtils.process(kuduContext , result , SINK_TABLE , KUDU_MASTER , schema , partitionID)
  }
}
