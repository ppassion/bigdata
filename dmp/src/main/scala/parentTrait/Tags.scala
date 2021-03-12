package parentTrait

/**
  * Created by laowang
  */
trait Tags {
  /**
    * 打标签的方法
    * @param args 传入的标签（标签个数不确定）
    * */
  def makeTags(args:Any*):Map[String , Double]
}
