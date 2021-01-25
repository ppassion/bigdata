import java.text.SimpleDateFormat
import java.util.Date

object DateUtils {
  // 在object中定义的成员变量，相当于Java中定义一个静态变量
  // 定义一个SimpleDateFormat日期时间格式化对象
  val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")

  // 构造代码 => 执行Object中的main方法时，会运行Object的主构造器 =》它是无参的，{}中的内容
  println("构造代码")

  // 相当于Java中定义一个静态方法
  def format(date:Date) = simpleDateFormat.format(date)

  // main是一个静态方法，所以必须要写在object中
  def main(args: Array[String]): Unit = {
    println { DateUtils.format(new Date()) }
  }

}
