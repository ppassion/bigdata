class Student(val name:String, val age:Int) {
  println("student 主构造器")

  val address:String="beijing"
  // 定义一个参数的辅助构造器
  def this(name:String) {
    // 第一行必须调用主构造器、其他辅助构造器或者super父类的构造器
    this(name, 20)
  }

  def this(age:Int) {
    this("某某某", age)
  }
}

object MyDemo {
  def main(args: Array[String]): Unit = {
    //val zs = new Student("zs", 10)
    val zs2 = new Student(name = "ddd")
  }
}
