import scala.collection.mutable.{ArrayBuffer, ListBuffer}

//数组
val a = new Array[Int](5)
var b = new ArrayBuffer[String]
b += "aaa"

//元组
val c = (1, "张三", 20)
c._2

//map
var map1 = Map("zhangsan"->30, "lisi"->40)
val map2 = Map(("zhangsan", 30), ("lisi", 30))
map1 += ("wangwu" ->35)
map1.get("zhangsan")
map1.keys
map1.values
for(kv <- map1)
  print(kv)

//set
val a = Set(1,1,2,3,4,5)

//list
val list=ListBuffer(1,2,3,4)