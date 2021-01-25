val list = List(1,2,3,4)

//foreach
list.foreach((x) => { println(x * 2)})
list.foreach(println)

//map
list.map((x:Int)=>x*10)
list.map(_*10)

//flatmap
val list2 = List("hadoop hive spark flink", "hbase spark")
list2.map(x => x.split(" "))
val list3 = list2.map(_.split(" "))
list3.flatten
list2.flatMap(_.split(" "))

//filter
list.filter(_ > 2).map(_ * 10)

//sort
list.sorted.reverse
val list4=List("1 hadoop","3 flink","2 spark")
list4.sortBy(x => x.split(" ")(1).charAt(0))
list4.sortWith((x,y) => {x.split(" ")(0) > y.split(" ")(0)})

//group by
val list5 = List("aa"->"1", "bb"->"2", "cc"->"1")
list5.groupBy(_._2).map(x => x._1 -> x._2.size)

//reduce
list.reduce(_ + _)
