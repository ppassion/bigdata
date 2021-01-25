//val不可变 var可变
val a:Int = 10
var b:Int = 20
//a = 30
b = 30

//弱类型
val c = "scala"
val d = 1.2
print(c)
print(d)

//返回值取并集
val z = if(d>1) 1 else "error"

//重载
1.+(2)

//循环
val nums = 1 to 10
for(i <- nums if i >5) println(i)

//循环 推导式
val v = for(i <- 1 to 5) yield i * 10

//函数1
val func1 = (x:Int,y:Int) => {
  x + y
}
func1(1,2)

//函数2
val func2:(Int,Int) => Int = {
  (x,y) => (x + y)
}
func2(1,2)