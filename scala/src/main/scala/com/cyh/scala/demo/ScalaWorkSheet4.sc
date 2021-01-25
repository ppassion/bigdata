//柯里化
def add(x: Int, y: Int): Int = x + y
add(2,3)

def add2(x:Int) = (y:Int) => x + y
add2(1)(3)

val arr1 = Array("aaa","bbb")
val arr2 = Array("AAA","BBB")
arr1.corresponds(arr2)(_.equalsIgnoreCase(_))

