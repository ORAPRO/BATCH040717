

object TestFunc extends App{

  def f2(i:Int,name:String):String = {
    println("In function 2")
    i+name;
  }
  
  def f3(i:Int,name:String) = i+name;
  
   f2(1,"aaa");
  println(f3(2,"bbb"))
}