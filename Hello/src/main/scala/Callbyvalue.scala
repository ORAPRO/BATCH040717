
//how to a pass a parameter to a method "normally" is called call by value
object Callbyvalue 
   extends App {

  def time() : Long = {
      
      System.nanoTime
  }

  def exec(t: Long): Long = {
     
      println("t = " + t)
      
      t
  }

  println(exec(time()))

}
