//make a Parameter is called call by name

object Callbyname extends App {
  
  def time()= {
    
    System.nanoTime()
  }
  
   def exec(s: => Long) = {
      
      println("t = " +s)
      
    s
  }

  println(exec(time()))

  
}