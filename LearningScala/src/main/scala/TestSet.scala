

object TestSet {
  
  def main(args: Array[String]): Unit = {
    
    //Create a set
    
    val s = Set(1,1,1,12,3,3,4,5);
    
    val p1 = new Person("AAA",22);
    val p2 = new Person("AAA",22);
    val p3 = new Person("BBB",22);
    
    val ps  = Set(p1,p2,p3);
    
    println("SIZE OF PS"+ps.size);
    
    println("Size of the set "+s.size)
    
    for(i <- s)
    {
      println(i);
    }
    
    println("Head"+s.head)
    println("tail"+s.tail);
    
    println("Contains 3"+s.contains(3))
    
  }
}