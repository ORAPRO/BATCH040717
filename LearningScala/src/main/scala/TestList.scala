object TestList {
  
  def main(args: Array[String]): Unit = {
    println("In TestList");
    
    //Creating a list object
    
    
    val l = Nil;
    
    var l1 = List();
    
    val l2 = List(1,2,3,4,5,6,7);
    
    val numbers = 10 :: (20 :: (30 :: (40 :: Nil)))
    
    println("HEAD"+numbers.head);
    
    println("TAIL:"+numbers.tail);
    
    println(numbers.take(3));
    
    println(l2.size);
    
    val l3:List[Int] = List(4,5,6,7,8);
    
    l2:::l3
    
    println(l2.size);
    
    
    for(j <- l2)
    {
      println(j);
    }
  }
}