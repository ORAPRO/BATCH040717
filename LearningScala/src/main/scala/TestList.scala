

object TestList {
  
  def main(args: Array[String]): Unit = {
    println("In TestList");
    
    //Creating a list object
    
    
    val l = Nil;
    
    var l1 = scala.collection.mutable.LinkedList;
    
    var l2 = scala.collection.mutable.LinkedList(1,2,3,4,5,6,7);
    
    println(l2.toString())
    
    val numbers = 10 :: (20 :: (30 :: (40 :: Nil)))
    
    println("HEAD"+numbers.head);
    
    println("TAIL:"+numbers.tail);
    
    println(numbers.take(3));  
    
    println(numbers(0));
    
    println("L2 SIZE:"+l2.size);
    
    val l3=scala.collection.mutable.LinkedList(4,5,6,7,8);
    
    l2=l2.:+(33)
    println(l2.toString())
    
    println("L2 SIZE:"+l2.size);
    
    
    
    
    for(j <- l2)
    {
      println(j);
    }
  }
}