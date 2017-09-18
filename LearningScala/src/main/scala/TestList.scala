object TestList {
  
  def main(args: Array[String]): Unit = {
    println("In TestList");
    
    //Creating a list object
    
    val l = Nil;
    
    var l1 = List();
    
    var l2 = List(1,2,3,4,5,6,7);
    
    println(l2.size);
    
    for(j <- l2)
    {
      println(j);
    }
  }
}