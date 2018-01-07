

object Test {
  
  def test2():Unit = {
    test1(10);
  }
  
  
  def test1(a:Int):Unit = {
    var a =10;
    println(a);
    
    import java.util.ArrayList;
    val l = new ArrayList[Int];
    l.add(10);
    l.add(20);

    println(l);

    
  }
}