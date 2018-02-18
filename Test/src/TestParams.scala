

object TestParams {
  
  def main(args: Array[String]): Unit = {
    printName(lastName="lName",firstName="fname");
  }
  
  
  def testVarArgs(input:String*) =
  {
    println(input(0));
  }
  
  def printName(firstName:String, lastName:String)
  {
    println(firstName+""+lastName);
  }
  
}