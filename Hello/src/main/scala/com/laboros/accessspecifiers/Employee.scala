package com.laboros.accessspecifiers

class Employee {
  
  private[laboros] def printlnHello():Unit = {
    print("Helloworld");
  }
  
  protected def printlnHello2():Unit = {
    print("Helloworld");
  }
  
  private var i = 20;
  private[this] var j = 30;
  
  def compare(other:Employee):Unit = 
  {
    i = i+other.i;
//    j=this.j;
   
  }
}