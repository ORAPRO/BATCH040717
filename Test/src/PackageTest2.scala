

class PackageTest2 {
  
}


package bobsrockets { 

    package navigation { 

    package tests { 

        object Test1 { } 

} 

      ///// how to access Test1, Test2, Test3 here? 
    object testPackage{
    
    def main(args:Array[String]):Unit= {
      
      tests.Test1;
      bobsrockets.tests.Test2;
      _root_.tests.Test3;
      
      
    }
    }
    } 

    package tests { 

      object Test2 {} 

} } 

  package tests { 

    object Test3 {} 

} 