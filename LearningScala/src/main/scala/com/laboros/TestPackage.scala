package com.laboros //java style packaging

class TestPackage {
  
}

package com.laboros { //C# kind packaging
  class TestPackage1{
    
  }
}

package com{
  package laboros {
    class TestPackage2
    {
      
    }
    package Test{ //Can put into multiple packages
      object TestPackage2Test
      {
        val t = new laboros.TestPackage2(); // Direct access to TestPackage2 without com
      }
    }
  }
}

package bobsrockets {
    package navigation {
      package tests {
        object Test1 { def m = 2; }
      }
      
      class TestPackage{
        
        def m = {
          tests.Test1;
          bobsrockets.tests.Test2;
          _root_.com.laboros.tests.Test3;  // Every package is under _root_
        }
      }
      ///// how to access Test1, Test2, Test3 here?
//      Test1();
      
     
}

package tests {
      object Test2 {  }
} 
}
  package tests {
    object Test3 {  }
}