

class PackageTest {
  
}

// package bobsrockets.navigation 
//
//  class Navigator { } 

  // Namespace-like package 

//  package bobsrockets.navigation { 
//
//    class Navigator {
//      
//    } 
//
//  } 

  // Nested namespace-like package 

//  package bobsrockets { 
//
//    package navigation { 
//
//      class Navigator {  } 
//
//} } 

package bobsrockets { 

  package navigation { 

    class Navigator { } 

  } 

  package tests { 

    object NavigatorTest { 

      val x = new navigation.Navigator 

} 

} } 