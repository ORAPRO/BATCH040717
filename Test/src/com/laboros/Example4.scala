package com.laboros

class Example4 {
  
}

package outside {
  package inside {
    object Messages {
      // Accessible up to package 'inside'
      private[inside] val Insiders = "Hello Friends"
 
      // Accessible up to package 'outside'
      private[outside] val Outsiders = "Hello People"
    }
 
    object InsideGreeter {
      def sayHello(): Unit =
        // Can access both messages
        println(Messages.Insiders + " and " + Messages.Outsiders)
    }
  }
 
  object OutsideGreeter {
    def sayHello(): Unit =
      // Can only access the 'Outsiders' message
      println(inside.Messages.Outsiders)
//      println(inside.Messages.Iniders); -- gives Error
  }
}