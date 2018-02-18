

import scala.util.Random
import java.util.{ArrayList,HashMap};

class TestImport {
    def printRandom {
        val r = new Random
    }
}

class ImportTests {
    import scala.util.Random
    def printRandom {
        val r = new Random
    }
    
    def printRandom2{
      import scala.util.Random
      val r = new Random
    }
    
}