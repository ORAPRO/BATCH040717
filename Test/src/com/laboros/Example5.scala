package com.laboros

class Example5 {
  
}

class Counter {
  // Normal private member variable
  private var total = 0
 
  // Object-private member variable
  private[this] var lastAdded = 0
 
  def add(n: Int): Unit = {
    total += n
    lastAdded = n
  }
 
  def copyFrom(other: Counter): Unit = {
    // OK, private member from other instance is accessible
    total = other.total
 
    // ERROR, object-private member from other instance is not accessible
//    lastAdded = other.lastAdded
  }
}