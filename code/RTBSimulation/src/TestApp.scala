
object TestApp extends App {
   
  val labels = Array("NA", "5", "3", "a", "NA")
    
  print(org.apache.spark.PackageWrapper.labelToIndex)
}



package org.apache.spark
{
  object PackageWrapper{

    val labels = Array("NA", "5", "3", "a", "NA")
    
  val labelToIndex: util.collection.OpenHashMap[String, Double] = {
    val n = labels.length
    val map = new util.collection.OpenHashMap[String, Double](n)
    var i = 0
    while (i < n) {
      map.update(labels(i), i)
      i += 1
    }
    map.update("NA",i)
    map
  }


}

}