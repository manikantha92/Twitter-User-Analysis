package twitter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.HashPartitioner

object pageRank {

	def addDelta(edge: (Int, Double), delta: Double) : (Int, Double) = {

	if(edge._1 == 0) {

return (edge._1, 0.0)
}
else {return (edge._1, edge._2 + delta)}}
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: PageRank <file>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("PageRank")
    val sc = new SparkContext(conf)

    val k = 100

// creating list of nodes
    val nums = List(1 to k*k:_*)

// appending dummy rank to all nodes
    val dummy = for( a <- nums if a % k == 0 ) yield (a,0)

// creating pairs of edges with dangling nodes and appending dummy ranks as well
    val edges = for(x <- nums) yield (x, x+1)
    val graph = edges.zipWithIndex
    .filter { case (_, i) => (i + 1) % k != 0 }
    .map { case (e, _) => e } ++ dummy

   val zero = List((0,0.0))
   val ranks = for(x <- nums) yield (x,1/(k*k).toDouble) 

// appending dummy node with dummy rank
   val zRanks = ranks ++ zero

// creating RDD for ranks
   var rankRDD = sc.parallelize(zRanks).map(x=> (x._1, x._2.asInstanceOf[Double]))


// creating RDD for nodes and edges
   var graphRDD = sc.parallelize(graph).partitionBy(new HashPartitioner(4))

for(i <- 1 to 10) {	
   var graphRankJoin =  graphRDD.join(rankRDD)
   val temp = graphRankJoin.map({ case (x,y) => y}).groupByKey().map(x=> (x._1,x._2.sum))
  

   var delta = temp.lookup(0)(0)

   rankRDD = rankRDD.mapValues(x=> (x-x))
   var temp2 = rankRDD union temp
   temp2 = temp2.reduceByKey(_ + _)
   rankRDD = temp2.map(x => addDelta(x, (delta/(k*k).toDouble)))
  var sum = rankRDD.map({case (x,y) => y}).reduce((x,y) => x + y)
 scala.tools.nsc.io.File(args(1) +"sum.txt").appendAll("sum is " + i +" is " +sum + "\n")


	

//println("*********************************************************************** "+delta)    
}
  //val sum = rankRDD.map({case (x,y) => y}).reduce((x,y) => x + y)
println(rankRDD.toDebugString)
  rankRDD.filter({case (key, value) => key  <= 100}).coalesce(1).write.format("csv")save(args(1))
  //println("*********************************************************************** "+sum) 
  }
}