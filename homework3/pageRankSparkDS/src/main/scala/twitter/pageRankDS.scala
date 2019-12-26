package twitter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.HashPartitioner
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame


object pageRank {

	
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: PageRank <file>")
      System.exit(1)
    }

// import for sql and spark session
    val conf = new SparkConf().setAppName("PageRank")
    val sc = new SparkContext(conf)

val sparkSession = SparkSession.builder().config(conf).getOrCreate()

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
// for number of node as 3
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
   
   
//sparkSession.createDataset(textFile).toDF()

var rankDF = rankRDD.toDF("node1","rank")
//val rankDF =rankRDD.toDF("node","rank")
var graphDF =graphRDD.toDF("node1","node2")

graphDF.persist()
//graphDF.createOrReplaceTempView("graphDFView")
//rankDF.createOrReplaceTempView("rankDFView")
for(i <- 1 to 10) {

var joinedDF = graphDF.join(rankDF, "node1").groupBy("node2").agg(sum("rank")).toDF("node1","rank")
var delta = joinedDF.select("rank").filter("node1==0").first.getDouble(0)
val temp = joinedDF.filter(!($"node1" === 0)).union(rankDF.drop("rank").withColumn("rank", lit(0.toDouble))).groupBy("node1").agg(sum("rank").as("rank"))
rankDF = temp.withColumn("rank",when(!($"node1" === 0),temp.col("rank") + delta /(k*k)).otherwise(0.toDouble))

 var sumRank = rankDF.select("rank").rdd.map(_ (0).asInstanceOf[Double]).reduce(_ + _)
  println("********************************************************************************************* "+sumRank)

  scala.tools.nsc.io.File(args(0) + "sum.txt").appendAll("sum is "+ i + " "+ sumRank +"\n")
}


rankDF.filter($"node1" <= 100).coalesce(1).write.format("csv")save(args(1))

  }
}