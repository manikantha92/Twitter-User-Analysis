package twitter


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark
import org.apache.spark.api.java.JavaRDD.fromRDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SparkSession
import org.apache.spark.HashPartitioner
import sun.security.provider.certpath.AdjacencyList

import scala.tools.util.PathResolver.MkLines



object followerCountMain extends App {

  def extractVertices(sourceVertex: Int, vertex: Int, adjacencyList: List[Int], vertexDistances: Double): List[(Int, Double)] = {

    var newDistances : List[(Int, Double)] = List()
    newDistances :+= (vertex, vertexDistances)
    adjacencyList.map(x => if(x == sourceVertex) newDistances :+= (x,0.0) else newDistances :+= (x, vertexDistances + 1.0))
    return newDistances
  }

 override def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\ntwitter.followerCountMain <input dir> <output dir>")

      System.exit(1)
    }


    val conf = new SparkConf().setAppName("Follower Count").setMaster("local")
    val sc = new SparkContext(conf)
 val sparkSession = SparkSession.builder().config(conf).getOrCreate()

// split the line based on coma, used map on all the line and got the second word to the count

   var hp = new HashPartitioner(4)
    val textFile = sc.textFile(args(0))
   val sourceVertex = 1
    val graph = textFile.map(line => line.split(","))
     .map(x => (Integer.parseInt(x(0)), Integer.parseInt(x(1)))).groupByKey().mapValues(_.toList).persist()  //(x=>x._1).mapValues(_.map(_._2).mkString("(",",",")"))

    val vertexId = graph.map(x=> x._1.toInt)
    val adjacencyList = graph.map(x=> x._2)
   var distances = graph.map({case (vertexId, adjacencyList)=> if (vertexId == sourceVertex) (vertexId, 0.toDouble) else (vertexId, Double.PositiveInfinity)}).partitionBy(hp)

   var countConvergence = 0
   var flag = 1

   while(flag != 0) {

     val tempDistance = graph.rightOuterJoin(distances).flatMap({ case (vertice, (adjacencyList, vertexDistances)) =>
     extractVertices(sourceVertex,vertice, adjacencyList.getOrElse(List.empty), vertexDistances )}).partitionBy(hp).reduceByKey(math.min)

     val newDistances = tempDistance.join(distances).partitionBy(hp).collect{
       case (key, (source1, destination1)) if source1 != destination1 => (key, source1)

     }
     var newDistancesCount = newDistances.count()

     if(newDistancesCount == 0) {
       flag = 0
     }
     else {

       distances = tempDistance
     }
   }
   vertexId.collect().foreach(println)
   adjacencyList.collect().foreach(println)
 //distances = graph.mapValues()
   distances.collect().foreach(println)
  }
}