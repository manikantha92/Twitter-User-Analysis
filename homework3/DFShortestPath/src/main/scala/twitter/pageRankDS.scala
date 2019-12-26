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


object dfShortestPath {

 def main(args: Array[String]): Unit = {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\ntwitter.followerCountMain <input dir> <output dir>")

      System.exit(1)
    }

    val spark: SparkSession = SparkSession.builder().appName("DF shortest patha").config("spark.master", "local").getOrCreate()
    // val conf = new SparkConf().setAppName("DF shortest path").setMaster("local")
    val sc = spark.sparkContext
    //val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._
    //import sparkSession.implicits._

    val graph = spark.read.format("csv").load(args(0)).toDF("vertex1","vertex2")

    val sourceVertex = 1

    val hp = new HashPartitioner(4)

    var distances = graph.withColumn("distance", when(($"vertex1"=== sourceVertex), 0.0).otherwise(Double.PositiveInfinity)).withColumnRenamed("vertex1","vertex")
      .select("vertex","distance")

    graph.createOrReplaceTempView("GraphView")

    var countConvergence = 0
    var flag = 1

    while(flag != 0) {

      distances.createOrReplaceTempView("DistancesView")
      val tempDistance = spark.sql("select g.vertex2 as vertex, d.distance + 1.0 as dist from GraphView g, DistancesView d where g.vertex1 = d.vertex union select * from DistancesView")

      tempDistance.createOrReplaceTempView("TempDistanceView")

      var currentDistance = spark.sql("select vertex, min(dist) as distance from TempDistanceView group by vertex")
      var difference = currentDistance.select('vertex as "currentVertex", 'distance as "currentDistance")
        .join(distances.select('vertex as "dCurrentvertex",'distance as "dcurrentDistance"), (('currentvertex === 'dcurrentvertex) && !('currentdistance === 'dcurrentdistance))).count()

      if(difference > 0) {
        flag = 1
      }
      else {

        flag = 0
      }

      distances = currentDistance
      countConvergence += 1
      }



    distances.show()

  }
}