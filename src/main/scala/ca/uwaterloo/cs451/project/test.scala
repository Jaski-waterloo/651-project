package ca.uwaterloo.cs451.project


import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class testConf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output)
  val input = opt[String](descr = "input path", required = false)
  val output = opt[String](descr = "output path", required = false)
//   val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
//   val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
//   val executorCores = opt[Int](descr = "number of cores", required = false, default = Some(1))
//   val threshold = opt[Int](descr = "threshold", required = false, default = Some(10))
  verify()
}

object test extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new testConf(argv)

//     log.info("Input: " + args.input())
//     log.info("Output: " + args.output())
//     log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("test")
    val sc = new SparkContext(conf)

//     val outputDir = new Path(args.output())
//     FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
    
//     SCENE_ID,PRODUCT_ID,SPACECRAFT_ID,SENSOR_ID,DATE_ACQUIRED,
//     COLLECTION_NUMBER,COLLECTION_CATEGORY,SENSING_TIME,DATA_TYPE,
//     WRS_PATH,WRS_ROW,CLOUD_COVER,NORTH_LAT,SOUTH_LAT,
//     WEST_LON,EAST_LON,TOTAL_SIZE,BASE_URL
    val textFile = sc.textFile("customer_data.csv")
    .take(5)
    .map(line => {
      val tokens = line.split(',')
      (tokens(5), tokens(6), tokens(8), tokens(9), tokens(10))
    })
    .foreach(line => {
      println(line)
    })
  }
}


