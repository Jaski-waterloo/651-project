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
    .flatMap(line => {
      val tokens = line.split(',')
      tokens
    })
    
    textFile
    .map(line => {
      (line(1), 1)
    })
    .reduceByKey(_+_)
    .saveAsTextFile("numberOfProducts.txt")
    
    textFile
    .map(line => {
      ((line(1),line(7)),1)
    })
    .reduceByKey(_+_)
    .saveAsTextFile("productsOfCompanies.txt")
    
    textFile
    .map(line => {
      var yes = 0
      var no = 0
      var na = 0
      if(line(16) == "Yes") yes = 1
      else if(line(16) == "No") no = 1
      else na = 1
      (line(8), (yes, no, na))
    })
    .reduceByKey((v1,v2) => {
      (v1._1 + v2._1, v1._2 + v2._2, v1._3 + v2._3)
    })
    .saveAsTextFile("StatesConsumerDisputed.txt")
    
    textFile
    .map(line => {
      (line(8), line(12))
    })
    .reduceByKey(_+_)
    .saveAsTextFile("HowSubmitted.txt")
    
    textFile
    .map(line => {
      var yes = 0
      var no = 0
      var na = 0
      if(line(16) == "Yes") yes = 1
      else if(line(16) == "No") no = 1
      else na = 1
      (line(1), (yes, no, na))
    })
    .reduceByKey((v1,v2) => {
      (v1._1 + v2._1, v1._2 + v2._2, v1._3 + v2._3)
    })
    .saveAsTextFile("ProductDispute.txt")
    
    textFile
    .map(line => {
      var yes = 0
      var no = 0
      if(line(15) == "Yes") yes = 1
      else no = 1
      (line(8), (yes, no))
    })
    .reduceByKey((v1,v2) => {
      (v1._1 + v2._1, v1._2 + v2._2)
    })
    .saveAsTextFile("StateTimelyResponse.txt")
  }
}


