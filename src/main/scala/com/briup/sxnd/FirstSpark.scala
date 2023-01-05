package com.briup.sxnd

import org.apache.spark.{SPARK_BRANCH, SparkConf, SparkContext}

object FirstSpark {
  def main(args: Array[String]): Unit = {
    if(args.length<2){
      println("请输入需要进行")
      System.exit(0)
    }
    var conf =new SparkConf()
    conf.setMaster("local")
    conf.setAppName("diyige")

    var sc= new SparkContext(conf)
    var rdd=sc.textFile(args(0))
    var mapRdd=rdd.flatMap(_.split(" "))
    var word =mapRdd.map(x=>(x,1))
    var wordSum = word.reduceByKey(_+_)
    wordSum.foreach(println)
    wordSum.saveAsTextFile(args(1))
    sc.stop()

  }

}
