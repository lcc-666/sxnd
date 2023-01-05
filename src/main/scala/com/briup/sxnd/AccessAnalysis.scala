package com.briup.sxnd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import java.util.Calendar


object AccessAnalysis {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf =new SparkConf()

    conf.setMaster("local").setAppName("AccessAnalysis")

    var sc= new SparkContext(conf)
    var rdd =sc.textFile("/home/chaoge/Downloads/user_defined.log")

    var allData = rdd.map(mes=>{
      var datas = mes.replace("||",",").split(",")

      val ts =datas(0).replace(".","")

      val calendar =Calendar.getInstance()
      calendar.setTimeInMillis(ts.toLong)
      ((calendar.get(Calendar.DAY_OF_MONTH),
        calendar.get(Calendar.HOUR_OF_DAY)),(datas(17),datas(16),datas(19),(ts.toLong)))

    })
    allData.foreach(println)

  }

}
