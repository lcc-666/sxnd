import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.DriverManager
import java.util.Calendar


object AccessAnalysis {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf =new SparkConf()
    conf.setMaster("local").setAppName("AccessAnalysis")

    val sc = new SparkContext(conf)

//        var rdd =sc.textFile("./src/main/scala/com/briup/sxnd/user_defined.log")
    val rdd = sc.textFile("./src/main/scala/com/briup/sxnd/user_defined_2022-02-15.log")
//      val rdd = sc.textFile(args(0))


    val allData = rdd.map(mes => {
      val datas = mes.replace("||", ",").split(",")

      val ts = datas(0).replace(".", "")

      val calendar = Calendar.getInstance()
      calendar.setTimeInMillis(ts.toLong)
      ((calendar.get(Calendar.DAY_OF_MONTH),

//        calendar.get(Calendar.HOUR_OF_DAY)),(datas(17),datas(16),datas(19),(ts.toLong)))
        calendar.get(Calendar.HOUR_OF_DAY)), (datas(17), datas(15), datas(19), (ts.toLong)))

    })
//    allData.foreach(println)

    val cleanData =allData.filter(x=>{
      x._2._1.contains(".") && !x._2._1.equals("127.0.0.1") && !x._2._2.equals("") && x._2._2.length<20
    })
//    cleanData.foreach(println)

    val tsGroupData = cleanData.groupByKey().cache()
//    tsGroupData.foreach(println)

    val pv = cleanData.count()
//    println(pv)

    val hpv=tsGroupData.mapValues(x=>x.size)
//    hpv.foreach(println)

//    val uv=cleanData.map(x=>{
//      (x._1,x._2._2)
//    }).countByValue()
//    uv.foreach(println)
//
//
//    val ip=cleanData.values.map(x=>x._1).countByValue()
//    ip.foreach(println)

    val uv=cleanData.map(x=>x._2._2).distinct().count()
//    println(uv)

    val puv=tsGroupData.mapValues(x=>{
      val name=x.map(y=>y._2)
      name.iterator.toList.distinct.size
    })

    val ip =cleanData.map(x=>x._2._1).distinct().count()

    val hip =tsGroupData.mapValues(x=>{
      val ip=x.map(y=>y._1)
      ip.iterator.toList.distinct.size
    })
//    hip.foreach(println)

    val cv =cleanData.map{
      case (v1,v2) =>(v1,(v1,v2._3))
    }.values.filter(x=>x._2.contains("shopCar") || x._2.contains("order") ||
      x._2.contains("orderSure") || x._2.contains("confirm")).groupByKey().map{
      case (v1,v2) => (v1,v2.size)
    }
//    cv.foreach(println)

    val only = cleanData.map{
      case (v1,v2) =>((v1,v2._2),(v1,1))
    }.groupByKey().values.filter(x=>x.size==1).flatMap(x=>x).groupByKey().map{
      case (v1,v2) => (v1,v2.size)
    }
//    only.foreach(println)

    val all =cleanData.groupByKey().map{
      case (v1,v2)=>(v1,v2.size)
    }
//    all.foreach(println)

    val icm = only.rightOuterJoin(all).map{
      case (v1,v2) =>(v1,v2._1.getOrElse(0).toDouble/v2._2.toDouble)
    }
//    icm.foreach(println)

//    val userAt=cleanData.map(x=>{
//      (x._2._2,x._2._4)
//    }).groupByKey().mapValues(x=>x.max-x.min)

    val haat=cleanData.map(x=>{
      ((x._1,x._2._2),x._2._4)
    }).groupByKey().mapValues(x=>x.max-x.min)
      .map(x=>(x._1._1,x._2)).groupByKey().mapValues(x=>x.sum/x.size)
//    haat.foreach(println)

    val data = hpv.leftOuterJoin(puv)
      .leftOuterJoin(hip).leftOuterJoin(haat).leftOuterJoin(cv)
      .leftOuterJoin(icm)
      .map{
      case (v1,v2)=>(v1,v2._1._1._1._1._1,v2._1._1._1._1._2.getOrElse(0),v2._1._1._1._2.getOrElse(0)
        ,v2._1._1._2.getOrElse(0),v2._1._2.getOrElse(0),v2._2.getOrElse(0.0))
    }
//    data.foreach(println)

    data.foreachPartition{
      iter=>{
        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://localhost:3306/estore3"
        val user = "briup"
        val passwd = "briup"
        Class.forName(driver)
        val coon =DriverManager.getConnection(url,user,passwd)
        val sql= "insert into ref_data(day,hour,pv,uv,ip,times,cv,icm) values(?,?,?,?,?,?,?,?)"
        val prep = coon.prepareStatement(sql)
        iter.foreach(x=>{
          prep.setInt(1,x._1._1)
          prep.setInt(2,x._1._2)
          prep.setInt(3,x._2)
          prep.setInt(4,x._3)
          prep.setInt(5,x._4)
          prep.setDouble(6,x._5.asInstanceOf[Double])
          prep.setInt(7,x._6)
          prep.setDouble(8,x._7.toDouble)
          prep.execute()
        })
        prep.close()
        coon.close()
      }
    }
  }
}
