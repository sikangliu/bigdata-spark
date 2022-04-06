package com.lsk.bigdata.spark.core.rdd.operator.transform

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_GroupBy_Test {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - groupBy
        val rdd = sc.textFile("datas/apache.log")

        val timeRDD: RDD[(String, Iterable[(String, Int)])] = rdd.map(
            line => {
                val datas = line.split(" ")
                val time = datas(3)
                //time.substring(0, )
                val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
                val date: Date = sdf.parse(time)
                val sdf1 = new SimpleDateFormat("HH")
                val hour: String = sdf1.format(date)
                (hour, 1)
            }
        ).groupBy(_._1)

        timeRDD.map{
            case ( hour, iter ) => {
                (hour, iter.size)
            }
        }.collect.foreach(println)


        sc.stop()

//        (06,366)
//        (20,486)
//        (19,493)
//        (15,496)
//        (00,361)
//        (02,365)
//        (04,355)
//        (22,346)
//        (17,484)
//        (13,475)
//        (11,459)
//        (08,345)
//        (14,498)
//        (21,453)
//        (09,364)
//        (18,478)
//        (16,473)
//        (03,354)
//        (07,357)
//        (12,462)
//        (05,371)
//        (01,360)
//        (10,443)
//        (23,356)

    }
}
