package com.lsk.bigdata.spark.core.rdd.dep

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark01_RDD_Dep {

    def main(args: Array[String]): Unit = {

        val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparConf)

        val lines: RDD[String] = sc.textFile("datas/input.txt")
        println(lines.toDebugString)
        println("*************************")
        val words: RDD[String] = lines.flatMap(_.split(" "))
        println(words.toDebugString)
        println("*************************")
        val wordToOne = words.map(word=>(word,1))
        println(wordToOne.toDebugString)
        println("*************************")
        val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)
        println(wordToSum.toDebugString)
        println("*************************")
        val array: Array[(String, Int)] = wordToSum.collect()
        array.foreach(println)

//        (1) datas/input.txt MapPartitionsRDD[1] at textFile at Spark01_RDD_Dep.scala:13 []
//        |  datas/input.txt HadoopRDD[0] at textFile at Spark01_RDD_Dep.scala:13 []
//        *************************
//        (1) MapPartitionsRDD[2] at flatMap at Spark01_RDD_Dep.scala:16 []
//        |  datas/input.txt MapPartitionsRDD[1] at textFile at Spark01_RDD_Dep.scala:13 []
//        |  datas/input.txt HadoopRDD[0] at textFile at Spark01_RDD_Dep.scala:13 []
//        *************************
//        (1) MapPartitionsRDD[3] at map at Spark01_RDD_Dep.scala:19 []
//        |  MapPartitionsRDD[2] at flatMap at Spark01_RDD_Dep.scala:16 []
//        |  datas/input.txt MapPartitionsRDD[1] at textFile at Spark01_RDD_Dep.scala:13 []
//        |  datas/input.txt HadoopRDD[0] at textFile at Spark01_RDD_Dep.scala:13 []
//        *************************
//        (1) ShuffledRDD[4] at reduceByKey at Spark01_RDD_Dep.scala:22 []
//        +-(1) MapPartitionsRDD[3] at map at Spark01_RDD_Dep.scala:19 []
//        |  MapPartitionsRDD[2] at flatMap at Spark01_RDD_Dep.scala:16 []
//        |  datas/input.txt MapPartitionsRDD[1] at textFile at Spark01_RDD_Dep.scala:13 []
//        |  datas/input.txt HadoopRDD[0] at textFile at Spark01_RDD_Dep.scala:13 []
//        *************************
//        (scala,1)
//        (spark,1)
//        (hello,4)
//        (world,2)


        sc.stop()

    }
}
