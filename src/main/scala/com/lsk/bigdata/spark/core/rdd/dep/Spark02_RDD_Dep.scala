package com.lsk.bigdata.spark.core.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Dep {

    def main(args: Array[String]): Unit = {

        val sparConf = new SparkConf().setMaster("local").setAppName("Dep")
        val sc = new SparkContext(sparConf)

        val lines: RDD[String] = sc.textFile("datas/input.txt")
        println(lines.dependencies)
        println("*************************")
        val words: RDD[String] = lines.flatMap(_.split(" "))
        println(words.dependencies)
        println("*************************")
        val wordToOne = words.map(word=>(word,1))
        println(wordToOne.dependencies)
        println("*************************")
        val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)
        println(wordToSum.dependencies)
        println("*************************")
        val array: Array[(String, Int)] = wordToSum.collect()
        array.foreach(println)

//        List(org.apache.spark.OneToOneDependency@36c2b646)
//        *************************
//        List(org.apache.spark.OneToOneDependency@3cd9aa64)
//        *************************
//        List(org.apache.spark.OneToOneDependency@41eb94bc)
//        *************************
//        List(org.apache.spark.ShuffleDependency@57c00115)
//        *************************
//        (scala,1)
//        (spark,1)
//        (hello,4)
//        (world,2)

        sc.stop()

    }
}
