package com.lsk.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Req2_HotCategoryTop10SessionAnalysis {

    def main(args: Array[String]): Unit = {

        // TODO : Top10热门品类
        val sparConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
        val sc = new SparkContext(sparConf)

        val actionRDD = sc.textFile("datas/user_visit_action.txt")
        actionRDD.cache()
        val top10Ids: Array[String] = top10Category(actionRDD)

        // 1. 过滤原始数据,保留点击和前10品类ID
        val filterActionRDD = actionRDD.filter(
            action => {
                val datas = action.split("_")
                if ( datas(6) != "-1" ) {
                    top10Ids.contains(datas(6))
                } else {
                    false
                }
            }
        )

        // 2. 根据品类ID和sessionid进行点击量的统计
        val reduceRDD: RDD[((String, String), Int)] = filterActionRDD.map(
            action => {
                val datas = action.split("_")
                ((datas(6), datas(2)), 1)
            }
        ).reduceByKey(_ + _)

        // 3. 将统计的结果进行结构的转换
        //  (（ 品类ID，sessionId ）,sum) => ( 品类ID，（sessionId, sum） )
        val mapRDD = reduceRDD.map{
            case ( (cid, sid), sum ) => {
                ( cid, (sid, sum) )
            }
        }

        // 4. 相同的品类进行分组
        val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()

        // 5. 将分组后的数据进行点击量的排序，取前10名
        val resultRDD = groupRDD.mapValues(
            iter => {
                iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
            }
        )

        resultRDD.collect().foreach(println)


        sc.stop()
    }
    def top10Category(actionRDD:RDD[String]) = {
        val flatRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(
            action => {
                val datas = action.split("_")
                if (datas(6) != "-1") {
                    // 点击的场合
                    List((datas(6), (1, 0, 0)))
                } else if (datas(8) != "null") {
                    // 下单的场合
                    val ids = datas(8).split(",")
                    ids.map(id => (id, (0, 1, 0)))
                } else if (datas(10) != "null") {
                    // 支付的场合
                    val ids = datas(10).split(",")
                    ids.map(id => (id, (0, 0, 1)))
                } else {
                    Nil
                }
            }
        )

        val analysisRDD = flatRDD.reduceByKey(
            (t1, t2) => {
                ( t1._1+t2._1, t1._2 + t2._2, t1._3 + t2._3 )
            }
        )

        analysisRDD.sortBy(_._2, false).take(10).map(_._1)
    }
}


//(20,List((199f8e1d-db1a-4174-b0c2-ef095aaef3ee,8), (7eacf77a-c019-4072-8e09-840e5cca6569,8), (22e78a14-c5eb-45fe-a67d-2ce538814d98,7), (07b5fb82-da25-4968-9fd8-47485f4cf61e,7), (cde33446-095b-433c-927b-263ba7cd102a,7), (85157915-aa25-4a8d-8ca0-9da1ee67fa70,7), (215bdee7-db27-458d-80f4-9088d2361a2e,7), (5e3545a0-1521-4ad6-91fe-e792c20c46da,7), (ab27e376-3405-46e2-82cb-e247bf2a16fb,7), (d500c602-55db-4eb7-a343-3540c3ec7a36,7)))
//(19,List((fde62452-7c09-4733-9655-5bd3fb705813,9), (85157915-aa25-4a8d-8ca0-9da1ee67fa70,9), (d4c2b45d-7fa1-4eff-8473-42cecdaffd62,9), (329b966c-d61b-46ad-949a-7e37142d384a,8), (1b5e5ce7-cd04-4e78-9a6f-1c3dbb29ce39,8), (4d93913f-a892-490d-aa58-3a74b9099e29,7), (22e78a14-c5eb-45fe-a67d-2ce538814d98,7), (46e6b58a-ad5b-4330-9e67-bc2651f9c5e2,7), (5e3545a0-1521-4ad6-91fe-e792c20c46da,7), (b61734fd-b10d-456d-b189-2e3fe0adf31d,7)))
//(15,List((632972a4-f811-4000-b920-dc12ea803a41,10), (9fa653ec-5a22-4938-83c5-21521d083cd0,8), (66a421b0-839d-49ae-a386-5fa3ed75226f,8), (5e3545a0-1521-4ad6-91fe-e792c20c46da,8), (f34878b8-1784-4d81-a4d1-0c93ce53e942,8), (524378b7-e676-43e6-a566-a5493a9058d4,7), (e306c00b-a6c5-44c2-9c77-15e919340324,7), (89278c1a-1e33-45aa-a4b7-33223e37e9df,7), (394490ea-4e99-4b83-a79a-a97aaee5b021,7), (a9b9806c-90fb-4df1-94c6-d0fb12425fa3,7)))
//(2,List((66c96daa-0525-4e1b-ba55-d38a4b462b97,11), (b4589b16-fb45-4241-a576-28f77c6e4b96,11), (f34878b8-1784-4d81-a4d1-0c93ce53e942,10), (25fdfa71-c85d-4c28-a889-6df726f08ffb,9), (ab27e376-3405-46e2-82cb-e247bf2a16fb,8), (0b17692b-d603-479e-a031-c5001ab9009e,8), (39cd210e-9d54-4315-80bf-bed004996861,8), (213bc2d5-be6b-49a3-9cb6-f9afc5b69b3d,8), (f666d6ba-b3e8-45b1-a269-c5d6c08413c3,8), (eb7e9f6f-e0aa-4b10-b13b-75746150b8eb,7)))
//(17,List((4509c42c-3aa3-4d28-84c6-5ed27bbf2444,12), (bf390289-5c9d-4037-88b3-fdf386b3acd5,8), (1b5ac69b-5e00-4ff3-8a5c-6822e92ecc0c,8), (0416a1f7-350f-4ea9-9603-a05f8cfa0838,8), (9bdc044f-8593-49fc-bbf0-14c28f901d42,8), (dd3704d5-a2f9-40c1-b491-87d24bbddbdb,8), (ab16e1e4-b3fc-4d43-9c95-3d49ec26d59c,7), (fde62452-7c09-4733-9655-5bd3fb705813,7), (abbf9c96-eca3-4ecf-9c44-04193eb4b562,7), (329b966c-d61b-46ad-949a-7e37142d384a,7)))
//(13,List((329b966c-d61b-46ad-949a-7e37142d384a,8), (f736ee4a-cc14-4aa9-9a96-a98b0ad7cc3d,8), (1fb79ba2-4780-4652-9574-b1577c7112db,7), (0f227059-7006-419c-87b0-b2057b94505b,7), (632972a4-f811-4000-b920-dc12ea803a41,7), (7eacce38-ffbc-4f9c-a3ee-b1711f8927b0,7), (c0f70b31-fc3b-4908-af97-9b4936340367,7), (1b5e5ce7-cd04-4e78-9a6f-1c3dbb29ce39,7), (1b5ac69b-5e00-4ff3-8a5c-6822e92ecc0c,6), (a1cef5b4-9451-480f-9c9a-a52c9404a51a,6)))
//(11,List((329b966c-d61b-46ad-949a-7e37142d384a,12), (99f48b83-8f85-4bea-8506-c78cfe5a2136,7), (4509c42c-3aa3-4d28-84c6-5ed27bbf2444,7), (dc226249-ce13-442c-b6e4-bfc84649fff6,7), (2cd89b09-bae3-49b5-a422-9f9e0c12a040,7), (df9f26b6-385a-4ba3-ad75-6d3bd9f95c88,6), (e7f9a91d-ff65-4b5f-9488-2a3195e1d0c6,6), (5d2f3efb-be1c-4ee2-8fd5-545fd049e70c,6), (8fb4a0a9-e128-4d9a-8e11-fd4a9fd22474,6), (45e35ffa-f0e0-400e-a252-5605b4089625,6)))
//(7,List((a41bc6ea-b3e3-47ce-98af-48169da7c91b,9), (4dbd319c-3f44-48c9-9a71-a917f1d922c1,7), (9fa653ec-5a22-4938-83c5-21521d083cd0,7), (aef6615d-4c71-4d39-8062-9d5d778e55f1,7), (2d4b9c3e-2a9e-41b6-9573-9fde3533ed89,7), (95cb71b8-7033-448f-a4db-ae9861dd996b,7), (f34878b8-1784-4d81-a4d1-0c93ce53e942,7), (1eb7b1af-6532-4b39-ac36-c76b2611b1f1,6), (71f1c966-11e4-450f-81d2-c0b334710ccc,6), (329b966c-d61b-46ad-949a-7e37142d384a,6)))
//(9,List((199f8e1d-db1a-4174-b0c2-ef095aaef3ee,9), (329b966c-d61b-46ad-949a-7e37142d384a,8), (5e3545a0-1521-4ad6-91fe-e792c20c46da,8), (66c96daa-0525-4e1b-ba55-d38a4b462b97,7), (e306c00b-a6c5-44c2-9c77-15e919340324,7), (f205fd4f-f312-46d2-a850-26a16ac2734c,7), (4f0261cc-2cb1-40e0-9ffb-5587920c1084,7), (cbdbd1a4-7760-4195-bfba-fa44492bf906,7), (1caec16d-6af8-49a5-b5f1-d7f377f98a4e,7), (8a0f8fe1-d0f4-4687-aff3-7ce37c52ab71,7)))
//(12,List((a4b05ea2-2869-4f20-a82a-86352aa60e9f,8), (b4589b16-fb45-4241-a576-28f77c6e4b96,8), (22a687a0-07c9-4e84-adff-49dfc4fe96df,8), (73203aee-de2e-443e-93cb-014e38c0d30c,8), (64285623-54ad-4a1f-ae84-d8f85ebf94c6,7), (4c90a8a8-91d0-4888-908c-95dad1c5194e,7), (a735881e-4c30-4ddc-a1d9-ef2069d5fb5b,7), (89278c1a-1e33-45aa-a4b7-33223e37e9df,7), (ab16e1e4-b3fc-4d43-9c95-3d49ec26d59c,7), (c32dc073-4454-4fcd-bf55-fbfcc8e650f3,7)))
