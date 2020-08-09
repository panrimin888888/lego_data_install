package com.atguigu.spark.core.project.app

import com.atguigu.spark.core.project.bean.UserVisitAction
import org.apache.spark.{SparkConf, SparkContext}

object ProjectApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("ProjectApp")
    val sc = new SparkContext(conf)
    //1.读数据
    val sourceRDD = sc.textFile("e:/user_visit_action.txt")
    //2.封装到样例类
    val userVisitActionRDD = sourceRDD.map(line =>{
      val splits = line.split("_")
      UserVisitAction(
        splits(0),
        splits(1).toLong,
        splits(2),
        splits(3).toLong,
        splits(4),
        splits(5),
        splits(6).toLong,
        splits(7).toLong,
        splits(8),
        splits(9),
        splits(10),
        splits(11),
        splits(12).toLong)
    })
    //userVisitActionRDD.collect().foreach(println)
    val categoryCountList = CategoryTopAPP.calcCategoryTop10(sc,userVisitActionRDD)
    //categoryCountList.foreach(println)
    //需求二
    CategorySessionTopApp.statCategoryTop10Session(sc,categoryCountList,userVisitActionRDD)
    sc.stop()
  }

}
