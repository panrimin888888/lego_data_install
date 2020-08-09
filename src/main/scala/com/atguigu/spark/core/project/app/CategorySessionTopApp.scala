package com.atguigu.spark.core.project.app

import com.atguigu.spark.core.project.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object CategorySessionTopApp {
  def statCategoryTop10Session(sc: SparkContext,
                               categoryCountList: List[CategoryCountInfo],
                               userVisitActionRDD:RDD[UserVisitAction]) = {
    //1.先map出top10品类id
    val cids = categoryCountList.map(_.categoryId.toLong)
    val topCategoryActionRDD = userVisitActionRDD.filter(action => cids.contains(action.click_category_id))
    //2.计算每个平类下的每个session的点击量
    val cidAndSidCount = topCategoryActionRDD
      .map(action => ((action.click_category_id, action.session_id), 1))
      .reduceByKey(_ + _)
      .map {
        case ((cid, sid), count) => (cid, (sid, count))
      }
    val result = cidAndSidCount
      .groupByKey()
      .map {
        case (cid, sidCountIt) => (cid, sidCountIt.toList.sortBy(-_._2).take(10))
      }
    result.collect().foreach(println)
  }
}
