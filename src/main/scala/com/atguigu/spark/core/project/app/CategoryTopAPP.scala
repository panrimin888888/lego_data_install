package com.atguigu.spark.core.project.app

import com.atguigu.spark.core.project.acc.CategoryAcc
import com.atguigu.spark.core.project.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object CategoryTopAPP {
  def calcCategoryTop10(sc: SparkContext,actionRDD: RDD[UserVisitAction]): List[CategoryCountInfo] = {
    //1.创建累加器对象
    val acc = new CategoryAcc
    //2.注册累加器
    sc.register(acc,"CategoryAcc")
    //3.遍历RDD进行累加
    actionRDD.foreach(action => acc.add(action))
    //4.对数据进行处理top10
    val map = acc.value
    val categoryList = map.map {
      case (cid, (click, order, pay)) =>
        CategoryCountInfo(cid, click, order, pay)
    }.toList
    val result = categoryList.sortBy(x => (-x.clickCount,-x.orderCount,-x.payCount)).take(10)
    result
  }
}
