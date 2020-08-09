package com.atguigu.spark.core.project.acc

import com.atguigu.spark.core.project.bean.UserVisitAction
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/*
累加器类型：UserVisitAction
最终的返回值类型：
  计算每个品类的点击量，下单量，支付量
  Map[品类,(clickCount,orderCount,payCount)]
 */
class CategoryAcc extends AccumulatorV2[UserVisitAction,mutable.Map[String,(Long,Long,Long)]]{
  //可变map使用val就可以
  private val map = mutable.Map[String,(Long,Long,Long)]()
  //判断集合是否为空
  override def isZero: Boolean = map.isEmpty
  //复制累加器
  override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[String, (Long, Long, Long)]] = {
    val acc = new CategoryAcc
    acc.map.synchronized(acc.map ++= this.map)
    acc
  }
  //重置累加器
  override def reset(): Unit = map.clear()
  //累加，分区内聚合累加
  override def add(v: UserVisitAction): Unit = {
  /*
  进来的行为有可能是搜索（不处理），点击，下单，支付
  Map[品类,(clickCount,orderCount,payCount)]
   */
    v match {
        //判断是否为点击
      case action if action.click_category_id != -1 =>
        //点击的品类id
        val cid = action.click_category_id.toString
        //map中已经存储的cid的（点击量，下单量，支付量）
        val (click,order,pay) = map.getOrElse(cid,(0L,0L,0L))
        map += cid -> (click + 1L,order,pay)
      //判断是否为下单
      case action if action.order_category_ids != "null" =>
        //"1,2,3"
        val cids = action.order_category_ids.split(",")
        cids.foreach(cid => {
          val (click,order,pay) = map.getOrElse(cid,(0L,0L,0L))
          map += cid -> (click,order + 1L,pay)
        })
        //判断是否为支付
      case action if action.pay_category_ids != "null" =>
        val cids = action.pay_category_ids.split(",")
        cids.foreach(cid => {
          val (click,order,pay) = map.getOrElse(cid,(0L,0L,0L))
          map += cid -> (click,order,pay + 1)
        })
        //其他行为不做处理
      case _ =>
    }
  }
  //分区间的合并
  override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[String, (Long, Long, Long)]]): Unit = {
    //涉及到map的合并
    other match {
      case o: CategoryAcc =>
        //偏函数使用
        o.map.foreach{
          case (cid,(click,order,pay)) =>
            val (thisclick,thisorder,thispay) = map.getOrElse(cid, (0L, 0L, 0L))
            this.map += cid -> (thisclick+click,thisorder+order,thispay+pay)
        }
      case _ =>throw new UnsupportedOperationException
    }
  }

  override def value: mutable.Map[String, (Long, Long, Long)] = map
}
