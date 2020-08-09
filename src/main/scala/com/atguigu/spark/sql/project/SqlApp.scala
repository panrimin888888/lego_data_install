package com.atguigu.spark.sql.project

import java.text.DecimalFormat

import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, MapType, StringType, StructField, StructType}

object SqlApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("SqlApp")
      .enableHiveSupport()
      .config("spark.sql.shuffle.partitions", 10)
      .getOrCreate()
    spark.sql("use sparkpractice")
    spark.udf.register("remark",new CityRemarkUDAF)
    spark.sql(
      """
        |select ci.city_name,ci.area,pi.product_name,uva.click_product_id
        |from user_visit_action uva
        |join product_info pi on uva.click_product_id =pi.product_id
        |join city_info ci on uva.city_id = ci.city_id
        |""".stripMargin).createOrReplaceTempView("t1")
    spark.sql(
      """
        |select area,product_name,count(*) count,remark(city_name) remark
        |from t1
        |group by area,product_name
        |""".stripMargin).createOrReplaceTempView("t2")
    spark.sql(
      """
        |select
        |area,
        |product_name,
        |count,
        |remark,
        |rank() over(partition by area order by count desc) rk
        |from t2
        |""".stripMargin).createOrReplaceTempView("t3")
    spark.sql(
      """
        |select area,product_name,count,remark
        |from t3
        |where rk <= 3
        |""".stripMargin)
        .coalesce(1)
        .write
        .mode(SaveMode.Overwrite)
        .saveAsTable("result")

    spark.close()
  }
}
class CityRemarkUDAF extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(StructField("city",StringType)::Nil)

  override def bufferSchema: StructType =
    StructType(StructField("map",MapType(StringType,LongType))::StructField("total",LongType)::Nil)

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String,Long]()
    buffer(1) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    input match {
      case Row(cityName: String) =>
        buffer(1) = buffer.getLong(1) + 1L
        val map = buffer.getMap[String,Long](0)
        buffer(0) = map + (cityName -> (map.getOrElse(cityName,0L) + 1L))
      case _ =>
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val map1 = buffer1.getMap[String,Long](0)
    val total1 = buffer1.getLong(1)

    val map2 = buffer2.getMap[String,Long](0)
    val total2 = buffer2.getLong(1)

    buffer1(1) = total1+total2
    buffer1(0) = map1.foldLeft(map2){
      case (map,(city,count)) =>
        map + (city -> (map.getOrElse(city,0L) + count))
    }
  }

  override def evaluate(buffer: Row): Any = {
    val cityCount = buffer.getMap[String,Long](0)
    val total = buffer.getLong(1)
    val cityCountTop2 = cityCount.toList.sortBy(-_._2).take(2)
    val cityRemarkTop2 = cityCountTop2.map{
      case (city,count) => CityRemark(city,count.toDouble / total)
    }
    val cityRemark = cityRemarkTop2 :+ CityRemark("其他",cityRemarkTop2.foldLeft(1D)(_-_.rate))
    cityRemark.mkString(",")
  }
}
case class CityRemark(city: String,rate: Double){
  private val f = new DecimalFormat(".00%")

  override def toString: String = s"$city:${f.format(rate)}"
}
