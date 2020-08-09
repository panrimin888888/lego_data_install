package com.atguigu.spark.core.project.bean

/**
 * 品类各种行为数量
 * @param categoryId
 * @param clickCount
 * @param orderCount
 * @param payCount
 */
case class CategoryCountInfo(categoryId: String,
                             clickCount: Long,
                             orderCount: Long,
                             payCount: Long)
