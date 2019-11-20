package com.baizhi.flink.eventtime

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
 * AssignerWithPunctuatedWatermarks，一有元素进来就产生水位线，可以保证水位线是最新的，但是浪费
 * AssignerWithPeriodicWatermarks，定期产生水位线，
 */
class UserDefineAssignerWithPeriodicWatermarks extends AssignerWithPeriodicWatermarks[(String,Long)]{
  //定义最大乱序时间
  var maxOrderness = 2000L
  //处理节点看到最大时间的时间
  var maxSeenTime = 0L

  val format = new SimpleDateFormat("HH:mm:ss")
  override def getCurrentWatermark: Watermark = {
//    //打印当前水位线的时间
//    println("watermark:"+format.format(maxSeenTime-maxOrderness))
    new Watermark(maxSeenTime-maxOrderness)
  }

  override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
    //用元素的时间戳和maxSeenTime的时间戳做比对，把最大的值赋值给maxSeenTime
    maxSeenTime = Math.max(element._2,maxSeenTime)
    element._2
  }
}
