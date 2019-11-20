package com.baizhi.flink.join

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class UserAssignerWithPunctuatedWatermarks02 extends AssignerWithPunctuatedWatermarks[(String,String,Long)]{
  //定义最大乱序时间
  var maxOrderness = 2000L
  //处理节点看到最大时间的时间
  var maxSeenTime = 0L

  override def checkAndGetNextWatermark(lastElement: (String, String, Long), extractedTimestamp: Long): Watermark = {
    new Watermark(maxSeenTime-maxOrderness)
  }

  override def extractTimestamp(element: (String, String, Long), previousElementTimestamp: Long): Long = {
    maxSeenTime = Math.max(element._3,maxSeenTime)
    element._3
  }
}
