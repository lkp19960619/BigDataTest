package com.baizhi.flink.day02.datasource.filesource

import org.apache.flink.api.common.io.FilePathFilter
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
/**
  * readTextFile()
  */
object FileBasedDataSourceTest3 {
  def main(args: Array[String]): Unit = {
    //创建StreamExecutionEnvironment
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(3)
    //创建DataStream
    //设置FilePathFilter
    val inputFormat = new TextInputFormat(null)
    inputFormat.setFilesFilter(new FilePathFilter {
      override def filterPath(filePath: Path): Boolean = {
        println(filePath.getName)
        return false
      }
    })
    val dataStream = environment.readFile(inputFormat,"file:///d:\\data",FileProcessingMode.PROCESS_CONTINUOUSLY,1000)
    dataStream.flatMap(_.split("\\s+"))
      .map((_,1))
      .keyBy(t=>t._1)
      .sum(1)
      .print()
    environment.execute()
  }

}
