package com.baizhi.sql.operation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object DataframeOperationsTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("dataframe application").master("local[*]").getOrCreate()
    //导入spark的隐式增强
    import spark.implicits._
    //设置显示日志等级
    spark.sparkContext.setLogLevel("ERROR")
    //创建DF，通过RDD的方式
    val rdd = spark.sparkContext.makeRDD(List((1, "zs", 20000.00, true), (2, "ls", 20000.00, true), (3, "ww", 3000.00, false)))

    //把rdd装换为DF并给资源组每个值起别名
    val dataFrame = rdd.toDF("id", "name", "salary", "sex")
    //打印df结构
    dataFrame.printSchema()
    //默认输出前20行
    //dataFrame.show()
    //select查询指定字段
    dataFrame
    //隐式转换的写法，将一个字符串列名转换为一个Column对象
    //.select($"id",$"name",$"salary")
    //.select("id", "name", "sex")

    //下面的方法等价于select($"id",$"name",$"sex",$"salary" * 12 as "annual_salary")
    //.selectExpr("id", "name", "sex", "salary * 12 as annual_salary")//查询表达式

    //    dataFrame.select("id", "name","salary")
    //      //第二个参数表示列的来源
    //      .withColumn("annual_salary", $"salary" * 12)//withColumn()用来添加列或者替换列
    //      .withColumnRenamed("name","newName")//用来给列重新命名
    //      .drop("salary")//移除已经存在的列
    //      .show()
    //-------------------------------------------------------------------------------------------------

    //-------------------------------------------------------------------------------------------------
    //删除重复的数据  DropDuplicates
    //  dataFrame.select($"id",$"name",$"sex",$"salary")
    //      .dropDuplicates("salary")
    //      .show()
    //-------------------------------------------------------------------------------------------------

    //-------------------------------------------------------------------------------------------------
    //orderBy和sort用来做排序
    //    dataFrame
    //      .select($"id", $"name", $"salary", $"sex")
    //      .orderBy($"salary" desc)//按照工资做降序排列//多列排序，先按照第一个列排序，第一个相同再按照第二个列排序
    //      .show()
    //-------------------------------------------------------------------------------------------------

    import org.apache.spark.sql.functions._
    //-------------------------------------------------------------------------------------------------
    //agg聚合操作，在聚合之前需要先导入它的隐式转换
    //    var df = List(
    //      (1, "zs", true, 1, 15000),
    //      (2, "ls", false, 2, 18000),
    //      (3, "ww", false, 2, 14000),
    //      (4, "zl", false, 1, 18000),
    //      (5, "win7", false, 1, 16000))
    //      .toDF("id", "name", "sex", "dept", "salary")
    //      .groupBy("sex")
    //      //.agg(max("salary"), min("salary"), avg("salary"), sum("salary"))
    //      .agg(Map(("salary","max"),("salary","min"),("salary","avg"),("salary","sum")))//只能查最后一个字段
    //      .show()
    //-------------------------------------------------------------------------------------------------

    //-------------------------------------------------------------------------------------------------
    //limit用来显示返回的结果条数
    //        .limit(2).show()
    //-------------------------------------------------------------------------------------------------

    //where用来做匹配
    //.select($"id", $"name", $"sex", $"salary")
    //.where(($"name" like "%s%" and $"salary" > 15000) or $"name" === "win7")
    //.show( )
    //-------------------------------------------------------------------------------------------------

    //-------------------------------------------------------------------------------------------------
    //pivot用来做行转列
    var scoreDF = List(
      (1, "math", 85),
      (1, "chinese", 80),
      (1, "english", 90),
      (2, "math", 90),
      (2, "chinese", 80))
      .toDF("id", "course", "score")
    //    scoreDF
    //      .groupBy($"id")
    //      .pivot($"course")
    //      .max("score")
    //      .show()
    //-------------------------------------------------------------------------------------------------

    //-------------------------------------------------------------------------------------------------
    //na指的是对空值的一种处理方式，有两个方法
    //1.na().fill 填充 null赋予默认值
    //2.na().fill 删除为null的一行数据
    //    scoreDF
    //      .groupBy("id")
    //      .pivot("course")
    //      .max("score")
    //      .na.fill(86)//赋予默认值
    //      .na.drop()
    //      .show()
    //-------------------------------------------------------------------------------------------------

    //-------------------------------------------------------------------------------------------------
    /*
    over->窗口函数
      1.聚合函数
      2.排名函数
      3.分析函数
      作用：窗口函数使用over，对一组数据进行操作，返回普通列和聚合列
    */

    var df = List(
      (1, "zs", true, 1, 15000),
      (2, "ls", false, 2, 18000),
      (3, "ww", false, 2, 14000),
      (4, "zl", false, 1, 18000),
      (5, "win7", false, 1, 16000))
      .toDF("id", "name", "sex", "dept", "salary")

    //-------------------------------------------------------------------------------------------------
    //定义一个窗口函数
    //    val w1 = Window
    //      .partitionBy("dept") //根据部门分区：部门相同的数据划分到同一个分区
    //      .orderBy($"salary" desc) //对分区内的数据，按照工资salary进行降序排列
    //      //.rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing) //代表窗口数据可视范围
    //      .rowsBetween(Window.currentRow, 1) //代表窗口数据可视范围->rowsBetween表示根据行
    ////      .rangeBetween(0, 2000)
    //
    //    df
    //      .select($"id", $"name", $"sex", $"dept", $"salary") //查询出普通列
    //      .withColumn("sum", sum("id") over (w1)) //为每一行数据添加聚合列
    //      .show()
    //-------------------------------------------------------------------------------------------------

    //-------------------------------------------------------------------------------------------------
    //join->用来做两个Dataframe的连接，Dataframe就相当于一张数据库表
    //    val userInfoDF = spark.sparkContext.makeRDD(List((1, "zs"), (2, "ls"), (3, "ww"))).toDF("id", "name")
    //    val orderInfoDF = spark.sparkContext.makeRDD(List((1, "iphone", 10000, 1), (2, "mi9", 4500, 2), (3, "huawei", 5800, 3))).toDF("oid", "product", "price", "u_id")
    //    userInfoDF
    //      .join(orderInfoDF, $"id" === $"u_id", "left_outer") //第二个参数为连接字段,第三个参数指定连接类型，默认是内连接(inner)
    //      .show()
    //-------------------------------------------------------------------------------------------------

    //-------------------------------------------------------------------------------------------------
    //cube->多维度查询：根据多个分组可能进行数据的查询
    //比如cube(A,B)会尝试根据A和B的不同组合来进行分组查询
    //     group by A null
    //     group by B null
    //     group by AB null
    List(
      (110, 50, 80, 80),
      (120, 60, 95, 75),
      (120, 50, 96, 70))
      .toDF("height", "weight", "IQ", "EQ")
      .cube($"height", $"weight")//spark sql尝试根据指定的字段进行各种组合的分组，这种操作的好处，以后如果有任何指定字段的分区操作，都会出现在cube的结果表中
      .agg(avg("IQ"), avg("EQ"))
      .show()
    /*
      +------+------+-----------------+-------+
      |height|weight|          avg(IQ)|avg(EQ)|
      +------+------+-----------------+-------+
      |   110|    50|             80.0|   80.0|
      |   120|  null|             95.5|   72.5|
      |   120|    60|             95.0|   75.0|
      |  null|    60|             95.0|   75.0|
      |  null|  null|90.33333333333333|   75.0|
      |   120|    50|             96.0|   70.0|
      |   110|  null|             80.0|   80.0|
      |  null|    50|             88.0|   75.0|
      +------+------+-----------------+-------+
     */
    //-------------------------------------------------------------------------------------------------
    spark.stop()
  }
}
