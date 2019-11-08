package com.baizhi.sql

import org.apache.spark.sql.SparkSession

/**
  * Dataframe的纯SQL操作
  */
object DataFrameSqlOperation2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("dataframe sql operation2").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    val dataFrame = List((1, "zs", true, 18, 15000, 1), (2, "ls", false, 25, 16000, 2), (3, "ww", true, 18, 13000, 1)).toDF("id", "name", "sex", "age", "salary", "dept")
    dataFrame.createTempView("t_employee")
    //排序查询
    val sql =
      """
        | select * from t_employee
        | where salary > 13000
        | order by salary desc
      """.stripMargin
    spark
    //.sql("select * from t_employee where name like '%s%'")//模糊查询
    //.sql(sql)
    //        .sql(
    //          """
    //            | select dept,avg(salary) as avg_salary from t_employee
    //            | group by dept
    //            | order by avg_salary desc
    //          """)//分组查询，查询每个部门员工的平均工资

    //limit用来限制返回结果条数
    //      .sql(
    //      """
    //        | select dept,avg(salary) as avg_salary from t_employee
    //        | group by dept
    //        | order by avg_salary desc
    //        | limit 1
    //      """.stripMargin)
    //      .show()

    //having分组后过滤，用在group后面
    //      .sql(
    //      """
    //        | select sex,avg(salary) as avg_salary from t_employee
    //        | group by sex
    //        | having sex = true
    //        | order by avg_salary desc
    //        | limit 1
    //      """.stripMargin)

    //case when then else... else
    //        .sql(
    //          """
    //            | select
    //            | id,name,age,
    //            | case sex
    //            | when true
    //            |   then '男'
    //            | else '女'
    //            | end as newsex
    //            | from t_employee
    //          """.stripMargin)
    //      .show()

    //pivot用来做行转列
    //    var scoreDF = List(
    //      (1, "语文", 100),
    //      (1, "数学", 100),
    //      (1, "英语", 100),
    //      (2, "数学", 79),
    //      (2, "语文", 80),
    //      (2, "英语", 100))
    //      .toDF("id", "course", "score")
    //    scoreDF.createTempView("t_course")
    //    spark.sql(
    //      """
    //        | select * from t_course
    //        | pivot(max(score) for course in('数学','英语','语文'))
    //      """.stripMargin)
    //      .show()

    //join用来做两个DF的连接
    val userInfoDF = spark.sparkContext.makeRDD(List((1, "zs"), (2, "ls"), (3, "ww"))).toDF("id", "name")
    val orderInfoDF = spark.sparkContext.makeRDD(List((1, "iphone", 1000, 1), (2, "mi9", 999, 1), (3, "连衣裙", 99, 2))).toDF("oid", "product", "price", "uid")
    userInfoDF.createTempView("t_user")
    orderInfoDF.createTempView("t_order")
    //连接类型：inner    left_outer    right_outer   full    cross
    spark.sql(
      """
        | select * from t_user t1
        | inner join
        | t_order t2
        | on
        | t1.id = t2.uid
      """.stripMargin).show()


    spark.stop()
  }
}
