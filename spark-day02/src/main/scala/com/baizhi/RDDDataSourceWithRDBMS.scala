package com.baizhi

import java.sql.{DriverManager, PreparedStatement, ResultSet}
import java.sql.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapred.lib.db.{DBConfiguration, DBInputFormat, DBWritable}
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object RDDDataSourceWithRDBMS {
  def main(args: Array[String]): Unit = {
    //在本地运行
//    val conf = new SparkConf().setAppName("rdd with rdbms").setMaster("local[*]")
    //在集群上运行
    val conf = new SparkConf().setAppName("rdd with rdbms").setMaster("spark://Spark01:7077")
    val sc = new SparkContext(conf)

//    method1(sc)
    method2(sc)
    sc.stop()
  }

  //通过RDBMS创建Spark RDD对象
  //方法一
  /**
    * 通过jdbcRdd对象构建   在定义SQL语句时候必须指定查询范围区间
    *
    * @param sc
    */
  def method1(sc: SparkContext): Unit = {
    val rdd = new JdbcRDD[(Int, String, String, Date)](
      sc,
      () => {
        Class.forName("com.mysql.jdbc.Driver")
        DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "960619")
      },
      "select * from t_user where id >= ? and id <= ?",
      //上界和下界用来给？赋值
      1,
      26,
      1,
      rs => (rs.getInt("id"), rs.getString("username"), rs.getString("password"), rs.getDate("birthday"))
    )
    rdd.foreach(println)

  }

  def method2(sc: SparkContext): Unit = {
    val conf = new Configuration()
    //数据源连接参数
    conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "com.mysql.jdbc.Driver")
    conf.set(DBConfiguration.URL_PROPERTY, "jdbc:mysql://localhost:3306/test")
    conf.set(DBConfiguration.USERNAME_PROPERTY, "root")
    conf.set(DBConfiguration.PASSWORD_PROPERTY, "960619")
    //配置查询SQL语句
    conf.set(DBConfiguration.INPUT_QUERY, "select * from t_user")
    conf.set(DBConfiguration.INPUT_COUNT_QUERY, "select count(*) from t_user")
    //必须配置实体类的全限定名
    conf.set(DBConfiguration.INPUT_CLASS_PROPERTY,"com.baizhi.UserWritable")
    val rdd: RDD[(LongWritable, UserWritable)] = sc.newAPIHadoopRDD(conf, classOf[DBInputFormat[UserWritable]],classOf[LongWritable],classOf[UserWritable])
    rdd.foreach(t=>println(t._2.id+"\t"+t._2.username+"\t"+t._2.password+"\t"+t._2.birthday))
  }
}

/**
  * 自定义对象序列化类型
  */
class UserWritable extends DBWritable {
  var id: Int = _
  var username: String = _
  var password: String = _
  var birthday: Date = _

  override def write(preparedStatement: PreparedStatement): Unit = {
    preparedStatement.setInt(1,this.id)
    preparedStatement.setString(2,this.username)
    preparedStatement.setString(3,this.password)
    preparedStatement.setDate(4,this.birthday)
  }

  override def readFields(resultSet: ResultSet): Unit = {
    this.id=resultSet.getInt("id")
    this.username=resultSet.getString("username")
    this.password=resultSet.getString("password")
    this.birthday=resultSet.getDate("birthday")
  }
}
