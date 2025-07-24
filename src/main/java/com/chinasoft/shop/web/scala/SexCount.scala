package com.chinasoft.shop.web.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{BufferedWriter, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Statement}
import scala.collection.mutable.ArrayBuffer

object SexCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("SexCount")  // 性别(sex)统计
      .setMaster("kkwang:7077")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc: SparkContext = new SparkContext(conf)

    // 数据库配置
    val driver = "com.mysql.cj.jdbc.Driver"
    val url = "jdbc:mysql://172.20.10.3:3306/shixi_keshe?useSSL=false&serverTimezone=UTC&characterEncoding=utf8"
    val user = "root"
    val password = "wqs620429"
    val sourceTable = "data"  // 假设原表名不变
    val resultTable = "gender_distribution"  // 结果表名，体现sex统计

    createTableIfNotExists(driver, url, user, password, resultTable)

    // 从MySQL读取性别(sex)数据
    val sexRDD: RDD[String] = sc.parallelize(Seq(""))
      .flatMap { _ =>
        var conn: Connection = null
        var rs: ResultSet = null
        val sexes = ArrayBuffer[String]()

        try {
          Class.forName(driver)
          conn = DriverManager.getConnection(url, user, password)
          // 明确读取sex字段
          val sql = s"SELECT sex FROM $sourceTable"
          val stmt = conn.createStatement()
          rs = stmt.executeQuery(sql)

          while (rs.next()) {
            sexes += rs.getString("sex")
          }
          println(s"读取到的数据行数: ${sexes.size}")
        } catch {
          case e: Exception =>
            println(s"读取数据库失败: ${e.getMessage}")
        } finally {
          if (rs != null) rs.close()
          if (conn != null) conn.close()
        }

        sexes.iterator
      }

    // 统计各性别的数量
    val sexCountRDD: RDD[(String, Int)] = sexRDD
      .filter(sex => sex != null && sex.nonEmpty)
      .map(sex => (sex, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

    // 打印统计结果
    println("各性别(sex)数量统计:")
    val result = sexCountRDD.collect()
    result.foreach { case (sex, count) =>
      println(s"$sex: $count")
    }
    val total = result.map(_._2).sum
    println(s"统计总和: $total")

    // 将结果写入MySQL（全部替换为sex相关字段）
    sexCountRDD.foreachPartition { partition =>
      var conn: Connection = null
      var pstmt: PreparedStatement = null

      try {
        Class.forName(driver)
        conn = DriverManager.getConnection(url, user, password)
        conn.setAutoCommit(false)

        // 插入语句里的字段为sex，匹配结果表结构
        val sql = s"INSERT INTO $resultTable (gender, count) VALUES (?, ?) ON DUPLICATE KEY UPDATE count = ?"
        pstmt = conn.prepareStatement(sql)

        partition.foreach { case (sex, count) =>
          pstmt.setString(1, sex)
          pstmt.setInt(2, count)
          pstmt.setInt(3, count)
          pstmt.addBatch()
        }

        pstmt.executeBatch()
        conn.commit()
        println("结果已成功写入数据库")
      } catch {
        case e: Exception =>
          println(s"写入数据库失败: ${e.getMessage}")
          if (conn != null) conn.rollback()
      } finally {
        if (pstmt != null) pstmt.close()
        if (conn != null) conn.close()
      }
    }

    // 将结果写入CSV文件（文件名和表头适配sex）
    val csvOutputPath = "C:/Users/10537/Desktop/num_of_sex.csv"
    writeToCsv(sexCountRDD, csvOutputPath)

    sc.stop()
  }

  // 创建性别(sex)统计结果表
  def createTableIfNotExists(driver: String, url: String, user: String, password: String, tableName: String): Unit = {
    var conn: Connection = null
    var stmt: Statement = null

    try {
      Class.forName(driver)
      conn = DriverManager.getConnection(url, user, password)
      stmt = conn.createStatement()

      // 表结构里的字段定义为sex
      val createTableSQL =
        s"""CREATE TABLE IF NOT EXISTS $tableName (
           |  gender VARCHAR(10) NOT NULL PRIMARY KEY,
           |  count INT NOT NULL
           |) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
           |""".stripMargin

      stmt.executeUpdate(createTableSQL)
      println(s"确保表 $tableName 存在成功")
    } catch {
      case e: Exception =>
        println(s"创建表失败: ${e.getMessage}")
        throw e
    } finally {
      if (stmt != null) stmt.close()
      if (conn != null) conn.close()
    }
  }

  // 写入CSV的方法，表头使用sex
  def writeToCsv(rdd: RDD[(String, Int)], outputPath: String): Unit = {
    val header = "gender,count"
    val data = rdd.collect()
    try {
      val writer = new BufferedWriter(new OutputStreamWriter(
        new java.io.FileOutputStream(outputPath),
        StandardCharsets.UTF_8
      ))
      writer.write(header)
      writer.newLine()
      data.foreach { case (gender, count) =>
        writer.write(s"$gender,$count")
        writer.newLine()
      }
      writer.close()
      println(s"结果已写入CSV: $outputPath")
    } catch {
      case e: Exception =>
        println(s"写入CSV失败: ${e.getMessage}")
    }
  }
}