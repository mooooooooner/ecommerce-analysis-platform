package com.chinasoft.shop.web.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{BufferedWriter, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Statement}
import scala.collection.mutable.ArrayBuffer

object ProvinceCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("ProvinceCount")
      .setMaster("kkwang:7077")
    val sc: SparkContext = new SparkContext(conf)

    // 数据库配置（不变）
    val driver = "com.mysql.cj.jdbc.Driver"
    val url = "jdbc:mysql://172.20.10.3:3306/shixi_keshe?useSSL=false&serverTimezone=UTC&characterEncoding=utf8"
    val user = "root"
    val password = "wqs620429"
    val sourceTable = "data"
    val resultTable = "num_of_province"

    createTableIfNotExists(driver, url, user, password, resultTable)

    // 4. 从MySQL读取数据（修正：确保只读取一次）
    // 用单元素RDD触发读取（只执行一次），避免多分区重复读取
    val provinceRDD: RDD[String] = sc.parallelize(Seq(""))  // 单元素RDD（仅1个分区）
      .flatMap { _ =>  // 用flatMap展开数据，替代mapPartitions
        var conn: Connection = null
        var rs: ResultSet = null
        val provinces = ArrayBuffer[String]()

        try {
          Class.forName(driver)
          conn = DriverManager.getConnection(url, user, password)
          val sql = s"SELECT local FROM $sourceTable"
          val stmt = conn.createStatement()
          rs = stmt.executeQuery(sql)

          while (rs.next()) {
            provinces += rs.getString("local")
          }
          // 打印实际读取的行数（应等于表中总记录数）
          println(s"读取到的数据行数: ${provinces.size}")
        } catch {
          case e: Exception =>
            println(s"读取数据库失败: ${e.getMessage}")
        } finally {
          if (rs != null) rs.close()
          if (conn != null) conn.close()
        }

        provinces.iterator
      }

    // 5. 统计各省份数量（逻辑不变，但数据不再重复）
    val provinceCountRDD: RDD[(String, Int)] = provinceRDD
      .filter(province => province != null && province.nonEmpty)  // 过滤空值
      .map(province => (province, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

    // 6. 打印统计结果（此时总和应等于读取的行数）
    println("各省份数量统计:")
    val result = provinceCountRDD.collect()
    result.foreach { case (province, count) =>
      println(s"$province: $count")
    }
    // 验证总和是否正确
    val total = result.map(_._2).sum
    println(s"统计总和: $total")

    // 7. 将结果写入MySQL
    provinceCountRDD.foreachPartition { partition =>
      var conn: Connection = null
      var pstmt: PreparedStatement = null

      try {
        Class.forName(driver)
        conn = DriverManager.getConnection(url, user, password)
        conn.setAutoCommit(false)

        val sql = s"INSERT INTO $resultTable (province, count) VALUES (?, ?) ON DUPLICATE KEY UPDATE count = ?"
        pstmt = conn.prepareStatement(sql)

        partition.foreach { case (province, count) =>
          pstmt.setString(1, province)
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

    // 8. 将结果写入CSV文件
    val csvOutputPath = "C:/Users/lxy18/Desktop/num_of_province.csv"
    writeToCsv(provinceCountRDD, csvOutputPath)

    sc.stop()
  }

  // 创建表的辅助方法
  def createTableIfNotExists(driver: String, url: String, user: String, password: String, tableName: String): Unit = {
    var conn: Connection = null
    var stmt: Statement = null

    try {
      Class.forName(driver)
      conn = DriverManager.getConnection(url, user, password)
      stmt = conn.createStatement()

      val createTableSQL =
        s"""CREATE TABLE IF NOT EXISTS $tableName (
           |  province VARCHAR(50) NOT NULL PRIMARY KEY,
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

  def writeToCsv(rdd: RDD[(String, Int)], outputPath: String): Unit = {
    // 建议用BufferedWriter指定UTF-8编码，解决乱码
    val header = "province,count"
    val data = rdd.collect()
    try {
      val writer = new BufferedWriter(new OutputStreamWriter(
        new java.io.FileOutputStream(outputPath),
        StandardCharsets.UTF_8
      ))
      writer.write(header)
      writer.newLine()
      data.foreach { case (province, count) =>
        writer.write(s"$province,$count")
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
