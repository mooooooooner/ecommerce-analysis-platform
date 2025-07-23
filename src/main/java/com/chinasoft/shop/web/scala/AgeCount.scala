package com.chinasoft.shop.web.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{BufferedWriter, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Statement}
import scala.collection.mutable.ArrayBuffer

object AgeCount {
  def main(args: Array[String]): Unit = {
    // 1. 初始化Spark配置和上下文
    val conf: SparkConf = new SparkConf()
      .setAppName("AgeCount")

    val sc: SparkContext = new SparkContext(conf)

    // 2. 数据库连接配置
    val driver = "com.mysql.cj.jdbc.Driver"
    val url = "jdbc:mysql://172.20.10.3:3306/shixi_keshe?useSSL=false&serverTimezone=UTC&characterEncoding=utf8"
    val user = "root"
    val password = "wqs620429"
    val sourceTable = "data"  // 源数据表名
    val resultTable = "age_distribution"  // 存储年龄分布的表名

    // 3. 创建结果表（如果不存在）
    createTableIfNotExists(driver, url, user, password, resultTable)

    // 4. 从MySQL读取数据（年龄字段）
    val ageRDD: RDD[(String, Int)] = sc.parallelize(Seq(""))  // 单元素RDD避免重复读取
      .flatMap { _ =>
        var conn: Connection = null
        var rs: ResultSet = null
        val ageData = ArrayBuffer[(String, Int)]()  // (年龄段, 数量)
        try {
          Class.forName(driver)
          conn = DriverManager.getConnection(url, user, password)
          // 读取age字段并分组统计
          val sql = s"SELECT age FROM $sourceTable WHERE age IS NOT NULL"
          val stmt = conn.createStatement()
          rs = stmt.executeQuery(sql)

          while (rs.next()) {
            val age = rs.getInt("age")
            val ageGroup = getAgeGroup(age)  // 根据年龄划分年龄段
            ageData += ((ageGroup, 1))
          }
          println(s"读取到的年龄记录数: ${ageData.size}")
        } catch {
          case e: Exception =>
            println(s"读取数据库失败: ${e.getMessage}")
        } finally {
          if (rs != null) rs.close()
          if (conn != null) conn.close()
        }

        ageData.iterator
      }

    // 5. 按年龄段统计人数
    val ageDistributionRDD: RDD[(String, Int)] = ageRDD
      .reduceByKey(_ + _)  // 累加每个年龄段的人数
      .sortByKey()

    // 6. 打印统计结果
    println("\n各年龄段用户数量统计:")
    val result = ageDistributionRDD.collect()
    result.foreach { case (ageGroup, count) =>
      println(f"$ageGroup: $count")
    }

    // 7. 将结果写入MySQL
    ageDistributionRDD.foreachPartition { partition =>
      var conn: Connection = null
      var pstmt: PreparedStatement = null

      try {
        Class.forName(driver)
        conn = DriverManager.getConnection(url, user, password)
        conn.setAutoCommit(false)

        // 插入或更新各年龄段用户数量
        val sql = s"""INSERT INTO $resultTable (age_group, count)
                     |VALUES (?, ?) ON DUPLICATE KEY UPDATE count = ?""".stripMargin
        pstmt = conn.prepareStatement(sql)

        partition.foreach { case (ageGroup, count) =>
          pstmt.setString(1, ageGroup)
          pstmt.setInt(2, count)
          pstmt.setInt(3, count)  // 重复时更新
          pstmt.addBatch()
        }

        pstmt.executeBatch()
        conn.commit()
        println("\n结果已成功写入数据库")
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
    val csvOutputPath = "C:/Users/lxy18/Desktop/age_distribution.csv"
    writeToCsv(ageDistributionRDD, csvOutputPath)

    // 9. 关闭Spark上下文
    sc.stop()
  }

  // 根据年龄划分年龄段
  def getAgeGroup(age: Int): String = {
    if (age < 18) "18岁以下"
    else if (age >= 18 && age <= 24) "18-24岁"
    else if (age >= 25 && age <= 34) "25-34岁"
    else if (age >= 35 && age <= 44) "35-44岁"
    else if (age >= 45 && age <= 54) "45-54岁"
    else if (age >= 55 && age <= 64) "55-64岁"
    else "65岁以上"
  }

  // 创建年龄分布表（如果不存在）
  def createTableIfNotExists(driver: String, url: String, user: String, password: String, tableName: String): Unit = {
    var conn: Connection = null
    var stmt: Statement = null

    try {
      Class.forName(driver)
      conn = DriverManager.getConnection(url, user, password)
      stmt = conn.createStatement()

      val createTableSQL =
        s"""CREATE TABLE IF NOT EXISTS $tableName (
           |  age_group VARCHAR(20) NOT NULL PRIMARY KEY,
           |  count INT NOT NULL
           |) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
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

  // 写入CSV文件（解决乱码问题）
  def writeToCsv(rdd: RDD[(String, Int)], outputPath: String): Unit = {
    val header = "age_group,count"
    val data = rdd.collect()

    try {
      val writer = new BufferedWriter(new OutputStreamWriter(
        new java.io.FileOutputStream(outputPath),
        StandardCharsets.UTF_8  // 指定UTF-8编码
      ))
      writer.write(header)
      writer.newLine()

      data.foreach { case (ageGroup, count) =>
        writer.write(f"$ageGroup,$count")
        writer.newLine()
      }

      writer.close()
      println(s"结果已成功写入CSV文件: $outputPath")
    } catch {
      case e: Exception =>
        println(s"写入CSV文件失败: ${e.getMessage}")
    }
  }
}