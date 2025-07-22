package com.chinasoft.shop.web.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{BufferedWriter, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Statement}
import scala.collection.mutable.ArrayBuffer

object CategoryCount {
  def main(args: Array[String]): Unit = {
    // 1. 初始化Spark配置和上下文
    val conf: SparkConf = new SparkConf()
      .setAppName("CategoryCount")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // 2. 数据库连接配置
    val driver = "com.mysql.cj.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/shixi_keshe?useSSL=false&serverTimezone=UTC&characterEncoding=utf8"
    val user = "root"
    val password = "123456"
    val sourceTable = "data"  // 源数据表名
    val resultTable = "category_sales"  // 存储类别销售额的表名

    // 3. 创建结果表（如果不存在）
    createTableIfNotExists(driver, url, user, password, resultTable)

    // 4. 从MySQL读取数据（类别和价格字段）
    val categoryRDD: RDD[(String, Double)] = sc.parallelize(Seq(""))  // 单元素RDD避免重复读取
      .flatMap { _ =>
        var conn: Connection = null
        var rs: ResultSet = null
        val categoryData = ArrayBuffer[(String, Double)]()  // (类别, 销售额)
        try {
          Class.forName(driver)
          conn = DriverManager.getConnection(url, user, password)
          // 读取category_code和price字段
          val sql = s"SELECT category_code, price FROM $sourceTable WHERE price IS NOT NULL AND category_code IS NOT NULL"
          val stmt = conn.createStatement()
          rs = stmt.executeQuery(sql)

          while (rs.next()) {
            val category = rs.getString("category_code")
            val price = rs.getDouble("price")
            categoryData += ((category, price))
          }
          println(s"读取到的商品记录数: ${categoryData.size}")
        } catch {
          case e: Exception =>
            println(s"读取数据库失败: ${e.getMessage}")
        } finally {
          if (rs != null) rs.close()
          if (conn != null) conn.close()
        }

        categoryData.iterator
      }

    // 5. 按类别统计总销售额
    val categorySalesRDD: RDD[(String, Double)] = categoryRDD
      .reduceByKey(_ + _)  // 累加每个类别的销售额
      .sortByKey()

    // 6. 打印统计结果
    println("\n各类别总销售额统计:")
    val result = categorySalesRDD.collect()
    result.foreach { case (category, total) =>
      println(f"$category: ￥${total}%.2f")  // 保留两位小数
    }

    // 7. 将结果写入MySQL
    categorySalesRDD.foreachPartition { partition =>
      var conn: Connection = null
      var pstmt: PreparedStatement = null

      try {
        Class.forName(driver)
        conn = DriverManager.getConnection(url, user, password)
        conn.setAutoCommit(false)

        // 插入或更新各类别销售额
        val sql = s"""INSERT INTO $resultTable (category, total_sales)
                     |VALUES (?, ?) ON DUPLICATE KEY UPDATE total_sales = ?""".stripMargin
        pstmt = conn.prepareStatement(sql)

        partition.foreach { case (category, total) =>
          pstmt.setString(1, category)
          pstmt.setDouble(2, total)
          pstmt.setDouble(3, total)  // 重复时更新
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
    val csvOutputPath = "D:\\course\\pract\\BigData\\category_sales.csv"
    writeToCsv(categorySalesRDD, csvOutputPath)

    // 9. 关闭Spark上下文
    sc.stop()
  }

  // 创建类别销售额表（如果不存在）
  def createTableIfNotExists(driver: String, url: String, user: String, password: String, tableName: String): Unit = {
    var conn: Connection = null
    var stmt: Statement = null

    try {
      Class.forName(driver)
      conn = DriverManager.getConnection(url, user, password)
      stmt = conn.createStatement()

      val createTableSQL =
        s"""CREATE TABLE IF NOT EXISTS $tableName (
           |  category VARCHAR(100) NOT NULL PRIMARY KEY,
           |  total_sales DECIMAL(15, 2) NOT NULL
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
  def writeToCsv(rdd: RDD[(String, Double)], outputPath: String): Unit = {
    val header = "category,total_sales"
    val data = rdd.collect()

    try {
      val writer = new BufferedWriter(new OutputStreamWriter(
        new java.io.FileOutputStream(outputPath),
        StandardCharsets.UTF_8  // 指定UTF-8编码
      ))
      writer.write(header)
      writer.newLine()

      data.foreach { case (category, total) =>
        writer.write(f"$category,${total}%.2f")  // 保留两位小数
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