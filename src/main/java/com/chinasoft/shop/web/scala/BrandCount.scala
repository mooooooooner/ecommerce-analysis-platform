package com.chinasoft.shop.web.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{BufferedWriter, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Statement}
import scala.collection.mutable.ArrayBuffer

object BrandCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("BrandSalesCount")

      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc: SparkContext = new SparkContext(conf)

    // 数据库配置
    val driver = "com.mysql.cj.jdbc.Driver"
    val url = "jdbc:mysql://172.20.10.3:3306/shixi_keshe?useSSL=false&serverTimezone=UTC&characterEncoding=utf8"
    val user = "root"
    val password = "wqs620429"
    val sourceTable = "data"
    val resultTable = "brand_sales"

    createTableIfNotExists(driver, url, user, password, resultTable)

    // 从MySQL读取品牌(brand)和价格(price)数据，过滤掉brand为null的记录
    val brandPriceRDD: RDD[(String, Double)] = sc.parallelize(Seq(""))
      .flatMap { _ =>
        var conn: Connection = null
        var rs: ResultSet = null
        val brandPrices = ArrayBuffer[(String, Double)]()

        try {
          Class.forName(driver)
          conn = DriverManager.getConnection(url, user, password)
          val sql = s"SELECT brand, price FROM $sourceTable"
          val stmt = conn.createStatement()
          rs = stmt.executeQuery(sql)

          while (rs.next()) {
            val brand = rs.getString("brand")
            val price = rs.getDouble("price")
            // 过滤掉brand为null的记录
            if (brand != null) {
              brandPrices += ((brand, price))
            }
          }
          println(s"读取到的数据行数（已过滤null品牌）: ${brandPrices.size}")
        } catch {
          case e: Exception =>
            println(s"读取数据库失败: ${e.getMessage}")
        } finally {
          if (rs != null) rs.close()
          if (conn != null) conn.close()
        }

        brandPrices.iterator
      }

    // 统计各品牌的总销量（price求和）和商品数量（计数）
    val brandSalesRDD: RDD[(String, (Double, Int))] = brandPriceRDD
      .map { case (brand, price) => (brand, (price, 1)) }
      .reduceByKey { case ((sum1, count1), (sum2, count2)) =>
        (sum1 + sum2, count1 + count2)
      }
      .sortBy(_._2._1, ascending = false)

    // 只保留总销量最多的10个品牌（基于非null品牌统计结果）
    val top10BrandSalesRDD: RDD[(String, (Double, Int))] = brandSalesRDD.zipWithIndex()
      .filter { case (_, index) => index < 10 }
      .map { case ((brand, (totalSales, totalCount)), _) => (brand, (totalSales, totalCount)) }

    // 打印Top10统计结果
    println("总销量最多的10个非null品牌统计:")
    val top10Result = top10BrandSalesRDD.collect()
    top10Result.foreach { case (brand, (totalSales, totalCount)) =>
      println(s"$brand: 总销量 = $totalSales, 商品数量 = $totalCount")
    }

    // 将Top10结果写入MySQL
    top10BrandSalesRDD.foreachPartition { partition =>
      var conn: Connection = null
      var pstmt: PreparedStatement = null

      try {
        Class.forName(driver)
        conn = DriverManager.getConnection(url, user, password)
        conn.setAutoCommit(false)

        val sql = s"INSERT INTO $resultTable (brand, total_sales, total_count) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE total_sales = ?, total_count = ?"
        pstmt = conn.prepareStatement(sql)

        partition.foreach { case (brand, (totalSales, totalCount)) =>
          pstmt.setString(1, brand)
          pstmt.setDouble(2, totalSales)
          pstmt.setInt(3, totalCount)
          pstmt.setDouble(4, totalSales)
          pstmt.setInt(5, totalCount)
          pstmt.addBatch()
        }

        pstmt.executeBatch()
        conn.commit()
        println(s"Top10非null品牌销售数据已成功写入数据库表: $resultTable")
      } catch {
        case e: Exception =>
          println(s"写入数据库失败: ${e.getMessage}")
          if (conn != null) conn.rollback()
      } finally {
        if (pstmt != null) pstmt.close()
        if (conn != null) conn.close()
      }
    }

    // 将Top10结果写入CSV文件
    val csvOutputPath = "C:/Users/lxy18/Desktop/top10_brand_sales.csv"
    writeToCsv(top10BrandSalesRDD, csvOutputPath)

    sc.stop()
  }

  // 创建品牌销售统计结果表
  def createTableIfNotExists(driver: String, url: String, user: String, password: String, tableName: String): Unit = {
    var conn: Connection = null
    var stmt: Statement = null

    try {
      Class.forName(driver)
      conn = DriverManager.getConnection(url, user, password)
      stmt = conn.createStatement()

      val createTableSQL =
        s"""CREATE TABLE IF NOT EXISTS $tableName (
           |  brand VARCHAR(50) NOT NULL PRIMARY KEY,
           |  total_sales DECIMAL(10, 2) NOT NULL,
           |  total_count INT NOT NULL
           |) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
           |""".stripMargin

      stmt.executeUpdate(createTableSQL)
      println(s"确保品牌销售统计表 $tableName 存在成功")
    } catch {
      case e: Exception =>
        println(s"创建表失败: ${e.getMessage}")
        throw e
    } finally {
      if (stmt != null) stmt.close()
      if (conn != null) conn.close()
    }
  }

  // 写入CSV的方法（适配品牌销售统计）
  def writeToCsv(rdd: RDD[(String, (Double, Int))], outputPath: String): Unit = {
    val header = "brand,total_sales,total_count"
    val data = rdd.collect()
    try {
      val writer = new BufferedWriter(new OutputStreamWriter(
        new java.io.FileOutputStream(outputPath),
        StandardCharsets.UTF_8
      ))
      writer.write(header)
      writer.newLine()
      data.foreach { case (brand, (totalSales, totalCount)) =>
        writer.write(s"$brand,$totalSales,$totalCount")
        writer.newLine()
      }
      writer.close()
      println(s"Top10非null品牌销售数据已写入CSV: $outputPath")
    } catch {
      case e: Exception =>
        println(s"写入CSV失败: ${e.getMessage}")
    }
  }
}