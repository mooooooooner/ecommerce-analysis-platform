package com.chinasoft.shop.web.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{BufferedWriter, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Statement}
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ArrayBuffer

object MoneyCount {
  def main(args: Array[String]): Unit = {
    // 1. 初始化Spark配置和上下文
    val conf: SparkConf = new SparkConf()
      .setAppName("MoneyCount")
      .setMaster("kkwang:7077")  // 本地模式，生产环境移除
    val sc: SparkContext = new SparkContext(conf)

    // 2. 数据库连接配置（使用你的数据库参数）
    val driver = "com.mysql.cj.jdbc.Driver"
    val url = "jdbc:mysql://172.20.10.3:3306/shixi_keshe?useSSL=false&serverTimezone=UTC&characterEncoding=utf8"
    val user = "root"
    val password = "wqs620429"
    val sourceTable = "data"  // 源数据表名（与ProvinceCount相同）
    val resultTable = "daily_sales"  // 存储每日销售额的表名

    // 3. 创建结果表（如果不存在）
    createTableIfNotExists(driver, url, user, password, resultTable)

    // 4. 从MySQL读取数据（日期和价格字段）
    val salesRDD: RDD[(String, Double)] = sc.parallelize(Seq(""))  // 单元素RDD避免重复读取
      .flatMap { _ =>
        var conn: Connection = null
        var rs: ResultSet = null
        val salesData = ArrayBuffer[(String, Double)]()  // (日期, 价格)
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd")  // 统一日期格式

        try {
          Class.forName(driver)
          conn = DriverManager.getConnection(url, user, password)
          // 读取event_time（日期）和price（价格）字段
          val sql = s"SELECT event_time, price FROM $sourceTable WHERE price IS NOT NULL"
          val stmt = conn.createStatement()
          rs = stmt.executeQuery(sql)

          while (rs.next()) {
            // 处理日期：提取年月日（忽略时间）
            val eventTime = rs.getString("event_time")
            val date = try {
              dateFormat.format(dateFormat.parse(eventTime))  // 统一格式为yyyy-MM-dd
            } catch {
              case _: Exception => "invalid_date"  // 处理格式错误的日期
            }

            // 处理价格：转换为Double
            val price = try {
              rs.getDouble("price")
            } catch {
              case _: Exception => 0.0  // 处理价格异常值
            }

            salesData += ((date, price))
          }
          println(s"读取到的销售记录数: ${salesData.size}")
        } catch {
          case e: Exception =>
            println(s"读取数据库失败: ${e.getMessage}")
        } finally {
          if (rs != null) rs.close()
          if (conn != null) conn.close()
        }

        salesData.iterator
      }

    // 5. 按日期统计总销售额
    val dailySalesRDD: RDD[(String, Double)] = salesRDD
      .filter { case (date, _) => date != "invalid_date" && date.nonEmpty }  // 过滤无效日期
      .reduceByKey(_ + _)  // 累加每日销售额
      .sortByKey()  // 按日期排序

    // 6. 打印统计结果
    println("\n每日总销售额统计:")
    val result = dailySalesRDD.collect()
    result.foreach { case (date, total) =>
      println(f"$date: ￥${total}%.2f")  // 保留两位小数
    }
    // 验证总销售额（可选）
    val totalSales = result.map(_._2).sum
    println(f"\n总销售额合计: ￥${totalSales}%.2f")

    // 7. 将结果写入MySQL
    dailySalesRDD.foreachPartition { partition =>
      var conn: Connection = null
      var pstmt: PreparedStatement = null

      try {
        Class.forName(driver)
        conn = DriverManager.getConnection(url, user, password)
        conn.setAutoCommit(false)

        // 插入或更新每日销售额
        val sql = s"""INSERT INTO $resultTable (date, total_sales)
                     |VALUES (?, ?) ON DUPLICATE KEY UPDATE total_sales = ?""".stripMargin
        pstmt = conn.prepareStatement(sql)

        partition.foreach { case (date, total) =>
          pstmt.setString(1, date)
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
    val csvOutputPath = "C:/Users/lxy18/Desktop/daily_sales.csv"
    writeToCsv(dailySalesRDD, csvOutputPath)

    // 9. 关闭Spark上下文
    sc.stop()
  }

  // 创建每日销售额表（如果不存在）
  def createTableIfNotExists(driver: String, url: String, user: String, password: String, tableName: String): Unit = {
    var conn: Connection = null
    var stmt: Statement = null

    try {
      Class.forName(driver)
      conn = DriverManager.getConnection(url, user, password)
      stmt = conn.createStatement()

      // 关键修正：将//注释改为MySQL支持的--注释（注意双减号后加空格）
      val createTableSQL =
        s"""CREATE TABLE IF NOT EXISTS $tableName (
           |  date VARCHAR(20) NOT NULL PRIMARY KEY,  -- 日期（yyyy-MM-dd）
           |  total_sales DECIMAL(10, 2) NOT NULL     -- 总销售额（保留两位小数）
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

  // 写入CSV文件（解决乱码问题）
  def writeToCsv(rdd: RDD[(String, Double)], outputPath: String): Unit = {
    val header = "date,total_sales"
    val data = rdd.collect()

    try {
      val writer = new BufferedWriter(new OutputStreamWriter(
        new java.io.FileOutputStream(outputPath),
        StandardCharsets.UTF_8  // 指定UTF-8编码
      ))
      writer.write(header)
      writer.newLine()

      data.foreach { case (date, total) =>
        writer.write(f"$date,${total}%.2f")  // 保留两位小数
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
