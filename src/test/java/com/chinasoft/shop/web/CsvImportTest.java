package com.chinasoft.shop.web;

import com.chinasoft.shop.web.model.DailySales;
import com.chinasoft.shop.web.model.ProvinceCount;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import javax.sql.DataSource;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * CSV导入MySQL测试类
 * 用于测试将CSV文件数据导入到MySQL数据库
 */
@SpringBootTest
@ActiveProfiles("test")
public class CsvImportTest {

    @Autowired
    private DataSource dataSource;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * 测试导入每日销售数据CSV文件
     */
    @Test
    public void testImportDailySalesCsv() {
        String csvFilePath = "src/test/resources/daily_sales.csv";
        importDailySalesFromCsv(csvFilePath);
    }

    /**
     * 测试导入省份统计数据CSV文件
     */
    @Test
    public void testImportProvinceCountCsv() {
        String csvFilePath = "src/test/resources/province_count.csv";
        importProvinceCountFromCsv(csvFilePath);
    }

    /**
     * 从CSV文件导入每日销售数据
     * @param csvFilePath CSV文件路径
     */
    public void importDailySalesFromCsv(String csvFilePath) {
        List<DailySales> dailySalesList = new ArrayList<>();
        
        try (CSVReader reader = new CSVReader(new FileReader(csvFilePath))) {
            // 跳过标题行
            reader.readNext();
            
            String[] line;
            while ((line = reader.readNext()) != null) {
                if (line.length >= 2) {
                    DailySales dailySales = new DailySales();
                    dailySales.setDate(line[0]);
                    dailySales.setTotalSales(new BigDecimal(line[1]));
                    dailySalesList.add(dailySales);
                }
            }
            
            // 批量插入数据库
            String sql = "INSERT INTO daily_sales (date, total_sales) VALUES (?, ?) " +
                        "ON DUPLICATE KEY UPDATE total_sales = VALUES(total_sales)";
            
            for (DailySales dailySales : dailySalesList) {
                jdbcTemplate.update(sql, dailySales.getDate(), dailySales.getTotalSales());
            }
            
            System.out.println("成功导入 " + dailySalesList.size() + " 条每日销售数据");
            
        } catch (IOException | CsvValidationException e) {
            System.err.println("导入CSV文件失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 从CSV文件导入省份统计数据
     * @param csvFilePath CSV文件路径
     */
    public void importProvinceCountFromCsv(String csvFilePath) {
        List<ProvinceCount> provinceCountList = new ArrayList<>();
        
        try (CSVReader reader = new CSVReader(new FileReader(csvFilePath))) {
            // 跳过标题行
            reader.readNext();
            
            String[] line;
            while ((line = reader.readNext()) != null) {
                if (line.length >= 2) {
                    ProvinceCount provinceCount = new ProvinceCount();
                    provinceCount.setProvince(line[0]);
                    provinceCount.setCount(Integer.parseInt(line[1]));
                    provinceCountList.add(provinceCount);
                }
            }
            
            // 批量插入数据库
            String sql = "INSERT INTO num_of_province (province, count) VALUES (?, ?) " +
                        "ON DUPLICATE KEY UPDATE count = VALUES(count)";
            
            for (ProvinceCount provinceCount : provinceCountList) {
                jdbcTemplate.update(sql, provinceCount.getProvince(), provinceCount.getCount());
            }
            
            System.out.println("成功导入 " + provinceCountList.size() + " 条省份统计数据");
            
        } catch (IOException | CsvValidationException e) {
            System.err.println("导入CSV文件失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 通用CSV导入方法
     * @param csvFilePath CSV文件路径
     * @param tableName 目标表名
     * @param columns 列名数组
     */
    public void importGenericCsv(String csvFilePath, String tableName, String[] columns) {
        try (CSVReader reader = new CSVReader(new FileReader(csvFilePath))) {
            // 跳过标题行
            reader.readNext();
            
            String[] line;
            int count = 0;
            
            while ((line = reader.readNext()) != null) {
                if (line.length >= columns.length) {
                    // 构建INSERT语句
                    StringBuilder sql = new StringBuilder();
                    sql.append("INSERT INTO ").append(tableName).append(" (");
                    
                    for (int i = 0; i < columns.length; i++) {
                        if (i > 0) sql.append(", ");
                        sql.append(columns[i]);
                    }
                    
                    sql.append(") VALUES (");
                    
                    for (int i = 0; i < columns.length; i++) {
                        if (i > 0) sql.append(", ");
                        sql.append("?");
                    }
                    
                    sql.append(") ON DUPLICATE KEY UPDATE ");
                    
                    for (int i = 0; i < columns.length; i++) {
                        if (i > 0) sql.append(", ");
                        sql.append(columns[i]).append(" = VALUES(").append(columns[i]).append(")");
                    }
                    
                    // 执行插入
                    jdbcTemplate.update(sql.toString(), (Object[]) line);
                    count++;
                }
            }
            
            System.out.println("成功导入 " + count + " 条数据到表 " + tableName);
            
        } catch (IOException | CsvValidationException e) {
            System.err.println("导入CSV文件失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
}