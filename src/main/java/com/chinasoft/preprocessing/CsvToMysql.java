package com.chinasoft.preprocessing;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;

public class CsvToMysql {
    private static final String URL_TEMPLATE = "jdbc:mysql://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    private static final int BATCH_SIZE = 1000;

    public static void main(String[] args) {
        Map<String, String> mysqlConfig = new HashMap<>();
        mysqlConfig.put("user", "root");
        mysqlConfig.put("password", "lxy040310");
        mysqlConfig.put("host", "localhost");
        mysqlConfig.put("port", "3306");
        mysqlConfig.put("db", "shixi_keshe");
        mysqlConfig.put("table", "data");

        String csvFilePath = "C:/Users/lxy18/Desktop/Electronic_product_sales_analysis.csv";
        String encoding = detectEncoding(csvFilePath);

        // 1. 预览数据格式
        previewDataFormat(csvFilePath, encoding);

        // 2. 读取CSV文件
        List<String[]> csvData = readCsvFile(csvFilePath, encoding);
        if (csvData == null || csvData.isEmpty()) {
            System.out.println("未读取到数据，程序退出");
            return;
        }

        // 3. 数据预处理
        List<Map<String, Object>> processedData = preprocessData(csvData);
        if (processedData == null || processedData.isEmpty()) {
            System.out.println("数据预处理失败，程序退出");
            return;
        }

        // 4. 创建数据库
        if (!createDatabase(mysqlConfig)) {
            System.out.println("数据库创建失败，程序退出");
            return;
        }

        // 5. 写入MySQL表
        writeToMysql(mysqlConfig, processedData);
    }

    // 检测文件编码
    private static String detectEncoding(String filePath) {
        try (InputStream is = new FileInputStream(filePath)) {
            byte[] bytes = new byte[4096];
            int read = is.read(bytes);
            if (read > 3 && bytes[0] == (byte) 0xEF && bytes[1] == (byte) 0xBB && bytes[2] == (byte) 0xBF) {
                return "UTF-8";
            }
            return "GBK";
        } catch (IOException e) {
            System.out.println("检测编码失败，使用默认编码GBK");
            return "GBK";
        }
    }

    // 预览数据格式
    private static void previewDataFormat(String filePath, String encoding) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                new FileInputStream(filePath), encoding))) {
            System.out.println("数据原始格式预览：");
            String line;
            int count = 0;
            while ((line = reader.readLine()) != null && count < 5) {
                System.out.println(line);
                count++;
            }
        } catch (IOException e) {
            System.out.println("预览数据格式失败：" + e.getMessage());
        }
    }

    // 读取CSV文件
    private static List<String[]> readCsvFile(String filePath, String encoding) {
        try (CSVReader reader = new CSVReaderBuilder(
                new InputStreamReader(new FileInputStream(filePath), encoding))
                .build()) {
            List<String[]> allData = reader.readAll();
            System.out.printf("成功读取CSV文件，共 %d 行数据%n", allData.size());
            return allData;
        } catch (IOException | CsvException e) {
            System.out.println("读取CSV失败：" + e.getMessage());
            return null;
        }
    }

    // 数据预处理
    private static List<Map<String, Object>> preprocessData(List<String[]> csvData) {
        List<Map<String, Object>> result = new ArrayList<>();

        if (csvData.isEmpty()) {
            return result;
        }

        // 获取表头
        String[] headers = csvData.get(0);
        List<String> headerList = Arrays.asList(headers);

        // 检查必要列
        List<String> requiredColumns = Arrays.asList("id", "event_time", "order_id", "product_id");
        List<String> missingCols = new ArrayList<>();
        for (String col : requiredColumns) {
            if (!headerList.contains(col)) {
                missingCols.add(col);
            }
        }

        if (!missingCols.isEmpty()) {
            System.out.printf("错误：CSV文件缺少必要列：%s%n", missingCols);
            System.out.printf("实际列名：%s%n", headerList);
            return null;
        }

        // 处理数据行
        for (int i = 1; i < csvData.size(); i++) {
            String[] row = csvData.get(i);
            if (row.length < headers.length) {
                continue; // 跳过不完整的行
            }

            Map<String, Object> processedRow = new HashMap<>();
            for (int j = 0; j < headers.length; j++) {
                String header = headers[j];
                String value = row[j];

                if (value == null || value.trim().isEmpty() || "nan".equalsIgnoreCase(value.trim())) {
                    processedRow.put(header, null);
                    continue;
                }

                // 处理不同类型的列
                switch (header) {
                    case "id":
                    case "age":
                        try {
                            processedRow.put(header, Integer.parseInt(value.trim()));
                        } catch (NumberFormatException e) {
                            processedRow.put(header, null);
                        }
                        break;
                    case "event_time":
                        processedRow.put(header, parseDate(value.trim()));
                        break;
                    case "price":
                        try {
                            processedRow.put(header, Double.parseDouble(value.trim()));
                        } catch (NumberFormatException e) {
                            processedRow.put(header, null);
                        }
                        break;
                    default:
                        processedRow.put(header, value.trim());
                }
            }
            result.add(processedRow);
        }

        System.out.println("数据预处理完成");
        return result;
    }

    // 解析日期
    private static LocalDate parseDate(String dateStr) {
        List<DateTimeFormatter> formatters = Arrays.asList(
                DateTimeFormatter.ISO_LOCAL_DATE,
                DateTimeFormatter.ofPattern("yyyy/M/d"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX")
        );

        for (DateTimeFormatter formatter : formatters) {
            try {
                return LocalDate.parse(dateStr, formatter);
            } catch (DateTimeParseException e) {
                // 尝试下一个格式
            }
        }

        System.out.printf("无法解析日期格式：%s%n", dateStr);
        return null;
    }

    // 创建数据库
    private static boolean createDatabase(Map<String, String> config) {
        String url = String.format("jdbc:mysql://%s:%s/?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
                config.get("host"), config.get("port"));

        try (Connection conn = DriverManager.getConnection(
                url, config.get("user"), config.get("password"));
             Statement stmt = conn.createStatement()) {

            String dbName = config.get("db");
            String createDbSql = String.format(
                    "CREATE DATABASE IF NOT EXISTS %s CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
                    dbName);
            stmt.executeUpdate(createDbSql);

            System.out.printf("数据库 %s 已准备就绪%n", dbName);
            return true;
        } catch (SQLException e) {
            System.out.println("MySQL连接失败：" + e.getMessage());
            System.out.println("请检查：1. MySQL服务是否启动；2. 用户名密码是否正确；3. 端口是否正确");
            return false;
        }
    }

    // 写入MySQL表
    private static void writeToMysql(Map<String, String> config, List<Map<String, Object>> data) {
        String url = String.format(URL_TEMPLATE,
                config.get("host"),
                Integer.parseInt(config.get("port")),
                config.get("db"));

        try (Connection conn = DriverManager.getConnection(
                url, config.get("user"), config.get("password"))) {

            // 关闭自动提交，使用事务
            conn.setAutoCommit(false);

            // 创建表
            createTable(conn, config.get("table"));

            // 准备SQL插入语句
            String sql = buildInsertSql(config.get("table"), data.get(0));
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                int count = 0;

                for (Map<String, Object> row : data) {
                    setParams(pstmt, row);
                    pstmt.addBatch();
                    count++;

                    if (count % BATCH_SIZE == 0) {
                        pstmt.executeBatch();
                        conn.commit();
                        System.out.printf("已处理 %d 条记录%n", count);
                    }
                }

                // 处理剩余批次
                if (count % BATCH_SIZE != 0) {
                    pstmt.executeBatch();
                    conn.commit();
                    System.out.printf("已处理 %d 条记录%n", count);
                }
            }

            System.out.printf("数据已成功写入表 %s.%s%n",
                    config.get("db"), config.get("table"));
        } catch (SQLException e) {
            System.out.println("数据写入失败：" + e.getMessage());
        }
    }

    // 创建表
    private static void createTable(Connection conn, String tableName) throws SQLException {
        String dropTableSql = "DROP TABLE IF EXISTS " + tableName;
        String createTableSql = "CREATE TABLE " + tableName + " (" +
                "id INT, " +
                "event_time DATE, " +
                "order_id VARCHAR(50), " +
                "product_id VARCHAR(50), " +
                "category_id VARCHAR(50), " +
                "category_code VARCHAR(100), " +
                "brand VARCHAR(50), " +
                "price FLOAT, " +
                "user_id VARCHAR(50), " +
                "age INT, " +
                "sex VARCHAR(10), " +
                "local VARCHAR(50)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci";

        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(dropTableSql);
            stmt.executeUpdate(createTableSql);
        }
    }

    // 构建插入SQL
    private static String buildInsertSql(String tableName, Map<String, Object> row) {
        StringBuilder columns = new StringBuilder();
        StringBuilder placeholders = new StringBuilder();

        for (String key : row.keySet()) {
            if (columns.length() > 0) {
                columns.append(", ");
                placeholders.append(", ");
            }
            columns.append("`").append(key).append("`");
            placeholders.append("?");
        }

        return String.format("INSERT INTO %s (%s) VALUES (%s)",
                tableName, columns, placeholders);
    }

    // 设置预处理语句参数
    private static void setParams(PreparedStatement pstmt, Map<String, Object> row) throws SQLException {
        int index = 1;
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            Object value = entry.getValue();
            if (value == null) {
                pstmt.setNull(index, getSqlType(entry.getKey()));
            } else if (value instanceof LocalDate) {
                pstmt.setDate(index, java.sql.Date.valueOf((LocalDate) value));
            } else if (value instanceof Integer) {
                pstmt.setInt(index, (Integer) value);
            } else if (value instanceof Double) {
                pstmt.setDouble(index, (Double) value);
            } else {
                pstmt.setString(index, value.toString());
            }
            index++;
        }
    }

    // 获取SQL类型
    private static int getSqlType(String columnName) {
        switch (columnName) {
            case "id":
            case "age":
                return Types.INTEGER;
            case "event_time":
                return Types.DATE;
            case "price":
                return Types.FLOAT;
            default:
                return Types.VARCHAR;
        }
    }
}
