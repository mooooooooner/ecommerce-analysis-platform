package com.chinasoft.preprocessing;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;

import java.io.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CsvProcessor {
    public static void main(String[] args) {
        String inputFile = "C:/Users/lxy18/Desktop/Electronic_product_sales_analysis.csv";
        String outputFile = "output_with_date_processed.csv";
        String encoding = detectEncoding(inputFile);

        try (CSVReader reader = new CSVReaderBuilder(
                new InputStreamReader(new FileInputStream(inputFile), encoding))
                .withSkipLines(1)  // 跳过标题行
                .build();
             CSVWriter writer = new CSVWriter(
                     new OutputStreamWriter(new FileOutputStream(outputFile), "UTF-8"),
                     CSVWriter.DEFAULT_SEPARATOR,
                     CSVWriter.NO_QUOTE_CHARACTER,
                     CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                     CSVWriter.DEFAULT_LINE_END)) {

            // 读取所有数据行
            List<String[]> allData = reader.readAll();

            // 处理数据
            List<String[]> processedData = new ArrayList<>();

            // 添加标题行，id在第一列
            String[] headers = {"id"};
            String[] originalHeaders = readHeaders(inputFile, encoding);
            List<String> headerList = new ArrayList<>(Arrays.asList(originalHeaders));
            int idIndex = headerList.indexOf("id");
            if (idIndex != -1) {
                headerList.remove(idIndex);
            }
            headerList.addAll(1, Arrays.asList(headers));
            processedData.add(headerList.toArray(new String[0]));

            // 处理每一行数据
            for (int i = 0; i < allData.size(); i++) {
                String[] row = allData.get(i);
                List<String> processedRow = new ArrayList<>(Arrays.asList(row));

                // 1. 设置id列（从0开始）
                if (idIndex != -1) {
                    processedRow.set(idIndex, String.valueOf(i));
                } else {
                    processedRow.add(0, String.valueOf(i));
                }

                // 2. 处理event_time列，只保留年月日
                int eventTimeIndex = headerList.indexOf("event_time");
                if (eventTimeIndex != -1 && eventTimeIndex < processedRow.size()) {
                    String eventTime = processedRow.get(eventTimeIndex);
                    try {
                        // 尝试解析日期时间
                        LocalDate date = parseDateTime(eventTime);
                        processedRow.set(eventTimeIndex, date.toString());
                    } catch (Exception e) {
                        // 解析失败，保持原样或设为null
                        processedRow.set(eventTimeIndex, "");
                    }
                }

                processedData.add(processedRow.toArray(new String[0]));
            }

            // 写入处理后的数据
            writer.writeAll(processedData);

            // 打印前5行验证结果
            System.out.println("处理后的数据（前5行）：");
            for (int i = 1; i <= Math.min(5, processedData.size() - 1); i++) {
                String[] row = processedData.get(i);
                int idIdx = Arrays.asList(processedData.get(0)).indexOf("id");
                int timeIdx = Arrays.asList(processedData.get(0)).indexOf("event_time");
                System.out.printf("id: %s, event_time: %s%n",
                        idIdx != -1 ? row[idIdx] : "N/A",
                        timeIdx != -1 ? row[timeIdx] : "N/A");
            }

        } catch (IOException | CsvException e) {
            e.printStackTrace();
        }
    }

    // 检测文件编码
    private static String detectEncoding(String filePath) {
        try (InputStream is = new FileInputStream(filePath)) {
            byte[] bytes = new byte[4096];
            int read = is.read(bytes);
            if (read > 3 && bytes[0] == (byte)0xEF && bytes[1] == (byte)0xBB && bytes[2] == (byte)0xBF) {
                return "UTF-8";
            }
            // 这里可以添加更复杂的编码检测逻辑
            return "GBK"; // 默认使用GBK
        } catch (IOException e) {
            return "GBK"; // 出错时默认使用GBK
        }
    }

    // 读取标题行
    private static String[] readHeaders(String filePath, String encoding) throws IOException {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(filePath), encoding))) {
            String line = reader.readLine();
            return line != null ? line.split(",") : new String[0];
        }
    }

    // 解析日期时间字符串，提取日期部分
    private static LocalDate parseDateTime(String dateTimeStr) {
        // 处理常见的日期时间格式
        try {
            // 尝试ISO格式：2023-01-01T12:00:00
            return LocalDate.parse(dateTimeStr.substring(0, 10));
        } catch (Exception e1) {
            try {
                // 尝试其他常见格式
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                return LocalDate.parse(dateTimeStr, formatter);
            } catch (Exception e2) {
                try {
                    // 尝试带时区的格式
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX");
                    return LocalDate.parse(dateTimeStr, formatter);
                } catch (Exception e3) {
                    // 可以添加更多格式尝试...
                    throw new IllegalArgumentException("Unsupported date format: " + dateTimeStr);
                }
            }
        }
    }
}
