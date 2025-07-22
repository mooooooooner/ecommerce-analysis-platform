# 电商用户行为数据分析与可视化平台

本项目是一个结合了后端大数据处理（Spark）和前端数据可视化（ECharts）的全栈数据分析平台。它通过分析电商用户行为日志数据，提取有价值的业务指标，并以一个美观、直观的Web仪表盘进行展示。

## ✨ 项目特色

- **技术栈分离**: 后端使用Spark进行大规模数据批处理，结果存入MySQL；前端通过Spring Boot API获取数据，实现前后端分离。
- **数据可视化**: 使用ECharts构建了一个简洁、大气的仪表盘，包含地图、折线图、柱状图、饼图等多种图表。
- **高可扩展性**: 项目结构清晰，无论是增加新的Spark分析任务还是前端图表都非常方便。
- **一站式体验**: 从数据处理到最终展示，提供了一个完整的数据项目生命周期范例。

## 🚀 技术栈

- **后端处理**: Apache Spark (Scala), MySQL
- **后端服务**: Spring Boot (Java), Spring Data JPA
- **前端展示**: HTML, CSS, JavaScript, ECharts
- **项目构建**: Maven

## 📊 当前已实现功能（写完记得更新）

1.  **用户省份分布统计**:
    -   通过Spark分析用户来源地，统计各省份的用户数量。
    -   前端使用中国地图和柱状图进行可视化。
2.  **每日销售额统计**:
    -   通过Spark分析订单数据，按天聚合总销售额。
    -   前端使用平滑的面积折线图展示销售趋势。
5.  **年龄段统计**:
    -   通过Spark分析用户年龄数据，统计各年龄段的用户数量。
6.  **商品类别统计**:
    -   通过Spark分析商品类别和销售额数据，统计各类别的总销售额（可统计不同级别分类）。

## 🏗️ 项目结构


    internship/
    ├── pom.xml                 # Maven项目配置
    ├── README.md               # 项目说明文档
    └── src/
        └── main/
            ├── java/
            │   └── com/chinasoft/shop/
            │       ├── scala/  # Spark分析任务 (Scala)
            │       └── web/    # Spring Boot后端服务 (Java)
            └── resources/
                ├── application.properties  # Spring Boot配置文件
                └── static/                 # 前端静态资源 (HTML/CSS/JS)
    

## 🏃‍♀️ 如何运行（记得最后再删除）

### 步骤1: 运行Spark分析任务

1.  确保你的MySQL数据库服务已启动，并且数据库 `shixi_keshe` 和源数据表 `data` 已存在。
2.  在你的IDE（如IntelliJ IDEA）中，右键点击 `ProvinceCount.scala` 和 `MoneyCount.scala` 文件，选择 `Run` 来执行它们。
3.  执行成功后，检查MySQL中是否已生成 `num_of_province` 和 `daily_sales` 两张结果表，并且包含数据。

### 步骤2: 启动后端Web服务

1.  打开 `src/main/resources/application.properties` 文件，**确认并修改**你的MySQL数据库连接信息（URL, username, password）。
2.  在IDE中找到 `ShopAnalysisApplication.java` 文件。
3.  右键点击该文件，选择 `Run 'ShopAnalysisApplication'`。
4.  等待控制台输出 "Started ShopAnalysisApplication in ... seconds"，表示后端服务已在 `http://localhost:8080` 启动。

### 步骤3: 查看前端页面

1.  打开你的浏览器（推荐Chrome或Firefox）。
2.  在地址栏输入 `http://localhost:8080`。
3.  你将看到数据分析仪表盘，图表会自动加载并显示数据。

---
    