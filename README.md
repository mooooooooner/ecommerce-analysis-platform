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

## 📊 当前已实现功能

1.  **用户省份分布统计**:
    -   通过Spark分析用户来源地，统计各省份的用户数量。
    -   前端使用中国地图和柱状图进行可视化。
2.  **每日销售额统计**:
    -   通过Spark分析订单数据，按天聚合总销售额。
    -   前端使用平滑的面积折线图展示销售趋势。

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
## 🏃‍♀️ 如何运行

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

## 🛠️ 如何为项目添加新的分析模块（给合作者的指南）

以添加 **“用户性别分布”** 统计为例，你需要完成以下步骤：

### 1. 数据库准备

在MySQL中执行以下SQL，创建用于存储性别统计结果的表：
CREATE TABLE IF NOT EXISTS gender_distribution (
  gender VARCHAR(10) NOT NULL PRIMARY KEY,
  count INT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;*(更多待开发的统计表SQL，请参考项目文档或代码注释)*

### 2. 编写新的Spark分析脚本

-   在 `src/main/java/com/chinasoft/shop/scala/` 目录下创建一个新的Scala文件，例如 `GenderCount.scala`。
-   编写Spark代码，从源数据表 `data` 读取数据，按性别（假设字段为`gender`）进行`count`统计。
-   将统计结果 `(性别, 数量)` 写入到 `gender_distribution` 表中。

### 3. 更新后端服务

1.  **创建Model**: 在 `web/model/` 目录下创建 `GenderDistribution.java` 类，映射数据库表。
2.  **创建Repository**: 在 `web/repository/` 目录下创建 `GenderDistributionRepository.java` 接口，继承 `JpaRepository`。
3.  **更新Controller**: 在 `DataViewController.java` 中，取消 `getGenderData()` 方法的注释，并实现从 `GenderDistributionRepository` 读取数据的逻辑。

    ```java
    // 注入Repository
    @Autowired
    private GenderDistributionRepository genderRepository;

    @GetMapping("/gender")
    public List<GenderDistribution> getGenderData() {
        return genderRepository.findAll();
    }
    ```

### 4. 更新前端图表

-   在 `index.html` 中，确保有一个用于显示性别分布的图表容器（已预留）。
-   在 `app.js` 中，找到 `initGenderChart` 函数，它已经编写好了从 `/api/data/gender` 获取数据并渲染饼图的逻辑。
-   当后端API准备好后，前端图表将自动从占位符状态变为真实的数据图表。

通过遵循以上步骤，你可以轻松地为平台添加更多的数据分析维度！
    