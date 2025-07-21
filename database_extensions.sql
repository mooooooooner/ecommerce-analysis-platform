-- 1. 用户性别分布表
-- 用于存储按性别统计的用户数量。
-- Spark任务应计算每个性别的用户数并存入此表。
CREATE TABLE IF NOT EXISTS gender_distribution (
  gender VARCHAR(10) NOT NULL PRIMARY KEY, -- 性别 (例如: '男性', '女性', '未知')
  count INT NOT NULL                      -- 对应性别的用户数量
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- 2. 品牌销售统计表
-- 用于存储各品牌的总销售额或销量。
-- Spark任务应按品牌分组，计算总销售额。
CREATE TABLE IF NOT EXISTS brand_sales (
  brand VARCHAR(100) NOT NULL PRIMARY KEY,     -- 品牌名称
  total_sales DECIMAL(15, 2) NOT NULL,         -- 总销售额
  total_count BIGINT NOT NULL                  -- 总销量 (可选)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- 3. 用户年龄段分布表
-- 用于存储不同年龄段的用户数量。
-- Spark任务需要先根据用户出生日期或年龄字段计算年龄段，然后统计数量。
CREATE TABLE IF NOT EXISTS age_distribution (
  age_group VARCHAR(20) NOT NULL PRIMARY KEY, -- 年龄段 (例如: '18-24岁', '25-34岁')
  count INT NOT NULL                          -- 该年龄段的用户数量
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- 4. 商品类别销售统计表
-- 用于存储各商品类别的总销售额或销量。
-- Spark任务应按商品类别分组，计算总销售额。
CREATE TABLE IF NOT EXISTS category_sales (
  category VARCHAR(100) NOT NULL PRIMARY KEY,   -- 商品类别名称
  total_sales DECIMAL(15, 2) NOT NULL           -- 总销售额
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    