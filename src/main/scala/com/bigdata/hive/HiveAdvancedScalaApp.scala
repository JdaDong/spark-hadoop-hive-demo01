package com.bigdata.hive

import java.sql.{Connection, DriverManager, ResultSet, Statement}
import scala.util.{Failure, Success, Try, Using}

/**
 * Hive 高级特性 Scala 示例
 * 
 * 函数式编程风格实现
 */
object HiveAdvancedScalaApp {
  
  private val JDBC_URL = "jdbc:hive2://localhost:10000/default"
  private val USERNAME = "hadoop"
  private val PASSWORD = ""
  
  def main(args: Array[String]): Unit = {
    // 加载驱动
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    
    // 使用资源管理
    Using(DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD)) { conn =>
      println("========== Hive 高级特性 Scala 示例 ==========\n")
      
      // 函数式执行各个功能
      val features = List(
        ("分区表与动态分区", () => partitioningDemo(conn)),
        ("复杂数据类型", () => complexTypesDemo(conn)),
        ("窗口函数与分析", () => windowFunctionsDemo(conn)),
        ("事务表与ACID", () => transactionalDemo(conn)),
        ("性能优化技巧", () => optimizationDemo(conn)),
        ("高级查询模式", () => advancedQueriesDemo(conn))
      )
      
      features.foreach { case (name, func) =>
        executeFeature(name, func)
      }
      
      println("\n✓ 所有功能演示完成!")
      
    } match {
      case Success(_) => println("✓ 程序正常结束")
      case Failure(e) => e.printStackTrace()
    }
  }
  
  /**
   * 执行功能并处理异常
   */
  private def executeFeature(name: String, func: () => Unit): Unit = {
    Try(func()) match {
      case Success(_) => // 成功
      case Failure(e) => println(s"✗ $name 执行失败: ${e.getMessage}")
    }
  }
  
  /**
   * 1. 分区表与动态分区
   */
  private def partitioningDemo(conn: Connection): Unit = {
    println("========== 1. 分区表与动态分区 (Scala) ==========")
    
    withStatement(conn) { stmt =>
      // 创建分区表
      stmt.execute(
        """CREATE TABLE IF NOT EXISTS orders_partitioned (
          |    order_id INT,
          |    customer STRING,
          |    amount DOUBLE
          |) PARTITIONED BY (year INT, month INT, day INT)
          |STORED AS PARQUET
        """.stripMargin)
      
      println("✓ 创建多级分区表: orders_partitioned")
      
      // 创建源数据
      stmt.execute(
        """CREATE TABLE IF NOT EXISTS orders_source (
          |    order_id INT, customer STRING, amount DOUBLE,
          |    year INT, month INT, day INT)
        """.stripMargin)
      
      // 插入测试数据
      val orderData = List(
        (1, "Alice", 100.0, 2024, 1, 15),
        (2, "Bob", 200.0, 2024, 1, 16),
        (3, "Charlie", 150.0, 2024, 2, 10),
        (4, "David", 300.0, 2024, 2, 11)
      )
      
      orderData.foreach { case (id, customer, amount, y, m, d) =>
        stmt.execute(s"INSERT INTO orders_source VALUES ($id, '$customer', $amount, $y, $m, $d)")
      }
      
      println(s"✓ 插入 ${orderData.size} 条源数据")
      
      // 启用动态分区
      List(
        "SET hive.exec.dynamic.partition = true",
        "SET hive.exec.dynamic.partition.mode = nonstrict",
        "SET hive.exec.max.dynamic.partitions = 10000",
        "SET hive.exec.max.dynamic.partitions.pernode = 1000"
      ).foreach(stmt.execute)
      
      println("✓ 启用动态分区配置")
      
      // 动态分区插入
      stmt.execute(
        """INSERT OVERWRITE TABLE orders_partitioned PARTITION (year, month, day)
          |SELECT order_id, customer, amount, year, month, day
          |FROM orders_source
        """.stripMargin)
      
      println("✓ 动态分区插入完成")
      
      // 查询分区 (函数式风格)
      val partitions = executeQuery(stmt, "SHOW PARTITIONS orders_partitioned") { rs =>
        Iterator.continually(rs).takeWhile(_.next()).map(_.getString(1)).toList
      }
      
      println(s"✓ 自动创建的分区 (${partitions.size}个):")
      partitions.foreach(p => println(s"  - $p"))
      
      // 分区统计
      val stats = partitions.groupBy(_.split("/")(0)).view.mapValues(_.size)
      println("✓ 分区统计:")
      stats.foreach { case (year, count) => println(s"  - $year: $count 个分区") }
    }
    
    println()
  }
  
  /**
   * 2. 复杂数据类型
   */
  private def complexTypesDemo(conn: Connection): Unit = {
    println("========== 2. 复杂数据类型 (Scala) ==========")
    
    withStatement(conn) { stmt =>
      // 创建复杂类型表
      stmt.execute(
        """CREATE TABLE IF NOT EXISTS products_complex (
          |    product_id INT,
          |    name STRING,
          |    tags ARRAY<STRING>,
          |    attributes MAP<STRING, STRING>,
          |    specs STRUCT<weight:DOUBLE, dimensions:STRING, color:STRING>
          |) STORED AS ORC
        """.stripMargin)
      
      println("✓ 创建包含 Array/Map/Struct 的表")
      
      // 插入复杂数据
      stmt.execute(
        """INSERT INTO products_complex VALUES (
          |    1, 'Laptop',
          |    array('Electronics', 'Computer', 'Premium'),
          |    map('brand', 'Dell', 'model', 'XPS', 'warranty', '3years'),
          |    named_struct('weight', 1.5, 'dimensions', '30x20x2cm', 'color', 'Silver')
          |)
        """.stripMargin)
      
      stmt.execute(
        """INSERT INTO products_complex VALUES (
          |    2, 'Phone',
          |    array('Electronics', 'Mobile', 'Smartphone'),
          |    map('brand', 'Apple', 'model', 'iPhone', 'storage', '256GB'),
          |    named_struct('weight', 0.2, 'dimensions', '15x7x0.8cm', 'color', 'Black')
          |)
        """.stripMargin)
      
      println("✓ 插入复杂类型数据")
      
      // Array 操作 (函数式风格)
      println("\n✓ Array 操作:")
      val arrayResults = executeQuery(stmt,
        """SELECT name, 
          |    tags[0] as first_tag,
          |    size(tags) as tag_count,
          |    array_contains(tags, 'Electronics') as is_electronic
          |FROM products_complex
        """.stripMargin) { rs =>
        Iterator.continually(rs).takeWhile(_.next()).map { r =>
          (r.getString("name"), r.getString("first_tag"), 
           r.getInt("tag_count"), r.getBoolean("is_electronic"))
        }.toList
      }
      
      arrayResults.foreach { case (name, tag, count, isElec) =>
        println(s"  - $name: 首标签=$tag, 标签数=$count, 电子产品=$isElec")
      }
      
      // Map 操作
      println("\n✓ Map 操作:")
      val mapResults = executeQuery(stmt,
        """SELECT name,
          |    attributes['brand'] as brand,
          |    attributes['model'] as model,
          |    size(attributes) as attr_count,
          |    map_keys(attributes) as keys
          |FROM products_complex
        """.stripMargin) { rs =>
        Iterator.continually(rs).takeWhile(_.next()).map { r =>
          (r.getString("name"), r.getString("brand"), 
           r.getString("model"), r.getInt("attr_count"))
        }.toList
      }
      
      mapResults.foreach { case (name, brand, model, count) =>
        println(s"  - $name: $brand $model (属性数: $count)")
      }
      
      // Struct 操作
      println("\n✓ Struct 操作:")
      val structResults = executeQuery(stmt,
        """SELECT name,
          |    specs.weight as weight,
          |    specs.color as color,
          |    specs.dimensions as dimensions
          |FROM products_complex
        """.stripMargin) { rs =>
        Iterator.continually(rs).takeWhile(_.next()).map { r =>
          (r.getString("name"), r.getDouble("weight"), r.getString("color"))
        }.toList
      }
      
      structResults.foreach { case (name, weight, color) =>
        println(f"  - $name: ${weight}kg, $color")
      }
      
      // LATERAL VIEW + explode
      println("\n✓ LATERAL VIEW 展开:")
      val exploded = executeQuery(stmt,
        """SELECT name, tag
          |FROM products_complex
          |LATERAL VIEW explode(tags) tagTable AS tag
        """.stripMargin) { rs =>
        Iterator.continually(rs).takeWhile(_.next()).map { r =>
          (r.getString("name"), r.getString("tag"))
        }.toList
      }
      
      exploded.groupBy(_._1).foreach { case (name, tags) =>
        println(s"  - $name: ${tags.map(_._2).mkString(", ")}")
      }
    }
    
    println()
  }
  
  /**
   * 3. 窗口函数与分析
   */
  private def windowFunctionsDemo(conn: Connection): Unit = {
    println("========== 3. 窗口函数与分析 (Scala) ==========")
    
    withStatement(conn) { stmt =>
      // 创建销售数据表
      stmt.execute(
        """CREATE TABLE IF NOT EXISTS daily_sales (
          |    sale_date STRING,
          |    region STRING,
          |    product STRING,
          |    amount DOUBLE
          |)
        """.stripMargin)
      
      // 插入测试数据 (函数式生成)
      val salesData = for {
        region <- List("North", "South", "East", "West")
        day <- 1 to 5
      } yield (f"2024-01-$day%02d", region, s"Product${day % 3}", 100.0 + scala.util.Random.nextInt(200))
      
      salesData.foreach { case (date, region, product, amount) =>
        stmt.execute(s"INSERT INTO daily_sales VALUES ('$date', '$region', '$product', $amount)")
      }
      
      println(s"✓ 插入 ${salesData.size} 条销售数据")
      
      // 排名窗口函数
      println("\n✓ 排名函数 (ROW_NUMBER, RANK, DENSE_RANK):")
      val rankings = executeQuery(stmt,
        """SELECT region, product, amount,
          |    ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) as row_num,
          |    RANK() OVER (PARTITION BY region ORDER BY amount DESC) as rank,
          |    DENSE_RANK() OVER (PARTITION BY region ORDER BY amount DESC) as dense_rank
          |FROM daily_sales
        """.stripMargin) { rs =>
        Iterator.continually(rs).takeWhile(_.next()).map { r =>
          (r.getString("region"), r.getString("product"), r.getDouble("amount"),
           r.getInt("row_num"), r.getInt("rank"), r.getInt("dense_rank"))
        }.toList.take(10)
      }
      
      rankings.foreach { case (region, product, amount, rowNum, rank, denseRank) =>
        println(f"  - $region%-6s $product%-10s: $$${amount}%.2f (ROW:$rowNum, RANK:$rank, DENSE:$denseRank)")
      }
      
      // 聚合窗口函数
      println("\n✓ 聚合窗口函数:")
      val aggregates = executeQuery(stmt,
        """SELECT region, product, amount,
          |    SUM(amount) OVER (PARTITION BY region) as region_total,
          |    AVG(amount) OVER (PARTITION BY region) as region_avg,
          |    COUNT(*) OVER (PARTITION BY region) as region_count
          |FROM daily_sales
        """.stripMargin) { rs =>
        Iterator.continually(rs).takeWhile(_.next()).map { r =>
          (r.getString("region"), r.getDouble("amount"), 
           r.getDouble("region_total"), r.getDouble("region_avg"))
        }.toList.take(8)
      }
      
      aggregates.foreach { case (region, amount, total, avg) =>
        println(f"  - $region%-6s: 金额=$$${amount}%.2f, 区域总计=$$${total}%.2f, 区域平均=$$${avg}%.2f")
      }
      
      // LEAD/LAG - 前后比较
      println("\n✓ LEAD/LAG 函数 (同比分析):")
      val leadLag = executeQuery(stmt,
        """SELECT region, sale_date, amount,
          |    LAG(amount, 1, 0) OVER (PARTITION BY region ORDER BY sale_date) as prev_day,
          |    LEAD(amount, 1, 0) OVER (PARTITION BY region ORDER BY sale_date) as next_day,
          |    amount - LAG(amount, 1, 0) OVER (PARTITION BY region ORDER BY sale_date) as diff
          |FROM daily_sales
        """.stripMargin) { rs =>
        Iterator.continually(rs).takeWhile(_.next()).map { r =>
          (r.getString("region"), r.getString("sale_date"), 
           r.getDouble("amount"), r.getDouble("diff"))
        }.toList.take(10)
      }
      
      leadLag.foreach { case (region, date, amount, diff) =>
        val change = if (diff > 0) s"+$$${diff}" else if (diff < 0) s"-$$${-diff}" else "="
        println(f"  - $region%-6s $date: $$${amount}%.2f ($change)")
      }
      
      // 累计汇总
      println("\n✓ 累计汇总:")
      val cumulative = executeQuery(stmt,
        """SELECT region, sale_date, amount,
          |    SUM(amount) OVER (PARTITION BY region ORDER BY sale_date 
          |        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_sum
          |FROM daily_sales
          |ORDER BY region, sale_date
        """.stripMargin) { rs =>
        Iterator.continually(rs).takeWhile(_.next()).map { r =>
          (r.getString("region"), r.getString("sale_date"), 
           r.getDouble("amount"), r.getDouble("cumulative_sum"))
        }.toList.take(10)
      }
      
      cumulative.foreach { case (region, date, amount, cumSum) =>
        println(f"  - $region%-6s $date: 当日=$$${amount}%.2f, 累计=$$${cumSum}%.2f")
      }
    }
    
    println()
  }
  
  /**
   * 4. 事务表与ACID
   */
  private def transactionalDemo(conn: Connection): Unit = {
    println("========== 4. 事务表与ACID (Scala) ==========")
    
    withStatement(conn) { stmt =>
      // 创建事务表
      stmt.execute(
        """CREATE TABLE IF NOT EXISTS inventory_transactional (
          |    item_id INT,
          |    item_name STRING,
          |    quantity INT,
          |    price DOUBLE
          |) STORED AS ORC
          |TBLPROPERTIES (
          |    'transactional' = 'true',
          |    'orc.compress' = 'SNAPPY'
          |)
        """.stripMargin)
      
      println("✓ 创建事务表: inventory_transactional")
      
      // 初始数据
      val items = List(
        (1, "Laptop", 10, 1000.0),
        (2, "Mouse", 50, 25.0),
        (3, "Keyboard", 30, 50.0),
        (4, "Monitor", 15, 300.0)
      )
      
      items.foreach { case (id, name, qty, price) =>
        stmt.execute(s"INSERT INTO inventory_transactional VALUES ($id, '$name', $qty, $price)")
      }
      
      println(s"✓ 插入 ${items.size} 条初始库存")
      
      // UPDATE 操作 (函数式风格)
      val updates = List(
        ("Laptop", 5, "售出5台笔记本"),
        ("Mouse", 10, "售出10个鼠标")
      )
      
      updates.foreach { case (item, qty, desc) =>
        stmt.execute(s"UPDATE inventory_transactional SET quantity = quantity - $qty WHERE item_name = '$item'")
        println(s"✓ UPDATE: $desc")
      }
      
      // DELETE 操作
      stmt.execute("DELETE FROM inventory_transactional WHERE quantity < 10")
      println("✓ DELETE: 删除库存不足10的商品")
      
      // 查询当前状态
      println("\n✓ 当前库存状态:")
      val inventory = executeQuery(stmt,
        """SELECT item_name, quantity, price, 
          |    quantity * price as total_value
          |FROM inventory_transactional
          |ORDER BY total_value DESC
        """.stripMargin) { rs =>
        Iterator.continually(rs).takeWhile(_.next()).map { r =>
          (r.getString("item_name"), r.getInt("quantity"), 
           r.getDouble("price"), r.getDouble("total_value"))
        }.toList
      }
      
      inventory.foreach { case (name, qty, price, value) =>
        println(f"  - $name%-10s: 数量=$qty%3d, 单价=$$${price}%.2f, 总值=$$${value}%.2f")
      }
      
      // 统计
      val totalValue = inventory.map(_._4).sum
      println(f"\n✓ 库存总价值: $$${totalValue}%.2f")
      
      // ACID 特性说明
      println("\n✓ ACID 保证:")
      println("  - Atomicity: 事务中的操作要么全部成功,要么全部失败")
      println("  - Consistency: 数据保持一致性状态")
      println("  - Isolation: 并发事务互不干扰")
      println("  - Durability: 提交后的数据持久化")
    }
    
    println()
  }
  
  /**
   * 5. 性能优化技巧
   */
  private def optimizationDemo(conn: Connection): Unit = {
    println("========== 5. 性能优化技巧 (Scala) ==========")
    
    withStatement(conn) { stmt =>
      // 性能优化配置 (函数式风格)
      val optimizations = Map(
        "Map-side JOIN" -> List(
          "SET hive.auto.convert.join = true",
          "SET hive.mapjoin.smalltable.filesize = 25000000"
        ),
        "并行执行" -> List(
          "SET hive.exec.parallel = true",
          "SET hive.exec.parallel.thread.number = 16"
        ),
        "向量化查询" -> List(
          "SET hive.vectorized.execution.enabled = true",
          "SET hive.vectorized.execution.reduce.enabled = true"
        ),
        "CBO优化器" -> List(
          "SET hive.cbo.enable = true",
          "SET hive.compute.query.using.stats = true",
          "SET hive.stats.autogather = true"
        ),
        "谓词下推" -> List(
          "SET hive.optimize.ppd = true",
          "SET hive.optimize.index.filter = true"
        ),
        "动态分区裁剪" -> List(
          "SET hive.optimize.partition.columns.separate = true"
        ),
        "小文件合并" -> List(
          "SET hive.merge.mapfiles = true",
          "SET hive.merge.mapredfiles = true",
          "SET hive.merge.size.per.task = 268435456",
          "SET hive.merge.smallfiles.avgsize = 16777216"
        ),
        "输出压缩" -> List(
          "SET hive.exec.compress.output = true",
          "SET mapreduce.output.fileoutputformat.compress.codec = org.apache.hadoop.io.compress.SnappyCodec"
        )
      )
      
      println("✓ 应用性能优化配置:\n")
      optimizations.foreach { case (category, settings) =>
        println(s"  [$category]")
        settings.foreach { setting =>
          Try(stmt.execute(setting)) match {
            case Success(_) => println(s"    ✓ ${setting.split("SET ")(1)}")
            case Failure(_) => println(s"    ✗ ${setting.split("SET ")(1)}")
          }
        }
        println()
      }
      
      // 查询优化建议
      println("✓ 查询优化最佳实践:")
      val bestPractices = List(
        "使用分区裁剪 - WHERE 包含分区列",
        "使用列裁剪 - 避免 SELECT *",
        "小表在 JOIN 左侧 - 利用 Map-side JOIN",
        "使用 EXPLAIN 分析执行计划",
        "定期更新表统计信息 - ANALYZE TABLE",
        "使用合适的文件格式 - ORC/Parquet",
        "合理设置分桶数 - 2^n 个桶",
        "避免数据倾斜 - 使用 DISTRIBUTE BY",
        "使用物化视图缓存常用查询",
        "合理设置 Reducer 数量"
      )
      
      bestPractices.zipWithIndex.foreach { case (practice, idx) =>
        println(f"  ${idx + 1}%2d. $practice")
      }
    }
    
    println()
  }
  
  /**
   * 6. 高级查询模式
   */
  private def advancedQueriesDemo(conn: Connection): Unit = {
    println("========== 6. 高级查询模式 (Scala) ==========")
    
    withStatement(conn) { stmt =>
      // CTE (公共表表达式) - 函数式风格
      println("✓ CTE (WITH 子句) - 多级查询:")
      val cteResults = executeQuery(stmt,
        """WITH 
          |    region_sales AS (
          |        SELECT region, SUM(amount) as total
          |        FROM daily_sales
          |        GROUP BY region
          |    ),
          |    ranked_regions AS (
          |        SELECT region, total,
          |            RANK() OVER (ORDER BY total DESC) as rank
          |        FROM region_sales
          |    )
          |SELECT * FROM ranked_regions WHERE rank <= 3
        """.stripMargin) { rs =>
        Iterator.continually(rs).takeWhile(_.next()).map { r =>
          (r.getString("region"), r.getDouble("total"), r.getInt("rank"))
        }.toList
      }
      
      cteResults.foreach { case (region, total, rank) =>
        println(f"  $rank. $region%-6s: $$${total}%.2f")
      }
      
      // CASE WHEN - 数据分类
      println("\n✓ CASE WHEN 表达式:")
      val categorized = executeQuery(stmt,
        """SELECT product, amount,
          |    CASE
          |        WHEN amount < 100 THEN 'Low'
          |        WHEN amount < 200 THEN 'Medium'
          |        ELSE 'High'
          |    END as category
          |FROM daily_sales
        """.stripMargin) { rs =>
        Iterator.continually(rs).takeWhile(_.next()).map { r =>
          (r.getString("product"), r.getDouble("amount"), r.getString("category"))
        }.toList.take(10)
      }
      
      val grouped = categorized.groupBy(_._3).view.mapValues(_.size)
      println("  分类统计:")
      grouped.foreach { case (category, count) =>
        println(s"    - $category: $count 条")
      }
      
      // GROUPING SETS - 多维分析
      println("\n✓ GROUPING SETS (多维度汇总):")
      Try {
        val groupingSets = executeQuery(stmt,
          """SELECT region, product, SUM(amount) as total
            |FROM daily_sales
            |GROUP BY region, product
            |GROUPING SETS (
            |    (region, product),
            |    (region),
            |    (product),
            |    ()
            |)
            |ORDER BY region NULLS LAST, product NULLS LAST
          """.stripMargin) { rs =>
          Iterator.continually(rs).takeWhile(_.next()).map { r =>
            (Option(r.getString("region")), 
             Option(r.getString("product")), 
             r.getDouble("total"))
          }.toList.take(15)
        }
        
        groupingSets.foreach {
          case (Some(r), Some(p), total) => println(f"  - $r%-6s $p%-10s: $$${total}%.2f")
          case (Some(r), None, total) => println(f"  - $r%-6s 小计: $$${total}%.2f")
          case (None, Some(p), total) => println(f"  - [全部] $p%-10s: $$${total}%.2f")
          case (None, None, total) => println(f"  - 总计: $$${total}%.2f")
        }
      } match {
        case Success(_) => 
        case Failure(_) => println("  注意: GROUPING SETS 需要 Hive 0.11+ 版本")
      }
      
      // 相关子查询
      println("\n✓ 相关子查询 (找出高于区域平均值的记录):")
      val correlated = executeQuery(stmt,
        """SELECT s1.region, s1.product, s1.amount
          |FROM daily_sales s1
          |WHERE s1.amount > (
          |    SELECT AVG(s2.amount)
          |    FROM daily_sales s2
          |    WHERE s1.region = s2.region
          |)
        """.stripMargin) { rs =>
        Iterator.continually(rs).takeWhile(_.next()).map { r =>
          (r.getString("region"), r.getString("product"), r.getDouble("amount"))
        }.toList.take(10)
      }
      
      correlated.groupBy(_._1).foreach { case (region, records) =>
        println(s"  - $region (${records.size}条): ${records.map(r => f"$$${r._3}%.2f").mkString(", ")}")
      }
    }
    
    println()
  }
  
  // ========== 辅助函数 ==========
  
  /**
   * 资源管理 - Statement
   */
  private def withStatement[T](conn: Connection)(f: Statement => T): T = {
    Using(conn.createStatement())(f).get
  }
  
  /**
   * 执行查询并处理结果
   */
  private def executeQuery[T](stmt: Statement, query: String)(f: ResultSet => T): T = {
    Using(stmt.executeQuery(query))(f).get
  }
}
