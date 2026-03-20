package com.bigdata.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}

/**
 * Spark + Hive 数据处理 - Scala 实现
 * 演示 Spark SQL 高级特性和复杂数据处理
 */
object SparkHiveApplication {
  
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  
  def main(args: Array[String]): Unit = {
    // 创建 SparkSession with Hive 支持
    val spark = SparkSession.builder()
      .appName("Spark Hive Scala Application")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()
    
    try {
      // 设置日志级别
      spark.sparkContext.setLogLevel("WARN")
      
      // 1. 创建数据库和表
      setupHiveDatabase(spark)
      
      // 2. 批量数据处理
      batchDataProcessing(spark)
      
      // 3. 复杂查询和窗口函数
      complexQueryWithWindowFunctions(spark)
      
      // 4. DataFrame API 操作
      dataFrameOperations(spark)
      
      // 5. 表连接操作
      tableJoinOperations(spark)
      
    } catch {
      case e: Exception =>
        logger.error("Error in Spark Hive application", e)
    } finally {
      spark.stop()
    }
  }
  
  /**
   * 设置 Hive 数据库和表
   */
  def setupHiveDatabase(spark: SparkSession): Unit = {
    logger.info("Setting up Hive database...")
    
    import spark.sql
    
    sql("CREATE DATABASE IF NOT EXISTS bigdata_db")
    sql("USE bigdata_db")
    
    // 创建产品表
    sql("DROP TABLE IF EXISTS products")
    sql(
      """CREATE TABLE IF NOT EXISTS products (
        |  product_id INT,
        |  product_name STRING,
        |  category STRING,
        |  price DOUBLE,
        |  stock INT
        |) STORED AS PARQUET
        |""".stripMargin)
    
    // 插入产品数据
    sql(
      """INSERT INTO products VALUES
        |  (101, 'Laptop Pro', 'Electronics', 1299.99, 50),
        |  (102, 'Wireless Mouse', 'Electronics', 29.99, 200),
        |  (103, 'Desk Chair', 'Furniture', 199.99, 75),
        |  (104, 'Notebook', 'Stationery', 4.99, 500),
        |  (105, '4K Monitor', 'Electronics', 399.99, 30),
        |  (106, 'Standing Desk', 'Furniture', 449.99, 25),
        |  (107, 'Pen Set', 'Stationery', 12.99, 300),
        |  (108, 'USB-C Hub', 'Electronics', 59.99, 100)
        |""".stripMargin)
    
    logger.info("Database and tables created successfully")
  }
  
  /**
   * 批量数据处理
   */
  def batchDataProcessing(spark: SparkSession): Unit = {
    logger.info("Performing batch data processing...")
    
    import spark.sql
    
    // 创建订单表
    sql("DROP TABLE IF EXISTS orders")
    sql(
      """CREATE TABLE IF NOT EXISTS orders (
        |  order_id INT,
        |  product_id INT,
        |  quantity INT,
        |  order_date STRING,
        |  customer_id INT
        |) STORED AS PARQUET
        |""".stripMargin)
    
    // 插入订单数据
    sql(
      """INSERT INTO orders VALUES
        |  (2001, 101, 2, '2026-03-01', 501),
        |  (2002, 102, 5, '2026-03-02', 502),
        |  (2003, 103, 1, '2026-03-03', 503),
        |  (2004, 101, 1, '2026-03-04', 504),
        |  (2005, 105, 3, '2026-03-05', 505),
        |  (2006, 104, 10, '2026-03-06', 501),
        |  (2007, 106, 1, '2026-03-07', 506),
        |  (2008, 102, 3, '2026-03-08', 502)
        |""".stripMargin)
    
    // 订单统计
    val orderStats = sql(
      """SELECT
        |  COUNT(*) as total_orders,
        |  SUM(quantity) as total_quantity,
        |  COUNT(DISTINCT customer_id) as unique_customers
        |FROM orders
        |""".stripMargin)
    
    logger.info("Order Statistics:")
    orderStats.show()
  }
  
  /**
   * 复杂查询和窗口函数
   */
  def complexQueryWithWindowFunctions(spark: SparkSession): Unit = {
    logger.info("Executing complex queries with window functions...")
    
    import spark.sql
    
    // 使用窗口函数排名
    val rankedProducts = sql(
      """SELECT
        |  product_id,
        |  product_name,
        |  category,
        |  price,
        |  RANK() OVER (PARTITION BY category ORDER BY price DESC) as price_rank,
        |  ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) as row_num
        |FROM products
        |ORDER BY category, price_rank
        |""".stripMargin)
    
    logger.info("Ranked Products by Category:")
    rankedProducts.show()
    
    // 计算每个类别的价格统计
    val categoryStats = sql(
      """SELECT
        |  category,
        |  COUNT(*) as product_count,
        |  AVG(price) as avg_price,
        |  MIN(price) as min_price,
        |  MAX(price) as max_price,
        |  SUM(stock * price) as inventory_value
        |FROM products
        |GROUP BY category
        |ORDER BY inventory_value DESC
        |""".stripMargin)
    
    logger.info("Category Statistics:")
    categoryStats.show()
  }
  
  /**
   * DataFrame API 操作
   */
  def dataFrameOperations(spark: SparkSession): Unit = {
    logger.info("Performing DataFrame operations...")
    
    import spark.implicits._
    
    // 读取产品表
    val productsDF = spark.table("products")
    
    // 使用 DataFrame API 进行转换
    val expensiveProducts = productsDF
      .filter($"price" > 100)
      .select($"product_name", $"category", $"price", $"stock")
      .withColumn("total_value", $"price" * $"stock")
      .orderBy($"total_value".desc)
    
    logger.info("Expensive Products (price > 100):")
    expensiveProducts.show()
    
    // 分组聚合
    val categoryAgg = productsDF
      .groupBy("category")
      .agg(
        count("*").as("count"),
        avg("price").as("avg_price"),
        sum("stock").as("total_stock")
      )
      .orderBy($"avg_price".desc)
    
    logger.info("Category Aggregation:")
    categoryAgg.show()
  }
  
  /**
   * 表连接操作
   */
  def tableJoinOperations(spark: SparkSession): Unit = {
    logger.info("Performing table join operations...")
    
    import spark.implicits._
    
    // 读取表
    val productsDF = spark.table("products")
    val ordersDF = spark.table("orders")
    
    // Join 操作
    val orderDetails = ordersDF
      .join(productsDF, "product_id")
      .select(
        $"order_id",
        $"product_name",
        $"category",
        $"quantity",
        $"price",
        ($"quantity" * $"price").as("order_amount"),
        $"order_date",
        $"customer_id"
      )
      .orderBy($"order_id")
    
    logger.info("Order Details:")
    orderDetails.show()
    
    // 计算每个客户的订单统计
    val customerStats = orderDetails
      .groupBy("customer_id")
      .agg(
        count("*").as("order_count"),
        sum("order_amount").as("total_spent"),
        avg("order_amount").as("avg_order_amount")
      )
      .orderBy($"total_spent".desc)
    
    logger.info("Customer Statistics:")
    customerStats.show()
    
    // 最畅销产品
    val topSellingProducts = orderDetails
      .groupBy("product_name", "category")
      .agg(
        sum("quantity").as("total_sold"),
        sum("order_amount").as("total_revenue")
      )
      .orderBy($"total_revenue".desc)
    
    logger.info("Top Selling Products:")
    topSellingProducts.show()
  }
}
