package com.bigdata.phoenix

import org.slf4j.{Logger, LoggerFactory}
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Statement, Timestamp}

/**
 * Apache Phoenix Scala 版本
 * 
 * 演示 Phoenix 的核心功能：
 * 1. SQL 查询和 DML 操作
 * 2. 二级索引
 * 3. 视图和序列
 * 4. 事务支持
 * 5. 与 Spark 集成
 */
object PhoenixScalaApp {
  
  private val LOG: Logger = LoggerFactory.getLogger(getClass)
  private val PHOENIX_URL = "jdbc:phoenix:localhost:2181"
  
  def main(args: Array[String]): Unit = {
    // 加载驱动
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    
    LOG.info("=== Apache Phoenix Scala 示例 ===")
    
    var conn: Connection = null
    try {
      conn = DriverManager.getConnection(PHOENIX_URL)
      
      // 1. 表操作
      tableOperations(conn)
      
      // 2. 高级查询
      advancedQueries(conn)
      
      // 3. 用户自定义函数
      userDefinedFunctions(conn)
      
      // 4. 动态列
      dynamicColumns(conn)
      
      // 5. 数组类型
      arrayOperations(conn)
      
      // 6. 性能监控
      performanceMonitoring(conn)
      
      LOG.info("=== Phoenix Scala 示例完成 ===")
      
    } catch {
      case e: Exception => LOG.error("Phoenix error", e)
    } finally {
      if (conn != null) conn.close()
    }
  }
  
  /**
   * 1. 表操作
   */
  private def tableOperations(conn: Connection): Unit = {
    LOG.info("\n=== 1. 表操作 ===")
    
    val stmt = conn.createStatement()
    
    try {
      // 创建表（带压缩和 TTL）
      val createTableSQL = """
        CREATE TABLE IF NOT EXISTS products (
          product_id VARCHAR PRIMARY KEY,
          category VARCHAR,
          name VARCHAR,
          price DOUBLE,
          stock INTEGER,
          tags VARCHAR ARRAY,
          create_time TIMESTAMP
        ) 
        COMPRESSION='SNAPPY',
        TTL=2592000
        """
      
      stmt.execute(createTableSQL)
      LOG.info("Table 'products' created with compression and TTL")
      
      // 插入数据
      val upsertSQL = """
        UPSERT INTO products 
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
      
      val pstmt = conn.prepareStatement(upsertSQL)
      
      val products = Seq(
        ("P001", "Electronics", "Laptop", 5999.0, 100, Array("new", "hot"), new Timestamp(System.currentTimeMillis())),
        ("P002", "Electronics", "Phone", 3999.0, 200, Array("hot", "sale"), new Timestamp(System.currentTimeMillis())),
        ("P003", "Books", "Scala Programming", 89.0, 500, Array("programming"), new Timestamp(System.currentTimeMillis())),
        ("P004", "Electronics", "Tablet", 2999.0, 150, Array("new"), new Timestamp(System.currentTimeMillis()))
      )
      
      products.foreach { case (id, cat, name, price, stock, tags, time) =>
        pstmt.setString(1, id)
        pstmt.setString(2, cat)
        pstmt.setString(3, name)
        pstmt.setDouble(4, price)
        pstmt.setInt(5, stock)
        pstmt.setArray(6, conn.createArrayOf("VARCHAR", tags.asInstanceOf[Array[Object]]))
        pstmt.setTimestamp(7, time)
        pstmt.addBatch()
      }
      
      pstmt.executeBatch()
      conn.commit()
      LOG.info(s"Inserted ${products.length} products")
      
      pstmt.close()
      
    } finally {
      stmt.close()
    }
  }
  
  /**
   * 2. 高级查询
   */
  private def advancedQueries(conn: Connection): Unit = {
    LOG.info("\n=== 2. 高级查询 ===")
    
    // 窗口函数
    val windowSQL = """
      SELECT 
        category,
        name,
        price,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) as rank,
        AVG(price) OVER (PARTITION BY category) as avg_price
      FROM products
      """
    
    LOG.info("Window function query:")
    executeQuery(conn, windowSQL)
    
    // CASE WHEN
    val caseSQL = """
      SELECT 
        name,
        price,
        CASE 
          WHEN price > 3000 THEN 'High'
          WHEN price > 1000 THEN 'Medium'
          ELSE 'Low'
        END as price_range
      FROM products
      ORDER BY price DESC
      """
    
    LOG.info("\nCase when query:")
    executeQuery(conn, caseSQL)
    
    // 子查询
    val subQuerySQL = """
      SELECT * FROM products
      WHERE price > (SELECT AVG(price) FROM products)
      ORDER BY price DESC
      """
    
    LOG.info("\nSubquery - products above average price:")
    executeQuery(conn, subQuerySQL)
  }
  
  /**
   * 3. 用户自定义函数（UDF）
   */
  private def userDefinedFunctions(conn: Connection): Unit = {
    LOG.info("\n=== 3. UDF 示例 ===")
    
    // Phoenix 内置函数
    val builtinSQL = """
      SELECT 
        name,
        price,
        ROUND(price * 0.8, 2) as discount_price,
        UPPER(category) as upper_category,
        LENGTH(name) as name_length,
        SUBSTRING(product_id, 2, 3) as id_part
      FROM products
      """
    
    LOG.info("Built-in functions:")
    executeQuery(conn, builtinSQL)
    
    // 日期函数
    val dateSQL = """
      SELECT 
        name,
        create_time,
        TO_CHAR(create_time, 'yyyy-MM-dd') as date_str,
        CURRENT_DATE() as today,
        CURRENT_TIME() as now
      FROM products
      LIMIT 2
      """
    
    LOG.info("\nDate functions:")
    executeQuery(conn, dateSQL)
  }
  
  /**
   * 4. 动态列
   * Phoenix 支持动态访问 HBase 列
   */
  private def dynamicColumns(conn: Connection): Unit = {
    LOG.info("\n=== 4. 动态列 ===")
    
    val stmt = conn.createStatement()
    
    try {
      // 创建视图映射到 HBase 表（不指定所有列）
      val createViewSQL = """
        CREATE VIEW IF NOT EXISTS dynamic_products (
          pk VARCHAR PRIMARY KEY,
          info.name VARCHAR,
          info.price DOUBLE
        )
        AS SELECT * FROM products
        """
      
      stmt.execute(createViewSQL)
      LOG.info("Dynamic view created")
      
      // 查询固定列
      val query1 = "SELECT pk, info.name, info.price FROM dynamic_products"
      LOG.info("Query fixed columns:")
      executeQuery(conn, query1)
      
    } finally {
      stmt.close()
    }
  }
  
  /**
   * 5. 数组操作
   */
  private def arrayOperations(conn: Connection): Unit = {
    LOG.info("\n=== 5. 数组操作 ===")
    
    // 查询数组
    val arraySQL = """
      SELECT 
        name,
        tags,
        ARRAY_LENGTH(tags) as tag_count
      FROM products
      WHERE ARRAY_CONTAINS(tags, 'hot')
      """
    
    LOG.info("Products with 'hot' tag:")
    executeQuery(conn, arraySQL)
    
    // 数组聚合
    val arrayAggSQL = """
      SELECT 
        category,
        ARRAY_AGG(name) as product_names
      FROM products
      GROUP BY category
      """
    
    LOG.info("\nProducts grouped by category:")
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(arrayAggSQL)
    
    while (rs.next()) {
      val category = rs.getString("category")
      val names = rs.getArray("product_names")
      LOG.info(s"Category: $category | Products: ${names.getArray.asInstanceOf[Array[String]].mkString(", ")}")
    }
    
    rs.close()
    stmt.close()
  }
  
  /**
   * 6. 性能监控
   */
  private def performanceMonitoring(conn: Connection): Unit = {
    LOG.info("\n=== 6. 性能监控 ===")
    
    val stmt = conn.createStatement()
    
    try {
      // 查看表统计信息
      LOG.info("Table statistics:")
      
      // 执行计划
      val explainSQL = """
        EXPLAIN 
        SELECT * FROM products 
        WHERE category = 'Electronics' 
        AND price > 3000
        """
      
      LOG.info("\nExecution plan:")
      val rs = stmt.executeQuery(explainSQL)
      while (rs.next()) {
        LOG.info(s"  ${rs.getString(1)}")
      }
      rs.close()
      
      // 查询成本估算
      LOG.info("\nQuery hints:")
      LOG.info("  USE_SORT_MERGE_JOIN - Force sort-merge join")
      LOG.info("  NO_INDEX - Skip secondary index")
      LOG.info("  SERIAL - Force serial execution")
      
      // 带 hint 的查询
      val hintSQL = """
        SELECT /*+ NO_INDEX */ * 
        FROM products 
        WHERE category = 'Electronics'
        """
      
      LOG.info("\nQuery with NO_INDEX hint:")
      executeQuery(conn, hintSQL)
      
    } finally {
      stmt.close()
    }
  }
  
  /**
   * 辅助方法：执行查询
   */
  private def executeQuery(conn: Connection, sql: String): Unit = {
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(sql)
    val meta = rs.getMetaData
    val columnCount = meta.getColumnCount
    
    while (rs.next()) {
      val row = (1 to columnCount).map { i =>
        s"${meta.getColumnName(i)}: ${rs.getString(i)}"
      }.mkString(" | ")
      LOG.info(s"  $row")
    }
    
    rs.close()
    stmt.close()
  }
}

/**
 * Phoenix 与 Spark 集成
 */
object PhoenixSparkIntegration {
  
  private val LOG: Logger = LoggerFactory.getLogger(getClass)
  
  /**
   * 使用 Spark 读取 Phoenix 表
   */
  def readFromPhoenix(): Unit = {
    LOG.info("=== Spark + Phoenix 集成 ===")
    
    // Spark 代码示例（需要 phoenix-spark 依赖）
    val phoenixConfig = """
      |import org.apache.spark.sql.SparkSession
      |
      |val spark = SparkSession.builder()
      |  .appName("Phoenix Integration")
      |  .master("local[*]")
      |  .getOrCreate()
      |
      |// 读取 Phoenix 表
      |val df = spark.read
      |  .format("org.apache.phoenix.spark")
      |  .option("table", "users")
      |  .option("zkUrl", "localhost:2181")
      |  .load()
      |
      |df.show()
      |
      |// 写入 Phoenix 表
      |df.write
      |  .format("org.apache.phoenix.spark")
      |  .option("table", "users_backup")
      |  .option("zkUrl", "localhost:2181")
      |  .mode("overwrite")
      |  .save()
      |""".stripMargin
    
    LOG.info("Spark + Phoenix code example:\n{}", phoenixConfig)
  }
}
