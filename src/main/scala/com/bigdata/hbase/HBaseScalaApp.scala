package com.bigdata.hbase

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

/**
 * HBase 数据操作 - Scala 实现
 * 演示 HBase 高级特性和过滤器
 */
object HBaseScalaApp {
  
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val TABLE_NAME = "product_catalog"
  private val CF_INFO = "info"
  private val CF_STATS = "stats"
  
  def main(args: Array[String]): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "localhost")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    
    val connection = ConnectionFactory.createConnection(conf)
    
    try {
      // 1. 创建表
      createTable(connection)
      
      // 2. 插入产品数据
      insertProducts(connection)
      
      // 3. 使用过滤器查询
      queryWithFilters(connection)
      
      // 4. 范围扫描
      rangeScan(connection)
      
      // 5. 多版本数据
      multiVersionData(connection)
      
      // 6. 批量读取
      batchGet(connection)
      
      // 7. 条件更新
      conditionalUpdate(connection)
      
    } catch {
      case e: Exception =>
        logger.error("Error in HBase Scala application", e)
    } finally {
      connection.close()
      logger.info("HBase connection closed")
    }
  }
  
  /**
   * 创建表
   */
  def createTable(connection: Connection): Unit = {
    val admin = connection.getAdmin
    
    try {
      val tableName = TableName.valueOf(TABLE_NAME)
      
      if (admin.tableExists(tableName)) {
        logger.info(s"Table $TABLE_NAME already exists, deleting it...")
        admin.disableTable(tableName)
        admin.deleteTable(tableName)
      }
      
      // 创建表描述
      val tableDescBuilder = TableDescriptorBuilder.newBuilder(tableName)
      
      // 添加列族
      val infoFamily = ColumnFamilyDescriptorBuilder
        .newBuilder(Bytes.toBytes(CF_INFO))
        .setMaxVersions(5)
        .build()
      
      val statsFamily = ColumnFamilyDescriptorBuilder
        .newBuilder(Bytes.toBytes(CF_STATS))
        .setMaxVersions(5)
        .build()
      
      tableDescBuilder.setColumnFamily(infoFamily)
      tableDescBuilder.setColumnFamily(statsFamily)
      
      admin.createTable(tableDescBuilder.build())
      logger.info(s"Table $TABLE_NAME created successfully")
      
    } finally {
      admin.close()
    }
  }
  
  /**
   * 插入产品数据
   */
  def insertProducts(connection: Connection): Unit = {
    val table = connection.getTable(TableName.valueOf(TABLE_NAME))
    
    try {
      val products = List(
        ("prod001", "Laptop", "Electronics", "1299.99", "50", "100"),
        ("prod002", "Mouse", "Electronics", "29.99", "200", "450"),
        ("prod003", "Chair", "Furniture", "199.99", "75", "120"),
        ("prod004", "Notebook", "Stationery", "4.99", "500", "800"),
        ("prod005", "Monitor", "Electronics", "399.99", "30", "85")
      )
      
      val puts = products.map { case (id, name, category, price, stock, sold) =>
        val put = new Put(Bytes.toBytes(id))
        put.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("name"), Bytes.toBytes(name))
        put.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("category"), Bytes.toBytes(category))
        put.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("price"), Bytes.toBytes(price))
        put.addColumn(Bytes.toBytes(CF_STATS), Bytes.toBytes("stock"), Bytes.toBytes(stock))
        put.addColumn(Bytes.toBytes(CF_STATS), Bytes.toBytes("sold"), Bytes.toBytes(sold))
        put
      }
      
      table.put(puts.asJava)
      logger.info(s"Inserted ${products.size} products")
      
    } finally {
      table.close()
    }
  }
  
  /**
   * 使用过滤器查询
   */
  def queryWithFilters(connection: Connection): Unit = {
    val table = connection.getTable(TableName.valueOf(TABLE_NAME))
    
    try {
      import org.apache.hadoop.hbase.filter._
      
      logger.info("=== Querying Electronics products ===")
      
      // 创建单列值过滤器
      val filter = new SingleColumnValueFilter(
        Bytes.toBytes(CF_INFO),
        Bytes.toBytes("category"),
        CompareOperator.EQUAL,
        Bytes.toBytes("Electronics")
      )
      filter.setFilterIfMissing(true)
      
      val scan = new Scan()
      scan.setFilter(filter)
      
      val scanner = table.getScanner(scan)
      
      scanner.asScala.foreach { result =>
        val rowKey = Bytes.toString(result.getRow)
        val name = Bytes.toString(result.getValue(Bytes.toBytes(CF_INFO), Bytes.toBytes("name")))
        val price = Bytes.toString(result.getValue(Bytes.toBytes(CF_INFO), Bytes.toBytes("price")))
        val stock = Bytes.toString(result.getValue(Bytes.toBytes(CF_STATS), Bytes.toBytes("stock")))
        
        logger.info(s"Product: $rowKey, Name: $name, Price: $price, Stock: $stock")
      }
      
      scanner.close()
      
    } finally {
      table.close()
    }
  }
  
  /**
   * 范围扫描
   */
  def rangeScan(connection: Connection): Unit = {
    val table = connection.getTable(TableName.valueOf(TABLE_NAME))
    
    try {
      logger.info("=== Range scan from prod002 to prod004 ===")
      
      val scan = new Scan()
      scan.withStartRow(Bytes.toBytes("prod002"))
      scan.withStopRow(Bytes.toBytes("prod005"))
      
      val scanner = table.getScanner(scan)
      
      scanner.asScala.foreach { result =>
        val rowKey = Bytes.toString(result.getRow)
        val name = Bytes.toString(result.getValue(Bytes.toBytes(CF_INFO), Bytes.toBytes("name")))
        val category = Bytes.toString(result.getValue(Bytes.toBytes(CF_INFO), Bytes.toBytes("category")))
        
        logger.info(s"RowKey: $rowKey, Name: $name, Category: $category")
      }
      
      scanner.close()
      
    } finally {
      table.close()
    }
  }
  
  /**
   * 多版本数据
   */
  def multiVersionData(connection: Connection): Unit = {
    val table = connection.getTable(TableName.valueOf(TABLE_NAME))
    
    try {
      // 更新同一个单元格多次
      val rowKey = "prod001"
      
      for (i <- 1 to 3) {
        val put = new Put(Bytes.toBytes(rowKey))
        put.addColumn(Bytes.toBytes(CF_STATS), Bytes.toBytes("stock"), 
          Bytes.toBytes(s"${50 - i * 5}"))
        table.put(put)
        Thread.sleep(100) // 确保时间戳不同
      }
      
      logger.info(s"=== Multi-version data for $rowKey ===")
      
      // 读取多个版本
      val get = new Get(Bytes.toBytes(rowKey))
      get.readVersions(5)
      get.addColumn(Bytes.toBytes(CF_STATS), Bytes.toBytes("stock"))
      
      val result = table.get(get)
      val cells = result.getColumnCells(Bytes.toBytes(CF_STATS), Bytes.toBytes("stock"))
      
      cells.asScala.foreach { cell =>
        val value = Bytes.toString(CellUtil.cloneValue(cell))
        val timestamp = cell.getTimestamp
        logger.info(s"Version - Timestamp: $timestamp, Value: $value")
      }
      
    } finally {
      table.close()
    }
  }
  
  /**
   * 批量读取
   */
  def batchGet(connection: Connection): Unit = {
    val table = connection.getTable(TableName.valueOf(TABLE_NAME))
    
    try {
      logger.info("=== Batch get multiple products ===")
      
      val gets = List("prod001", "prod003", "prod005").map { rowKey =>
        new Get(Bytes.toBytes(rowKey))
      }
      
      val results = table.get(gets.asJava)
      
      results.foreach { result =>
        if (!result.isEmpty) {
          val rowKey = Bytes.toString(result.getRow)
          val name = Bytes.toString(result.getValue(Bytes.toBytes(CF_INFO), Bytes.toBytes("name")))
          val price = Bytes.toString(result.getValue(Bytes.toBytes(CF_INFO), Bytes.toBytes("price")))
          
          logger.info(s"RowKey: $rowKey, Name: $name, Price: $price")
        }
      }
      
    } finally {
      table.close()
    }
  }
  
  /**
   * 条件更新
   */
  def conditionalUpdate(connection: Connection): Unit = {
    val table = connection.getTable(TableName.valueOf(TABLE_NAME))
    
    try {
      val rowKey = "prod002"
      
      // 只有当 stock > 100 时才更新
      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes(CF_STATS), Bytes.toBytes("sold"), Bytes.toBytes("500"))
      
      val success = table.checkAndMutate(Bytes.toBytes(rowKey), Bytes.toBytes(CF_STATS))
        .qualifier(Bytes.toBytes("stock"))
        .ifMatches(CompareOperator.GREATER, Bytes.toBytes("100"))
        .thenPut(put)
      
      if (success) {
        logger.info(s"Conditional update succeeded for $rowKey")
      } else {
        logger.info(s"Conditional update failed for $rowKey (condition not met)")
      }
      
    } finally {
      table.close()
    }
  }
}
