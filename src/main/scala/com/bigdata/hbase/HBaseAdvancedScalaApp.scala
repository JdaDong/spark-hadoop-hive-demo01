package com.bigdata.hbase

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * HBase 高级特性 - Scala 实现
 * 演示协处理器、二级索引、性能优化等高级功能
 */
object HBaseAdvancedScalaApp {
  
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val TABLE_NAME = "advanced_scala"
  private val CF_DATA = "data"
  private val CF_INDEX = "index"
  
  def main(args: Array[String]): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "localhost")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    
    val connection = ConnectionFactory.createConnection(conf)
    
    try {
      // 1. 表预分区(Pre-splitting)
      createPreSplitTable(connection)
      
      // 2. 批量加载(Bulk Load)优化
      bulkLoadOptimization(connection)
      
      // 3. 扫描优化技巧
      scanOptimization(connection)
      
      // 4. 自定义过滤器
      customFilterDemo(connection)
      
      // 5. 多版本数据查询
      multiVersionQuery(connection)
      
      // 6. 行锁和并发控制
      rowLockDemo(connection)
      
      // 7. 协处理器概念演示
      coprocessorDemo()
      
      // 8. 二级索引实现思路
      secondaryIndexDemo(connection)
      
      // 9. 热点问题优化
      hotspotOptimization()
      
      // 10. 性能监控和调优
      performanceMonitoring(connection)
      
    } catch {
      case e: Exception =>
        logger.error("Error in HBase Advanced Scala application", e)
    } finally {
      connection.close()
      logger.info("HBase connection closed")
    }
  }
  
  /**
   * 1. 表预分区(Pre-splitting)
   */
  def createPreSplitTable(connection: Connection): Unit = {
    logger.info("=== Pre-Split Table Demo ===")
    
    val admin = connection.getAdmin
    
    try {
      val tableName = TableName.valueOf(TABLE_NAME)
      
      if (admin.tableExists(tableName)) {
        admin.disableTable(tableName)
        admin.deleteTable(tableName)
      }
      
      // 定义分区键
      val splitKeys = Array(
        Bytes.toBytes("row100"),
        Bytes.toBytes("row200"),
        Bytes.toBytes("row300"),
        Bytes.toBytes("row400"),
        Bytes.toBytes("row500")
      )
      
      val tableDescBuilder = TableDescriptorBuilder.newBuilder(tableName)
      
      val dataFamily = ColumnFamilyDescriptorBuilder
        .newBuilder(Bytes.toBytes(CF_DATA))
        .setMaxVersions(5)
        .setCompressionType(Compression.Algorithm.SNAPPY)
        .setBloomFilterType(BloomType.ROW)
        .build()
      
      val indexFamily = ColumnFamilyDescriptorBuilder
        .newBuilder(Bytes.toBytes(CF_INDEX))
        .setMaxVersions(1)
        .setInMemory(true)  // 索引数据常驻内存
        .build()
      
      tableDescBuilder.setColumnFamily(dataFamily)
      tableDescBuilder.setColumnFamily(indexFamily)
      
      // 创建预分区表
      admin.createTable(tableDescBuilder.build(), splitKeys)
      
      logger.info(s"Pre-split table created with ${splitKeys.length} regions")
      
      // 显示分区信息
      val regions = admin.getRegions(tableName)
      logger.info(s"Total regions: ${regions.size()}")
      
    } finally {
      admin.close()
    }
  }
  
  /**
   * 2. 批量加载优化
   */
  def bulkLoadOptimization(connection: Connection): Unit = {
    logger.info("=== Bulk Load Optimization ===")
    
    val table = connection.getTable(TableName.valueOf(TABLE_NAME))
    
    try {
      // 使用 BufferedMutator 进行批量写入
      val params = new BufferedMutatorParams(TableName.valueOf(TABLE_NAME))
        .writeBufferSize(10 * 1024 * 1024)  // 10MB buffer
      
      val mutator = connection.getBufferedMutator(params)
      
      val startTime = System.currentTimeMillis()
      
      // 批量生成数据
      (1 to 5000).foreach { i =>
        val rowKey = f"row$i%05d"
        val put = new Put(Bytes.toBytes(rowKey))
        put.addColumn(Bytes.toBytes(CF_DATA), Bytes.toBytes("name"), 
          Bytes.toBytes(s"User_$i"))
        put.addColumn(Bytes.toBytes(CF_DATA), Bytes.toBytes("age"), 
          Bytes.toBytes(s"${20 + i % 50}"))
        put.addColumn(Bytes.toBytes(CF_DATA), Bytes.toBytes("score"), 
          Bytes.toBytes(s"${i % 100}"))
        
        mutator.mutate(put)
        
        // 定期刷新,避免内存过大
        if (i % 1000 == 0) {
          mutator.flush()
          logger.info(s"Flushed $i records")
        }
      }
      
      mutator.flush()
      mutator.close()
      
      val endTime = System.currentTimeMillis()
      logger.info(s"Bulk load completed in ${endTime - startTime} ms")
      
    } finally {
      table.close()
    }
  }
  
  /**
   * 3. 扫描优化技巧
   */
  def scanOptimization(connection: Connection): Unit = {
    logger.info("=== Scan Optimization ===")
    
    val table = connection.getTable(TableName.valueOf(TABLE_NAME))
    
    try {
      // 优化1: 限制扫描范围
      logger.info("\n1. Range Scan:")
      val rangeScan = new Scan()
      rangeScan.withStartRow(Bytes.toBytes("row00100"))
      rangeScan.withStopRow(Bytes.toBytes("row00200"))
      rangeScan.setCaching(100)  // 设置缓存行数
      rangeScan.setBatch(10)     // 每次RPC返回的列数
      
      var count1 = 0
      val scanner1 = table.getScanner(rangeScan)
      scanner1.asScala.foreach(_ => count1 += 1)
      scanner1.close()
      logger.info(s"Range scan found $count1 records")
      
      // 优化2: 指定列族和列
      logger.info("\n2. Column-specific Scan:")
      val columnScan = new Scan()
      columnScan.addColumn(Bytes.toBytes(CF_DATA), Bytes.toBytes("name"))
      columnScan.addColumn(Bytes.toBytes(CF_DATA), Bytes.toBytes("age"))
      columnScan.setMaxResultSize(1024 * 1024)  // 限制结果大小
      
      // 优化3: 使用 FirstKeyOnlyFilter 只获取行键
      logger.info("\n3. Row Key Only Scan:")
      val keyOnlyScan = new Scan()
      keyOnlyScan.setFilter(new FirstKeyOnlyFilter())
      keyOnlyScan.setMaxResultSize(10 * 1024 * 1024)
      
      var count3 = 0
      val scanner3 = table.getScanner(keyOnlyScan)
      scanner3.asScala.take(10).foreach { result =>
        val rowKey = Bytes.toString(result.getRow)
        count3 += 1
      }
      scanner3.close()
      logger.info(s"Key-only scan processed $count3 records")
      
      // 优化4: 使用 KeyOnlyFilter 节省网络传输
      logger.info("\n4. Key Only Filter:")
      val keyOnlyFilter = new KeyOnlyFilter()
      val scan4 = new Scan()
      scan4.setFilter(keyOnlyFilter)
      scan4.setCaching(1000)
      
      // 优化5: 并行扫描(使用 Scan 的 reversed 功能)
      logger.info("\n5. Reversed Scan:")
      val reversedScan = new Scan()
      reversedScan.setReversed(true)
      reversedScan.setMaxResultSize(100)
      
      val scanner5 = table.getScanner(reversedScan)
      val results = scanner5.asScala.take(5).toList
      results.foreach { result =>
        logger.info(s"Reversed: ${Bytes.toString(result.getRow)}")
      }
      scanner5.close()
      
    } finally {
      table.close()
    }
  }
  
  /**
   * 4. 自定义过滤器组合
   */
  def customFilterDemo(connection: Connection): Unit = {
    logger.info("=== Custom Filter Demo ===")
    
    val table = connection.getTable(TableName.valueOf(TABLE_NAME))
    
    try {
      // 复杂过滤器组合
      val filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE)
      
      // 条件1: age > 50
      val filter1 = new SingleColumnValueFilter(
        Bytes.toBytes(CF_DATA),
        Bytes.toBytes("age"),
        CompareOperator.GREATER,
        Bytes.toBytes("50")
      )
      
      // 条件2: score >= 80
      val filter2 = new SingleColumnValueFilter(
        Bytes.toBytes(CF_DATA),
        Bytes.toBytes("score"),
        CompareOperator.GREATER_OR_EQUAL,
        Bytes.toBytes("80")
      )
      
      filterList.addFilter(filter1)
      filterList.addFilter(filter2)
      
      // 添加随机行过滤器(取样)
      val samplingFilter = new RandomRowFilter(0.1f)  // 10%采样率
      
      val finalFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL)
      finalFilter.addFilter(filterList)
      finalFilter.addFilter(samplingFilter)
      
      val scan = new Scan()
      scan.setFilter(finalFilter)
      
      val scanner = table.getScanner(scan)
      var count = 0
      scanner.asScala.foreach { result =>
        count += 1
        if (count <= 5) {
          val rowKey = Bytes.toString(result.getRow)
          val age = Bytes.toString(result.getValue(Bytes.toBytes(CF_DATA), Bytes.toBytes("age")))
          val score = Bytes.toString(result.getValue(Bytes.toBytes(CF_DATA), Bytes.toBytes("score")))
          logger.info(s"RowKey: $rowKey, Age: $age, Score: $score")
        }
      }
      logger.info(s"Total filtered records: $count")
      scanner.close()
      
    } finally {
      table.close()
    }
  }
  
  /**
   * 5. 多版本数据查询
   */
  def multiVersionQuery(connection: Connection): Unit = {
    logger.info("=== Multi-Version Query ===")
    
    val table = connection.getTable(TableName.valueOf(TABLE_NAME))
    
    try {
      val rowKey = "version_test"
      
      // 插入多个版本
      for (i <- 1 to 5) {
        val put = new Put(Bytes.toBytes(rowKey))
        put.addColumn(Bytes.toBytes(CF_DATA), Bytes.toBytes("value"), 
          Bytes.toBytes(s"version_$i"))
        table.put(put)
        Thread.sleep(100)
      }
      
      // 获取所有版本
      val get = new Get(Bytes.toBytes(rowKey))
      get.readAllVersions()
      get.addColumn(Bytes.toBytes(CF_DATA), Bytes.toBytes("value"))
      
      val result = table.get(get)
      val cells = result.getColumnCells(Bytes.toBytes(CF_DATA), Bytes.toBytes("value"))
      
      logger.info("All versions:")
      cells.asScala.foreach { cell =>
        val value = Bytes.toString(CellUtil.cloneValue(cell))
        val timestamp = cell.getTimestamp
        logger.info(s"  Timestamp: $timestamp, Value: $value")
      }
      
      // 获取指定数量的版本
      val get2 = new Get(Bytes.toBytes(rowKey))
      get2.readVersions(3)  // 只获取最近3个版本
      
      val result2 = table.get(get2)
      logger.info(s"Latest 3 versions: ${result2.size()} cells")
      
      // 获取指定时间范围的版本
      val get3 = new Get(Bytes.toBytes(rowKey))
      val endTime = System.currentTimeMillis()
      val startTime = endTime - 300  // 最近300ms
      get3.setTimeRange(startTime, endTime)
      
      val result3 = table.get(get3)
      logger.info(s"Versions in time range: ${result3.size()} cells")
      
    } finally {
      table.close()
    }
  }
  
  /**
   * 6. 行锁和并发控制
   */
  def rowLockDemo(connection: Connection): Unit = {
    logger.info("=== Row Lock Demo ===")
    
    val table = connection.getTable(TableName.valueOf(TABLE_NAME))
    
    try {
      val rowKey = "lock_test"
      
      // 初始化数据
      val initPut = new Put(Bytes.toBytes(rowKey))
      initPut.addColumn(Bytes.toBytes(CF_DATA), Bytes.toBytes("balance"), 
        Bytes.toBytes("1000"))
      table.put(initPut)
      
      // 模拟并发操作: 使用 checkAndMutate 保证原子性
      logger.info("Simulating concurrent balance operations...")
      
      // 操作1: 减少100
      var success = false
      var retries = 0
      while (!success && retries < 3) {
        val get = new Get(Bytes.toBytes(rowKey))
        val result = table.get(get)
        val currentBalance = Bytes.toString(
          result.getValue(Bytes.toBytes(CF_DATA), Bytes.toBytes("balance"))).toInt
        
        if (currentBalance >= 100) {
          val newBalance = currentBalance - 100
          val put = new Put(Bytes.toBytes(rowKey))
          put.addColumn(Bytes.toBytes(CF_DATA), Bytes.toBytes("balance"), 
            Bytes.toBytes(newBalance.toString))
          
          success = table.checkAndMutate(Bytes.toBytes(rowKey), Bytes.toBytes(CF_DATA))
            .qualifier(Bytes.toBytes("balance"))
            .ifEquals(Bytes.toBytes(currentBalance.toString))
            .thenPut(put)
          
          if (success) {
            logger.info(s"Deducted 100: $currentBalance -> $newBalance")
          } else {
            logger.info("CAS failed, retrying...")
            retries += 1
          }
        } else {
          logger.warn("Insufficient balance")
          success = true
        }
      }
      
    } finally {
      table.close()
    }
  }
  
  /**
   * 7. 协处理器(Coprocessor)概念演示
   */
  def coprocessorDemo(): Unit = {
    logger.info("=== Coprocessor Demo ===")
    logger.info("""
      |HBase Coprocessors are like database stored procedures:
      |
      |1. Observer Coprocessors:
      |   - PreGet/PostGet: Hook into Get operations
      |   - PrePut/PostPut: Hook into Put operations
      |   - PreDelete/PostDelete: Hook into Delete operations
      |   - Use cases: Audit logging, access control, data validation
      |
      |2. Endpoint Coprocessors:
      |   - Custom RPC protocols
      |   - Server-side data aggregation
      |   - Reduce data transfer to client
      |   - Use cases: Server-side computation, custom aggregations
      |
      |Benefits:
      |   - Reduce network traffic
      |   - Improve performance
      |   - Centralized business logic
      |
      |Example use case: 
      |   Instead of scanning millions of rows to calculate sum,
      |   use Endpoint Coprocessor to calculate on RegionServer
      """.stripMargin)
  }
  
  /**
   * 8. 二级索引实现思路
   */
  def secondaryIndexDemo(connection: Connection): Unit = {
    logger.info("=== Secondary Index Demo ===")
    
    val table = connection.getTable(TableName.valueOf(TABLE_NAME))
    
    try {
      logger.info("""
        |HBase Secondary Index Implementation Strategies:
        |
        |1. Index Table Pattern:
        |   Main Table: userId -> userData
        |   Index Table: email -> userId
        |   Query by email: Index Table -> Get userId -> Main Table
        |
        |2. Duplicate Data Pattern:
        |   Store same data with different row keys
        |   Trade storage for query performance
        |
        |3. Coprocessor-based Index:
        |   Use Observer Coprocessor to auto-maintain index
        |   Atomic updates to both main and index tables
        |
        |4. Phoenix Secondary Index:
        |   Use Apache Phoenix (SQL layer on HBase)
        |   Automatic index management
        """.stripMargin)
      
      // 简单的二级索引示例
      logger.info("\nExample: Email -> UserId index")
      
      // 主表插入
      val userId = "user001"
      val email = "john@example.com"
      
      val mainPut = new Put(Bytes.toBytes(userId))
      mainPut.addColumn(Bytes.toBytes(CF_DATA), Bytes.toBytes("email"), 
        Bytes.toBytes(email))
      mainPut.addColumn(Bytes.toBytes(CF_DATA), Bytes.toBytes("name"), 
        Bytes.toBytes("John Doe"))
      table.put(mainPut)
      
      // 索引表插入 (实际应该是另一个表)
      val indexPut = new Put(Bytes.toBytes(email))
      indexPut.addColumn(Bytes.toBytes(CF_INDEX), Bytes.toBytes("userId"), 
        Bytes.toBytes(userId))
      table.put(indexPut)
      
      logger.info(s"Created index: $email -> $userId")
      
      // 通过索引查询
      val indexGet = new Get(Bytes.toBytes(email))
      val indexResult = table.get(indexGet)
      
      if (!indexResult.isEmpty) {
        val foundUserId = Bytes.toString(
          indexResult.getValue(Bytes.toBytes(CF_INDEX), Bytes.toBytes("userId")))
        logger.info(s"Found userId from index: $foundUserId")
        
        // 再通过 userId 查询主表
        val mainGet = new Get(Bytes.toBytes(foundUserId))
        val mainResult = table.get(mainGet)
        val name = Bytes.toString(
          mainResult.getValue(Bytes.toBytes(CF_DATA), Bytes.toBytes("name")))
        logger.info(s"Found user name: $name")
      }
      
    } finally {
      table.close()
    }
  }
  
  /**
   * 9. 热点问题优化
   */
  def hotspotOptimization(): Unit = {
    logger.info("=== Hotspot Optimization ===")
    logger.info("""
      |HBase Hotspot (Region Hotspot) Solutions:
      |
      |1. Salting (加盐):
      |   Original RowKey: userId_timestamp
      |   Salted RowKey: hash(userId)%10 + userId_timestamp
      |   Distributes writes across multiple regions
      |
      |2. Reversing (反转):
      |   Original: timestamp_userId (sequential writes)
      |   Reversed: reverse(timestamp)_userId
      |   Avoids hotspot on latest timestamp region
      |
      |3. Hashing:
      |   Use hash of RowKey as prefix
      |   Example: md5(key).substring(0,4) + key
      |
      |4. Pre-splitting:
      |   Create table with predefined regions
      |   Distribute data from the start
      |
      |5. Multiple Column Families:
      |   Separate hot and cold data
      |   Different access patterns in different families
      |
      |Example: Salting Implementation
      """.stripMargin)
    
    // 加盐示例
    def saltedRowKey(userId: String, timestamp: Long): String = {
      val salt = Math.abs(userId.hashCode % 10)
      f"$salt%02d_${userId}_$timestamp"
    }
    
    val userId = "user123"
    val timestamp = System.currentTimeMillis()
    val salted = saltedRowKey(userId, timestamp)
    
    logger.info(s"Original: ${userId}_$timestamp")
    logger.info(s"Salted: $salted")
  }
  
  /**
   * 10. 性能监控和调优
   */
  def performanceMonitoring(connection: Connection): Unit = {
    logger.info("=== Performance Monitoring ===")
    
    val table = connection.getTable(TableName.valueOf(TABLE_NAME))
    
    try {
      // 获取表的统计信息
      val admin = connection.getAdmin
      val descriptor = admin.getDescriptor(TableName.valueOf(TABLE_NAME))
      
      logger.info(s"Table: ${descriptor.getTableName}")
      logger.info(s"Column Families: ${descriptor.getColumnFamilyCount}")
      
      descriptor.getColumnFamilies.foreach { cf =>
        logger.info(s"\nColumn Family: ${cf.getNameAsString}")
        logger.info(s"  Max Versions: ${cf.getMaxVersions}")
        logger.info(s"  TTL: ${cf.getTimeToLive} seconds")
        logger.info(s"  Compression: ${cf.getCompressionType}")
        logger.info(s"  Block Size: ${cf.getBlocksize} bytes")
        logger.info(s"  Bloom Filter: ${cf.getBloomFilterType}")
        logger.info(s"  In Memory: ${cf.isInMemory}")
        logger.info(s"  Block Cache: ${cf.isBlockCacheEnabled}")
      }
      
      logger.info("""
        |
        |Performance Tuning Tips:
        |
        |1. Client-side:
        |   - Use BufferedMutator for batch writes
        |   - Set appropriate cache/batch size for scans
        |   - Use column-specific Get/Scan
        |   - Enable client-side caching
        |
        |2. Table Design:
        |   - Design RowKey for even distribution
        |   - Use appropriate column families (usually 1-3)
        |   - Set reasonable TTL for time-series data
        |   - Enable compression (SNAPPY recommended)
        |
        |3. Region Management:
        |   - Pre-split tables for known data distribution
        |   - Monitor region sizes (aim for 10-50GB)
        |   - Balance region distribution
        |
        |4. Memory:
        |   - Configure MemStore size appropriately
        |   - Tune BlockCache size (default 40% heap)
        |   - Use off-heap BlockCache for large datasets
        |
        |5. Compaction:
        |   - Schedule major compactions during off-peak
        |   - Tune compaction thresholds
        |   - Monitor compaction queue
        |
        |6. Monitoring Metrics:
        |   - Request latency (Get/Put/Scan)
        |   - Region count and distribution
        |   - Compaction queue length
        |   - BlockCache hit ratio
        |   - MemStore size
        """.stripMargin)
      
      admin.close()
      
    } finally {
      table.close()
    }
  }
}
