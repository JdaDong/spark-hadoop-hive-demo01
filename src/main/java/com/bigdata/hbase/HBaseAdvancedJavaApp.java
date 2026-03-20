package com.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * HBase 高级特性 - Java 实现
 * 演示 HBase 的高级功能和优化技巧
 */
public class HBaseAdvancedJavaApp {
    
    private static final Logger logger = LoggerFactory.getLogger(HBaseAdvancedJavaApp.class);
    private static final String TABLE_NAME = "advanced_features";
    private static final String CF_DATA = "data";
    private static final String CF_META = "meta";
    
    private Configuration conf;
    private Connection connection;
    
    public HBaseAdvancedJavaApp() throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        connection = ConnectionFactory.createConnection(conf);
    }
    
    public static void main(String[] args) {
        try {
            HBaseAdvancedJavaApp app = new HBaseAdvancedJavaApp();
            
            // 1. 创建表(带高级配置)
            app.createAdvancedTable();
            
            // 2. 复杂过滤器组合
            app.complexFilterDemo();
            
            // 3. 计数器(原子增减)
            app.counterDemo();
            
            // 4. CAS 操作(Compare And Set)
            app.casOperationDemo();
            
            // 5. 批量操作优化
            app.batchOperationOptimization();
            
            // 6. 前缀过滤和模糊查询
            app.prefixAndFuzzyRowFilter();
            
            // 7. 列分页查询
            app.columnPaginationDemo();
            
            // 8. Bloom Filter 演示
            app.bloomFilterDemo();
            
            // 9. 压缩算法配置
            app.compressionDemo();
            
            // 10. TTL(Time To Live) 设置
            app.ttlDemo();
            
            app.close();
            
        } catch (Exception e) {
            logger.error("Error in HBase Advanced application: ", e);
        }
    }
    
    /**
     * 1. 创建带高级配置的表
     */
    public void createAdvancedTable() throws IOException {
        Admin admin = connection.getAdmin();
        
        try {
            TableName tableName = TableName.valueOf(TABLE_NAME);
            
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
            
            TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(tableName);
            
            // 配置列族1: 数据列族
            ColumnFamilyDescriptor dataFamily = ColumnFamilyDescriptorBuilder
                    .newBuilder(Bytes.toBytes(CF_DATA))
                    .setMaxVersions(5)                          // 保留5个版本
                    .setMinVersions(1)                          // 最少保留1个版本
                    .setTimeToLive(86400 * 7)                   // TTL: 7天
                    .setCompressionType(Compression.Algorithm.SNAPPY)  // 压缩算法
                    .setBloomFilterType(BloomType.ROW)          // Bloom Filter
                    .setBlocksize(65536)                        // Block大小: 64KB
                    .setInMemory(false)                         // 不强制内存驻留
                    .setBlockCacheEnabled(true)                 // 启用Block Cache
                    .build();
            
            // 配置列族2: 元数据列族
            ColumnFamilyDescriptor metaFamily = ColumnFamilyDescriptorBuilder
                    .newBuilder(Bytes.toBytes(CF_META))
                    .setMaxVersions(10)
                    .setCompressionType(Compression.Algorithm.GZ)
                    .setBloomFilterType(BloomType.ROWCOL)
                    .build();
            
            tableBuilder.setColumnFamily(dataFamily);
            tableBuilder.setColumnFamily(metaFamily);
            
            // 创建表
            admin.createTable(tableBuilder.build());
            logger.info("Advanced table created with optimized configuration");
            
            // 插入测试数据
            insertTestData();
            
        } finally {
            admin.close();
        }
    }
    
    /**
     * 插入测试数据
     */
    private void insertTestData() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        
        try {
            List<Put> puts = new ArrayList<>();
            
            for (int i = 1; i <= 20; i++) {
                String rowKey = String.format("row%03d", i);
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(CF_DATA), Bytes.toBytes("value"), 
                        Bytes.toBytes("value_" + i));
                put.addColumn(Bytes.toBytes(CF_DATA), Bytes.toBytes("score"), 
                        Bytes.toBytes(String.valueOf(i * 10)));
                put.addColumn(Bytes.toBytes(CF_META), Bytes.toBytes("type"), 
                        Bytes.toBytes(i % 2 == 0 ? "even" : "odd"));
                puts.add(put);
            }
            
            table.put(puts);
            logger.info("Inserted {} test records", puts.size());
            
        } finally {
            table.close();
        }
    }
    
    /**
     * 2. 复杂过滤器组合
     */
    public void complexFilterDemo() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        
        try {
            logger.info("=== Complex Filter Demo ===");
            
            // 创建过滤器列表
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            
            // 过滤器1: RowKey 前缀过滤
            Filter prefixFilter = new PrefixFilter(Bytes.toBytes("row01"));
            
            // 过滤器2: 单列值过滤 (score >= 100)
            Filter valueFilter = new SingleColumnValueFilter(
                    Bytes.toBytes(CF_DATA),
                    Bytes.toBytes("score"),
                    CompareOperator.GREATER_OR_EQUAL,
                    Bytes.toBytes("100")
            );
            
            filterList.addFilter(prefixFilter);
            filterList.addFilter(valueFilter);
            
            Scan scan = new Scan();
            scan.setFilter(filterList);
            
            ResultScanner scanner = table.getScanner(scan);
            
            logger.info("Results with complex filters:");
            for (Result result : scanner) {
                String rowKey = Bytes.toString(result.getRow());
                String value = Bytes.toString(result.getValue(
                        Bytes.toBytes(CF_DATA), Bytes.toBytes("value")));
                String score = Bytes.toString(result.getValue(
                        Bytes.toBytes(CF_DATA), Bytes.toBytes("score")));
                logger.info("RowKey: {}, Value: {}, Score: {}", rowKey, value, score);
            }
            
            scanner.close();
            
        } finally {
            table.close();
        }
    }
    
    /**
     * 3. 计数器(原子增减)
     */
    public void counterDemo() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        
        try {
            logger.info("=== Counter Demo ===");
            
            String rowKey = "counter_row";
            byte[] family = Bytes.toBytes(CF_DATA);
            byte[] qualifier = Bytes.toBytes("counter");
            
            // 初始化计数器
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(family, qualifier, Bytes.toBytes(0L));
            table.put(put);
            
            // 原子增加
            long value1 = table.incrementColumnValue(
                    Bytes.toBytes(rowKey), family, qualifier, 10L);
            logger.info("After increment by 10: {}", value1);
            
            long value2 = table.incrementColumnValue(
                    Bytes.toBytes(rowKey), family, qualifier, 5L);
            logger.info("After increment by 5: {}", value2);
            
            // 原子减少
            long value3 = table.incrementColumnValue(
                    Bytes.toBytes(rowKey), family, qualifier, -3L);
            logger.info("After decrement by 3: {}", value3);
            
            // 使用 Increment API
            Increment increment = new Increment(Bytes.toBytes(rowKey));
            increment.addColumn(family, qualifier, 20L);
            Result result = table.increment(increment);
            
            long finalValue = Bytes.toLong(result.getValue(family, qualifier));
            logger.info("Final counter value: {}", finalValue);
            
        } finally {
            table.close();
        }
    }
    
    /**
     * 4. CAS 操作(Compare And Set)
     */
    public void casOperationDemo() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        
        try {
            logger.info("=== CAS Operation Demo ===");
            
            String rowKey = "cas_row";
            byte[] family = Bytes.toBytes(CF_DATA);
            byte[] qualifier = Bytes.toBytes("status");
            
            // 初始化数据
            Put initPut = new Put(Bytes.toBytes(rowKey));
            initPut.addColumn(family, qualifier, Bytes.toBytes("pending"));
            table.put(initPut);
            logger.info("Initial status: pending");
            
            // CAS 操作: 只有当 status=pending 时才更新为 processing
            Put updatePut = new Put(Bytes.toBytes(rowKey));
            updatePut.addColumn(family, qualifier, Bytes.toBytes("processing"));
            
            boolean success1 = table.checkAndMutate(Bytes.toBytes(rowKey), family)
                    .qualifier(qualifier)
                    .ifEquals(Bytes.toBytes("pending"))
                    .thenPut(updatePut);
            
            logger.info("CAS update to 'processing': {}", success1);
            
            // 再次尝试相同的 CAS 操作(应该失败)
            Put updatePut2 = new Put(Bytes.toBytes(rowKey));
            updatePut2.addColumn(family, qualifier, Bytes.toBytes("processing"));
            
            boolean success2 = table.checkAndMutate(Bytes.toBytes(rowKey), family)
                    .qualifier(qualifier)
                    .ifEquals(Bytes.toBytes("pending"))
                    .thenPut(updatePut2);
            
            logger.info("Second CAS update to 'processing': {} (should be false)", success2);
            
            // 正确的 CAS: processing -> completed
            Put completePut = new Put(Bytes.toBytes(rowKey));
            completePut.addColumn(family, qualifier, Bytes.toBytes("completed"));
            
            boolean success3 = table.checkAndMutate(Bytes.toBytes(rowKey), family)
                    .qualifier(qualifier)
                    .ifEquals(Bytes.toBytes("processing"))
                    .thenPut(completePut);
            
            logger.info("CAS update to 'completed': {}", success3);
            
        } finally {
            table.close();
        }
    }
    
    /**
     * 5. 批量操作优化
     */
    public void batchOperationOptimization() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        
        try {
            logger.info("=== Batch Operation Optimization ===");
            
            // 使用 BufferedMutator 进行高效写入
            BufferedMutatorParams params = new BufferedMutatorParams(
                    TableName.valueOf(TABLE_NAME))
                    .writeBufferSize(5 * 1024 * 1024);  // 5MB buffer
            
            BufferedMutator mutator = connection.getBufferedMutator(params);
            
            long startTime = System.currentTimeMillis();
            
            // 批量插入10000条数据
            for (int i = 0; i < 10000; i++) {
                Put put = new Put(Bytes.toBytes("batch_" + i));
                put.addColumn(Bytes.toBytes(CF_DATA), Bytes.toBytes("data"), 
                        Bytes.toBytes("value_" + i));
                mutator.mutate(put);
            }
            
            mutator.flush();
            mutator.close();
            
            long endTime = System.currentTimeMillis();
            logger.info("Batch inserted 10000 records in {} ms", (endTime - startTime));
            
        } finally {
            table.close();
        }
    }
    
    /**
     * 6. 前缀过滤和模糊查询
     */
    public void prefixAndFuzzyRowFilter() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        
        try {
            logger.info("=== Prefix and Fuzzy Row Filter ===");
            
            // 前缀过滤
            logger.info("Prefix Filter (row01*):");
            Scan prefixScan = new Scan();
            prefixScan.setFilter(new PrefixFilter(Bytes.toBytes("row01")));
            
            ResultScanner prefixScanner = table.getScanner(prefixScan);
            int prefixCount = 0;
            for (Result result : prefixScanner) {
                prefixCount++;
            }
            logger.info("Found {} records with prefix 'row01'", prefixCount);
            prefixScanner.close();
            
            // 模糊行过滤 (FuzzyRowFilter)
            logger.info("\nFuzzy Row Filter (row0?5):");
            
            // 模式: row0X5, X可以是任意字符
            byte[] fuzzyKey = Bytes.toBytes("row0?5");
            byte[] fuzzyMask = new byte[]{0, 0, 0, 0, 0, 1, 0};  // 1表示该位可变
            
            List<Pair<byte[], byte[]>> fuzzyKeys = new ArrayList<>();
            fuzzyKeys.add(new Pair<>(fuzzyKey, fuzzyMask));
            
            Scan fuzzyScan = new Scan();
            fuzzyScan.setFilter(new FuzzyRowFilter(fuzzyKeys));
            
            ResultScanner fuzzyScanner = table.getScanner(fuzzyScan);
            for (Result result : fuzzyScanner) {
                String rowKey = Bytes.toString(result.getRow());
                logger.info("Matched: {}", rowKey);
            }
            fuzzyScanner.close();
            
        } finally {
            table.close();
        }
    }
    
    /**
     * 7. 列分页查询
     */
    public void columnPaginationDemo() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        
        try {
            logger.info("=== Column Pagination Demo ===");
            
            // 插入多列数据
            String rowKey = "multi_column_row";
            Put put = new Put(Bytes.toBytes(rowKey));
            for (int i = 0; i < 20; i++) {
                put.addColumn(Bytes.toBytes(CF_DATA), 
                        Bytes.toBytes("col_" + i), 
                        Bytes.toBytes("value_" + i));
            }
            table.put(put);
            
            // 列分页过滤器: 每次获取5列
            int pageSize = 5;
            int offset = 0;
            
            for (int page = 0; page < 4; page++) {
                logger.info("\n--- Page {} ---", page + 1);
                
                Get get = new Get(Bytes.toBytes(rowKey));
                get.setFilter(new ColumnPaginationFilter(pageSize, offset));
                
                Result result = table.get(get);
                
                for (Cell cell : result.rawCells()) {
                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    logger.info("{} = {}", qualifier, value);
                }
                
                offset += pageSize;
            }
            
        } finally {
            table.close();
        }
    }
    
    /**
     * 8. Bloom Filter 演示
     */
    public void bloomFilterDemo() throws IOException {
        logger.info("=== Bloom Filter Demo ===");
        logger.info("Bloom Filter types:");
        logger.info("- NONE: No Bloom Filter");
        logger.info("- ROW: Row-level Bloom Filter (default)");
        logger.info("- ROWCOL: Row+Column level Bloom Filter");
        logger.info("\nBloom Filter reduces disk I/O for Get operations");
        logger.info("ROW: Efficient for row-based queries");
        logger.info("ROWCOL: Efficient when querying specific columns");
    }
    
    /**
     * 9. 压缩算法演示
     */
    public void compressionDemo() throws IOException {
        logger.info("=== Compression Demo ===");
        logger.info("Available compression algorithms:");
        logger.info("- NONE: No compression");
        logger.info("- SNAPPY: Fast compression, moderate ratio (recommended)");
        logger.info("- GZ (GZIP): Slower, better compression ratio");
        logger.info("- LZO: Fast, good for text data");
        logger.info("- LZ4: Very fast, moderate ratio");
        logger.info("- ZSTD: Best compression ratio, moderate speed");
        logger.info("\nCurrent table uses SNAPPY for data and GZ for metadata");
    }
    
    /**
     * 10. TTL 演示
     */
    public void ttlDemo() throws IOException {
        logger.info("=== TTL (Time To Live) Demo ===");
        logger.info("Current table TTL configuration:");
        logger.info("- data family: 7 days (604800 seconds)");
        logger.info("- meta family: Default (no TTL)");
        logger.info("\nData older than TTL will be automatically deleted during compaction");
        logger.info("Use case: Log data, temporary cache, session data");
    }
    
    /**
     * 关闭连接
     */
    public void close() throws IOException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
            logger.info("HBase connection closed");
        }
    }
}
