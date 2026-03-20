package com.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * HBase 数据操作 - Java 实现
 * 演示 HBase 的 CRUD 操作
 */
public class HBaseJavaApp {
    
    private static final Logger logger = LoggerFactory.getLogger(HBaseJavaApp.class);
    private static final String TABLE_NAME = "user_info";
    private static final String COLUMN_FAMILY_BASIC = "basic";
    private static final String COLUMN_FAMILY_EXTRA = "extra";
    
    private Configuration conf;
    private Connection connection;
    
    public HBaseJavaApp() throws IOException {
        // 创建 HBase 配置
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        
        // 创建连接
        connection = ConnectionFactory.createConnection(conf);
    }
    
    public static void main(String[] args) {
        try {
            HBaseJavaApp app = new HBaseJavaApp();
            
            // 1. 创建表
            app.createTable();
            
            // 2. 插入数据
            app.insertData();
            
            // 3. 查询单条数据
            app.getData("user001");
            
            // 4. 扫描数据
            app.scanData();
            
            // 5. 更新数据
            app.updateData("user001");
            
            // 6. 删除数据
            app.deleteData("user003");
            
            // 7. 批量操作
            app.batchOperations();
            
            // 关闭连接
            app.close();
            
        } catch (Exception e) {
            logger.error("Error in HBase application: ", e);
        }
    }
    
    /**
     * 创建表
     */
    public void createTable() throws IOException {
        Admin admin = connection.getAdmin();
        
        try {
            TableName tableName = TableName.valueOf(TABLE_NAME);
            
            // 如果表已存在,先删除
            if (admin.tableExists(tableName)) {
                logger.info("Table {} already exists, deleting it...", TABLE_NAME);
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
            
            // 创建表描述
            TableDescriptorBuilder tableDescBuilder = TableDescriptorBuilder.newBuilder(tableName);
            
            // 添加列族
            ColumnFamilyDescriptor basicFamily = ColumnFamilyDescriptorBuilder
                    .newBuilder(Bytes.toBytes(COLUMN_FAMILY_BASIC))
                    .setMaxVersions(3)
                    .build();
            
            ColumnFamilyDescriptor extraFamily = ColumnFamilyDescriptorBuilder
                    .newBuilder(Bytes.toBytes(COLUMN_FAMILY_EXTRA))
                    .setMaxVersions(3)
                    .build();
            
            tableDescBuilder.setColumnFamily(basicFamily);
            tableDescBuilder.setColumnFamily(extraFamily);
            
            // 创建表
            admin.createTable(tableDescBuilder.build());
            logger.info("Table {} created successfully", TABLE_NAME);
            
        } finally {
            admin.close();
        }
    }
    
    /**
     * 插入数据
     */
    public void insertData() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        
        try {
            // 插入第一条数据
            Put put1 = new Put(Bytes.toBytes("user001"));
            put1.addColumn(Bytes.toBytes(COLUMN_FAMILY_BASIC), Bytes.toBytes("name"), Bytes.toBytes("Alice"));
            put1.addColumn(Bytes.toBytes(COLUMN_FAMILY_BASIC), Bytes.toBytes("age"), Bytes.toBytes("28"));
            put1.addColumn(Bytes.toBytes(COLUMN_FAMILY_EXTRA), Bytes.toBytes("email"), Bytes.toBytes("alice@example.com"));
            put1.addColumn(Bytes.toBytes(COLUMN_FAMILY_EXTRA), Bytes.toBytes("city"), Bytes.toBytes("Beijing"));
            table.put(put1);
            
            // 插入第二条数据
            Put put2 = new Put(Bytes.toBytes("user002"));
            put2.addColumn(Bytes.toBytes(COLUMN_FAMILY_BASIC), Bytes.toBytes("name"), Bytes.toBytes("Bob"));
            put2.addColumn(Bytes.toBytes(COLUMN_FAMILY_BASIC), Bytes.toBytes("age"), Bytes.toBytes("35"));
            put2.addColumn(Bytes.toBytes(COLUMN_FAMILY_EXTRA), Bytes.toBytes("email"), Bytes.toBytes("bob@example.com"));
            put2.addColumn(Bytes.toBytes(COLUMN_FAMILY_EXTRA), Bytes.toBytes("city"), Bytes.toBytes("Shanghai"));
            table.put(put2);
            
            // 插入第三条数据
            Put put3 = new Put(Bytes.toBytes("user003"));
            put3.addColumn(Bytes.toBytes(COLUMN_FAMILY_BASIC), Bytes.toBytes("name"), Bytes.toBytes("Charlie"));
            put3.addColumn(Bytes.toBytes(COLUMN_FAMILY_BASIC), Bytes.toBytes("age"), Bytes.toBytes("42"));
            put3.addColumn(Bytes.toBytes(COLUMN_FAMILY_EXTRA), Bytes.toBytes("email"), Bytes.toBytes("charlie@example.com"));
            put3.addColumn(Bytes.toBytes(COLUMN_FAMILY_EXTRA), Bytes.toBytes("city"), Bytes.toBytes("Shenzhen"));
            table.put(put3);
            
            logger.info("Data inserted successfully");
            
        } finally {
            table.close();
        }
    }
    
    /**
     * 查询单条数据
     */
    public void getData(String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        
        try {
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            
            if (!result.isEmpty()) {
                logger.info("=== Data for {} ===", rowKey);
                String name = Bytes.toString(result.getValue(Bytes.toBytes(COLUMN_FAMILY_BASIC), Bytes.toBytes("name")));
                String age = Bytes.toString(result.getValue(Bytes.toBytes(COLUMN_FAMILY_BASIC), Bytes.toBytes("age")));
                String email = Bytes.toString(result.getValue(Bytes.toBytes(COLUMN_FAMILY_EXTRA), Bytes.toBytes("email")));
                String city = Bytes.toString(result.getValue(Bytes.toBytes(COLUMN_FAMILY_EXTRA), Bytes.toBytes("city")));
                
                logger.info("Name: {}, Age: {}, Email: {}, City: {}", name, age, email, city);
            } else {
                logger.info("No data found for rowKey: {}", rowKey);
            }
            
        } finally {
            table.close();
        }
    }
    
    /**
     * 扫描数据
     */
    public void scanData() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        
        try {
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            
            logger.info("=== Scanning all data ===");
            for (Result result : scanner) {
                String rowKey = Bytes.toString(result.getRow());
                String name = Bytes.toString(result.getValue(Bytes.toBytes(COLUMN_FAMILY_BASIC), Bytes.toBytes("name")));
                String age = Bytes.toString(result.getValue(Bytes.toBytes(COLUMN_FAMILY_BASIC), Bytes.toBytes("age")));
                String email = Bytes.toString(result.getValue(Bytes.toBytes(COLUMN_FAMILY_EXTRA), Bytes.toBytes("email")));
                String city = Bytes.toString(result.getValue(Bytes.toBytes(COLUMN_FAMILY_EXTRA), Bytes.toBytes("city")));
                
                logger.info("RowKey: {}, Name: {}, Age: {}, Email: {}, City: {}", 
                        rowKey, name, age, email, city);
            }
            scanner.close();
            
        } finally {
            table.close();
        }
    }
    
    /**
     * 更新数据
     */
    public void updateData(String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        
        try {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY_BASIC), Bytes.toBytes("age"), Bytes.toBytes("29"));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY_EXTRA), Bytes.toBytes("city"), Bytes.toBytes("Guangzhou"));
            table.put(put);
            
            logger.info("Data updated for rowKey: {}", rowKey);
            
        } finally {
            table.close();
        }
    }
    
    /**
     * 删除数据
     */
    public void deleteData(String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        
        try {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            
            logger.info("Data deleted for rowKey: {}", rowKey);
            
        } finally {
            table.close();
        }
    }
    
    /**
     * 批量操作
     */
    public void batchOperations() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        
        try {
            List<Put> puts = new ArrayList<>();
            
            // 批量插入
            for (int i = 4; i <= 6; i++) {
                String rowKey = "user00" + i;
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY_BASIC), Bytes.toBytes("name"), 
                        Bytes.toBytes("User" + i));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY_BASIC), Bytes.toBytes("age"), 
                        Bytes.toBytes(String.valueOf(20 + i)));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY_EXTRA), Bytes.toBytes("email"), 
                        Bytes.toBytes("user" + i + "@example.com"));
                puts.add(put);
            }
            
            table.put(puts);
            logger.info("Batch insert completed, {} records inserted", puts.size());
            
        } finally {
            table.close();
        }
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
