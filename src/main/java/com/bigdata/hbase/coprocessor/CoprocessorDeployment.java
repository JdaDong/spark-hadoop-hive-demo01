package com.bigdata.hbase.coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * HBase Coprocessor 部署工具类
 * 
 * 协处理器部署方式：
 * 1. 静态部署：在 hbase-site.xml 中配置，对所有表生效
 * 2. 动态部署：通过表属性配置，只对指定表生效
 * 3. Shell 部署：使用 HBase Shell 命令
 */
public class CoprocessorDeployment {
    
    private static final Logger LOG = LoggerFactory.getLogger(CoprocessorDeployment.class);
    
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Admin admin = connection.getAdmin()) {
            
            // 示例1: 部署 Observer Coprocessor
            deployObserverCoprocessor(admin);
            
            // 示例2: 部署 Endpoint Coprocessor
            deployEndpointCoprocessor(admin);
            
            // 示例3: 从 HDFS 加载 Coprocessor
            deployFromHDFS(admin, conf);
            
            LOG.info("Coprocessor deployment examples completed");
        }
    }
    
    /**
     * 部署 Observer Coprocessor（动态部署）
     */
    public static void deployObserverCoprocessor(Admin admin) throws IOException {
        TableName tableName = TableName.valueOf("user_table");
        
        // 检查表是否存在
        if (!admin.tableExists(tableName)) {
            LOG.warn("Table {} does not exist", tableName);
            return;
        }
        
        // 禁用表
        if (!admin.isTableDisabled(tableName)) {
            admin.disableTable(tableName);
            LOG.info("Table {} disabled", tableName);
        }
        
        try {
            // 获取表描述符
            TableDescriptor oldDescriptor = admin.getDescriptor(tableName);
            
            // 创建新的表描述符，添加协处理器
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(oldDescriptor);
            
            // 添加 Observer Coprocessor
            // 格式: className|priority|key1=value1,key2=value2
            String coprocessorClassName = "com.bigdata.hbase.coprocessor.AuditLogObserver";
            builder.setCoprocessor(coprocessorClassName);
            
            // 更新表描述符
            admin.modifyTable(builder.build());
            LOG.info("Observer Coprocessor added to table {}", tableName);
            
        } finally {
            // 启用表
            admin.enableTable(tableName);
            LOG.info("Table {} enabled", tableName);
        }
    }
    
    /**
     * 部署 Endpoint Coprocessor
     */
    public static void deployEndpointCoprocessor(Admin admin) throws IOException {
        TableName tableName = TableName.valueOf("analytics_table");
        
        if (!admin.tableExists(tableName)) {
            LOG.warn("Table {} does not exist", tableName);
            return;
        }
        
        if (!admin.isTableDisabled(tableName)) {
            admin.disableTable(tableName);
        }
        
        try {
            TableDescriptor oldDescriptor = admin.getDescriptor(tableName);
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(oldDescriptor);
            
            // 添加 Endpoint Coprocessor
            String coprocessorClassName = "com.bigdata.hbase.coprocessor.RowCountEndpoint";
            builder.setCoprocessor(coprocessorClassName);
            
            admin.modifyTable(builder.build());
            LOG.info("Endpoint Coprocessor added to table {}", tableName);
            
        } finally {
            admin.enableTable(tableName);
        }
    }
    
    /**
     * 从 HDFS 加载 Coprocessor JAR
     * 适用于生产环境
     */
    public static void deployFromHDFS(Admin admin, Configuration conf) throws IOException {
        TableName tableName = TableName.valueOf("production_table");
        
        if (!admin.tableExists(tableName)) {
            LOG.warn("Table {} does not exist", tableName);
            return;
        }
        
        // 1. 首先将 JAR 上传到 HDFS
        String localJarPath = "target/hbase-coprocessors.jar";
        String hdfsJarPath = "hdfs://localhost:9000/hbase/coprocessors/hbase-coprocessors.jar";
        
        // 上传 JAR 到 HDFS
        // uploadJarToHDFS(conf, localJarPath, hdfsJarPath);
        
        // 2. 配置协处理器从 HDFS 加载
        if (!admin.isTableDisabled(tableName)) {
            admin.disableTable(tableName);
        }
        
        try {
            TableDescriptor oldDescriptor = admin.getDescriptor(tableName);
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(oldDescriptor);
            
            // 指定 HDFS 路径和类名
            String coprocessorSpec = hdfsJarPath + "|" + 
                                    "com.bigdata.hbase.coprocessor.AuditLogObserver|" +
                                    "1001";  // priority
            
            builder.setCoprocessor(coprocessorSpec);
            admin.modifyTable(builder.build());
            
            LOG.info("Coprocessor loaded from HDFS: {}", hdfsJarPath);
            
        } finally {
            admin.enableTable(tableName);
        }
    }
    
    /**
     * 移除 Coprocessor
     */
    public static void removeCoprocessor(Admin admin, TableName tableName, String className) 
            throws IOException {
        
        if (!admin.tableExists(tableName)) {
            LOG.warn("Table {} does not exist", tableName);
            return;
        }
        
        if (!admin.isTableDisabled(tableName)) {
            admin.disableTable(tableName);
        }
        
        try {
            TableDescriptor oldDescriptor = admin.getDescriptor(tableName);
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(oldDescriptor);
            
            // 移除协处理器
            builder.removeCoprocessor(className);
            admin.modifyTable(builder.build());
            
            LOG.info("Coprocessor {} removed from table {}", className, tableName);
            
        } finally {
            admin.enableTable(tableName);
        }
    }
    
    /**
     * 上传 JAR 到 HDFS
     */
    private static void uploadJarToHDFS(Configuration conf, String localPath, String hdfsPath) 
            throws IOException {
        
        FileSystem fs = FileSystem.get(conf);
        
        Path localJar = new Path(localPath);
        Path hdfsJar = new Path(hdfsPath);
        
        // 如果已存在则删除
        if (fs.exists(hdfsJar)) {
            fs.delete(hdfsJar, false);
        }
        
        // 复制到 HDFS
        fs.copyFromLocalFile(localJar, hdfsJar);
        LOG.info("JAR uploaded to HDFS: {}", hdfsPath);
        
        fs.close();
    }
    
    /**
     * Shell 命令示例（供参考）
     */
    public static void printShellCommands() {
        System.out.println("\n=== HBase Shell 部署命令示例 ===\n");
        
        System.out.println("# 1. 禁用表");
        System.out.println("disable 'user_table'");
        
        System.out.println("\n# 2. 添加协处理器");
        System.out.println("alter 'user_table', METHOD => 'table_att', " +
                          "'coprocessor' => 'hdfs://localhost:9000/hbase/coprocessors/hbase-coprocessors.jar|" +
                          "com.bigdata.hbase.coprocessor.AuditLogObserver|1001'");
        
        System.out.println("\n# 3. 启用表");
        System.out.println("enable 'user_table'");
        
        System.out.println("\n# 4. 查看协处理器");
        System.out.println("describe 'user_table'");
        
        System.out.println("\n# 5. 移除协处理器");
        System.out.println("alter 'user_table', METHOD => 'table_att_unset', NAME => 'coprocessor$1'");
    }
}
