package com.bigdata.hbase.coprocessor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * HBase Observer Coprocessor 示例：审计日志
 * 
 * 功能：
 * 1. 拦截所有的 Put/Delete 操作
 * 2. 记录操作日志（时间、用户、操作类型）
 * 3. 可以在操作前后执行自定义逻辑
 * 4. 实现数据验证、权限控制等
 * 
 * 部署方式：
 * 1. 编译成 JAR 放到 HBase lib 目录
 * 2. 在 hbase-site.xml 配置
 * 3. 或通过表配置动态加载
 */
public class AuditLogObserver implements RegionCoprocessor, RegionObserver {
    
    private static final Logger LOG = LoggerFactory.getLogger(AuditLogObserver.class);
    
    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }
    
    /**
     * Put 操作前的钩子
     */
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> c,
                       Put put,
                       WALEdit edit,
                       Durability durability) throws IOException {
        
        String tableName = c.getEnvironment().getRegionInfo().getTable().getNameAsString();
        String rowKey = Bytes.toString(put.getRow());
        
        LOG.info("=== Audit Log: Pre-Put Operation ===");
        LOG.info("Table: {}", tableName);
        LOG.info("RowKey: {}", rowKey);
        LOG.info("Timestamp: {}", System.currentTimeMillis());
        
        // 数据验证示例
        for (List<Cell> cells : put.getFamilyCellMap().values()) {
            for (Cell cell : cells) {
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                
                LOG.info("Column: {}:{}, Value: {}", family, qualifier, value);
                
                // 示例：验证年龄必须大于0
                if ("info".equals(family) && "age".equals(qualifier)) {
                    try {
                        int age = Integer.parseInt(value);
                        if (age <= 0 || age > 150) {
                            throw new IOException("Invalid age: " + age);
                        }
                    } catch (NumberFormatException e) {
                        throw new IOException("Age must be a number: " + value);
                    }
                }
            }
        }
    }
    
    /**
     * Put 操作后的钩子
     */
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> c,
                        Put put,
                        WALEdit edit,
                        Durability durability) throws IOException {
        
        String rowKey = Bytes.toString(put.getRow());
        LOG.info("=== Audit Log: Post-Put Operation ===");
        LOG.info("RowKey: {} successfully inserted", rowKey);
        
        // 可以在这里写入审计日志表
        // writeAuditLog(c, "PUT", rowKey);
    }
    
    /**
     * Delete 操作前的钩子
     */
    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> c,
                          Delete delete,
                          WALEdit edit,
                          Durability durability) throws IOException {
        
        String tableName = c.getEnvironment().getRegionInfo().getTable().getNameAsString();
        String rowKey = Bytes.toString(delete.getRow());
        
        LOG.info("=== Audit Log: Pre-Delete Operation ===");
        LOG.info("Table: {}", tableName);
        LOG.info("RowKey: {}", rowKey);
        LOG.info("Timestamp: {}", System.currentTimeMillis());
        
        // 示例：防止删除重要数据
        if (rowKey.startsWith("protected_")) {
            throw new IOException("Cannot delete protected row: " + rowKey);
        }
    }
    
    /**
     * Delete 操作后的钩子
     */
    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> c,
                           Delete delete,
                           WALEdit edit,
                           Durability durability) throws IOException {
        
        String rowKey = Bytes.toString(delete.getRow());
        LOG.info("=== Audit Log: Post-Delete Operation ===");
        LOG.info("RowKey: {} successfully deleted", rowKey);
    }
    
    /**
     * 批量操作前的钩子
     */
    @Override
    public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
                               MiniBatchOperationInProgress<org.apache.hadoop.hbase.client.Mutation> miniBatchOp)
            throws IOException {
        
        LOG.info("=== Audit Log: Batch Operation ===");
        LOG.info("Batch size: {}", miniBatchOp.size());
        
        for (int i = 0; i < miniBatchOp.size(); i++) {
            org.apache.hadoop.hbase.client.Mutation mutation = miniBatchOp.getOperation(i);
            String rowKey = Bytes.toString(mutation.getRow());
            String operationType = mutation instanceof Put ? "PUT" : "DELETE";
            
            LOG.info("Operation {}: Type={}, RowKey={}", i, operationType, rowKey);
        }
    }
    
    /**
     * 写入审计日志到单独的表
     * 实际生产环境中建议使用
     */
    private void writeAuditLog(ObserverContext<RegionCoprocessorEnvironment> c,
                               String operationType,
                               String rowKey) {
        // 伪代码示例
        // Table auditTable = connection.getTable(TableName.valueOf("audit_log"));
        // Put auditPut = new Put(Bytes.toBytes(System.currentTimeMillis() + "_" + rowKey));
        // auditPut.addColumn(Bytes.toBytes("log"), Bytes.toBytes("operation"), Bytes.toBytes(operationType));
        // auditPut.addColumn(Bytes.toBytes("log"), Bytes.toBytes("timestamp"), Bytes.toBytes(System.currentTimeMillis()));
        // auditTable.put(auditPut);
    }
}
