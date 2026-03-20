package com.bigdata.hbase.coprocessor;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * HBase Endpoint Coprocessor 示例：服务端聚合
 * 
 * 功能：
 * 1. 在服务端进行数据聚合，减少网络传输
 * 2. 类似数据库的存储过程
 * 3. 支持自定义的复杂计算逻辑
 * 
 * 使用场景：
 * - 行数统计
 * - 求和、平均值计算
 * - 复杂的数据处理
 * 
 * 注意：实际使用需要定义 protobuf 协议
 * 这里提供简化的示例代码
 */
public class RowCountEndpoint implements RegionCoprocessor {
    
    private static final Logger LOG = LoggerFactory.getLogger(RowCountEndpoint.class);
    private RegionCoprocessorEnvironment env;
    
    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment) env;
        } else {
            throw new CoprocessorException("Must be loaded on a Region!");
        }
    }
    
    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        // Cleanup
    }
    
    /**
     * 服务端行数统计
     * 避免将所有数据传输到客户端再统计
     */
    public long getRowCount(Scan scan) throws IOException {
        long count = 0;
        
        InternalScanner scanner = env.getRegion().getScanner(scan);
        
        try {
            List<Cell> results = new ArrayList<>();
            boolean hasMore;
            
            do {
                hasMore = scanner.next(results);
                if (!results.isEmpty()) {
                    count++;
                    results.clear();
                }
            } while (hasMore);
            
        } finally {
            scanner.close();
        }
        
        LOG.info("Row count for region {}: {}", 
                env.getRegionInfo().getRegionNameAsString(), count);
        
        return count;
    }
    
    /**
     * 服务端求和操作
     * 对指定列进行求和
     */
    public long getSum(Scan scan, byte[] family, byte[] qualifier) throws IOException {
        long sum = 0;
        
        InternalScanner scanner = env.getRegion().getScanner(scan);
        
        try {
            List<Cell> results = new ArrayList<>();
            boolean hasMore;
            
            do {
                hasMore = scanner.next(results);
                
                for (Cell cell : results) {
                    if (Bytes.equals(family, cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) &&
                        Bytes.equals(qualifier, cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())) {
                        
                        String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                        try {
                            sum += Long.parseLong(value);
                        } catch (NumberFormatException e) {
                            LOG.warn("Non-numeric value: {}", value);
                        }
                    }
                }
                results.clear();
                
            } while (hasMore);
            
        } finally {
            scanner.close();
        }
        
        LOG.info("Sum for region {}: {}", 
                env.getRegionInfo().getRegionNameAsString(), sum);
        
        return sum;
    }
    
    /**
     * 服务端平均值计算
     */
    public double getAverage(Scan scan, byte[] family, byte[] qualifier) throws IOException {
        long sum = 0;
        long count = 0;
        
        InternalScanner scanner = env.getRegion().getScanner(scan);
        
        try {
            List<Cell> results = new ArrayList<>();
            boolean hasMore;
            
            do {
                hasMore = scanner.next(results);
                
                for (Cell cell : results) {
                    if (Bytes.equals(family, cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) &&
                        Bytes.equals(qualifier, cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())) {
                        
                        String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                        try {
                            sum += Long.parseLong(value);
                            count++;
                        } catch (NumberFormatException e) {
                            LOG.warn("Non-numeric value: {}", value);
                        }
                    }
                }
                results.clear();
                
            } while (hasMore);
            
        } finally {
            scanner.close();
        }
        
        double average = count > 0 ? (double) sum / count : 0.0;
        
        LOG.info("Average for region {}: {}", 
                env.getRegionInfo().getRegionNameAsString(), average);
        
        return average;
    }
    
    /**
     * 服务端最大值查找
     */
    public long getMax(Scan scan, byte[] family, byte[] qualifier) throws IOException {
        long max = Long.MIN_VALUE;
        boolean found = false;
        
        InternalScanner scanner = env.getRegion().getScanner(scan);
        
        try {
            List<Cell> results = new ArrayList<>();
            boolean hasMore;
            
            do {
                hasMore = scanner.next(results);
                
                for (Cell cell : results) {
                    if (Bytes.equals(family, cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) &&
                        Bytes.equals(qualifier, cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())) {
                        
                        String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                        try {
                            long numValue = Long.parseLong(value);
                            if (!found || numValue > max) {
                                max = numValue;
                                found = true;
                            }
                        } catch (NumberFormatException e) {
                            LOG.warn("Non-numeric value: {}", value);
                        }
                    }
                }
                results.clear();
                
            } while (hasMore);
            
        } finally {
            scanner.close();
        }
        
        LOG.info("Max for region {}: {}", 
                env.getRegionInfo().getRegionNameAsString(), max);
        
        return max;
    }
}
