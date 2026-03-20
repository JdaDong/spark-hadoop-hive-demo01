package com.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.GzipCodec;

import java.io.*;
import java.net.URI;
import java.util.Arrays;

/**
 * HDFS 高级特性示例
 * 
 * 功能包括:
 * 1. 快照管理
 * 2. 配额管理
 * 3. 文件压缩
 * 4. 文件合并
 * 5. 副本因子调整
 * 6. 块信息查询
 * 7. DataNode 管理
 * 8. 存储策略管理
 * 9. 垃圾回收机制
 * 10. ACL 权限管理
 * 11. 缓存管理
 * 12. 异构存储
 */
public class HDFSAdvancedJavaApp {
    
    private static FileSystem fs;
    private static Configuration conf;
    private static final String HDFS_URI = "hdfs://localhost:9000";
    
    public static void main(String[] args) {
        try {
            // 初始化配置
            conf = new Configuration();
            conf.set("fs.defaultFS", HDFS_URI);
            conf.set("dfs.replication", "3");
            
            // 获取文件系统
            fs = FileSystem.get(new URI(HDFS_URI), conf, "hadoop");
            
            System.out.println("========== HDFS 高级特性示例 ==========\n");
            
            // 1. 快照管理
            snapshotManagement();
            
            // 2. 配额管理
            quotaManagement();
            
            // 3. 文件压缩
            compressionDemo();
            
            // 4. 文件合并
            mergeSmallFiles();
            
            // 5. 副本因子调整
            replicationFactorManagement();
            
            // 6. 块信息查询
            blockLocationInfo();
            
            // 7. DataNode 管理
            datanodeInfo();
            
            // 8. 存储策略管理
            storagePolicyManagement();
            
            // 9. 垃圾回收机制
            trashManagement();
            
            // 10. ACL 权限管理
            aclManagement();
            
            // 11. 缓存管理
            cacheManagement();
            
            // 12. 文件校验和
            checksumDemo();
            
            System.out.println("\n所有高级特性演示完成!");
            
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeFileSystem();
        }
    }
    
    /**
     * 1. 快照管理
     * 快照是文件系统在某个时刻的只读副本
     */
    private static void snapshotManagement() throws Exception {
        System.out.println("========== 1. 快照管理 ==========");
        
        String snapshotDir = "/user/test/snapshot-demo";
        Path dirPath = new Path(snapshotDir);
        
        // 创建目录和文件
        fs.mkdirs(dirPath);
        Path filePath = new Path(snapshotDir + "/data.txt");
        try (FSDataOutputStream out = fs.create(filePath)) {
            out.writeUTF("Original data");
        }
        
        if (fs instanceof DistributedFileSystem) {
            DistributedFileSystem dfs = (DistributedFileSystem) fs;
            
            // 允许目录创建快照
            dfs.allowSnapshot(dirPath);
            System.out.println("✓ 启用快照功能: " + snapshotDir);
            
            // 创建快照
            Path snapshot1 = dfs.createSnapshot(dirPath, "snap1");
            System.out.println("✓ 创建快照1: " + snapshot1);
            
            // 修改数据
            try (FSDataOutputStream out = fs.create(filePath, true)) {
                out.writeUTF("Modified data");
            }
            System.out.println("✓ 修改原始数据");
            
            // 创建第二个快照
            Path snapshot2 = dfs.createSnapshot(dirPath, "snap2");
            System.out.println("✓ 创建快照2: " + snapshot2);
            
            // 查看快照目录
            Path snapPath = new Path(snapshotDir + "/.snapshot/snap1/data.txt");
            try (FSDataInputStream in = fs.open(snapPath)) {
                String content = in.readUTF();
                System.out.println("✓ 快照1内容: " + content);
            }
            
            // 列出所有快照
            FileStatus[] snapshots = dfs.getFileStatus(dirPath).getSnapshots();
            System.out.println("✓ 快照列表:");
            for (FileStatus snap : snapshots) {
                System.out.println("  - " + snap.getPath().getName());
            }
            
            // 重命名快照
            dfs.renameSnapshot(dirPath, "snap1", "snapshot_v1");
            System.out.println("✓ 重命名快照: snap1 -> snapshot_v1");
            
            // 删除快照
            dfs.deleteSnapshot(dirPath, "snapshot_v1");
            System.out.println("✓ 删除快照: snapshot_v1");
            
            // 禁止快照
            dfs.disallowSnapshot(dirPath);
            System.out.println("✓ 禁用快照功能\n");
        }
    }
    
    /**
     * 2. 配额管理
     * 限制目录的名称配额和空间配额
     */
    private static void quotaManagement() throws Exception {
        System.out.println("========== 2. 配额管理 ==========");
        
        String quotaDir = "/user/test/quota-demo";
        Path dirPath = new Path(quotaDir);
        
        fs.mkdirs(dirPath);
        
        if (fs instanceof DistributedFileSystem) {
            DistributedFileSystem dfs = (DistributedFileSystem) fs;
            
            // 设置名称配额(最多10个文件/目录)
            dfs.setQuota(dirPath, 10, HdfsConstants.QUOTA_DONT_SET);
            System.out.println("✓ 设置名称配额: 最多10个文件/目录");
            
            // 设置空间配额(最多1GB)
            long spaceQuota = 1024L * 1024 * 1024; // 1GB
            dfs.setQuota(dirPath, HdfsConstants.QUOTA_DONT_SET, spaceQuota);
            System.out.println("✓ 设置空间配额: 1GB");
            
            // 查询配额信息
            ContentSummary summary = dfs.getContentSummary(dirPath);
            System.out.println("✓ 配额信息:");
            System.out.println("  名称配额: " + summary.getQuota());
            System.out.println("  空间配额: " + summary.getSpaceQuota() / (1024 * 1024) + " MB");
            System.out.println("  已用文件数: " + summary.getFileAndDirectoryCount());
            System.out.println("  已用空间: " + summary.getSpaceConsumed() / 1024 + " KB");
            
            // 清除配额
            dfs.setQuota(dirPath, HdfsConstants.QUOTA_RESET, HdfsConstants.QUOTA_RESET);
            System.out.println("✓ 清除配额限制\n");
        }
    }
    
    /**
     * 3. 文件压缩
     * 支持多种压缩格式
     */
    private static void compressionDemo() throws Exception {
        System.out.println("========== 3. 文件压缩 ==========");
        
        String srcFile = "/user/test/compress/source.txt";
        String gzipFile = "/user/test/compress/source.txt.gz";
        
        // 创建源文件
        Path srcPath = new Path(srcFile);
        fs.mkdirs(srcPath.getParent());
        try (FSDataOutputStream out = fs.create(srcPath)) {
            for (int i = 0; i < 1000; i++) {
                out.writeBytes("This is line " + i + " for compression test.\n");
            }
        }
        
        long originalSize = fs.getFileStatus(srcPath).getLen();
        System.out.println("✓ 原始文件大小: " + originalSize + " bytes");
        
        // Gzip 压缩
        Path gzipPath = new Path(gzipFile);
        CompressionCodec codec = new GzipCodec();
        codec.setConf(conf);
        
        try (FSDataInputStream in = fs.open(srcPath);
             FSDataOutputStream out = fs.create(gzipPath);
             OutputStream compressedOut = codec.createOutputStream(out)) {
            
            IOUtils.copyBytes(in, compressedOut, conf);
        }
        
        long compressedSize = fs.getFileStatus(gzipPath).getLen();
        System.out.println("✓ 压缩后大小: " + compressedSize + " bytes");
        System.out.println("✓ 压缩率: " + String.format("%.2f%%", 
            (1 - (double)compressedSize / originalSize) * 100));
        
        // 解压缩
        String decompressedFile = "/user/test/compress/decompressed.txt";
        Path decompPath = new Path(decompressedFile);
        
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec decompCodec = factory.getCodec(gzipPath);
        
        try (FSDataInputStream in = fs.open(gzipPath);
             InputStream decompressedIn = decompCodec.createInputStream(in);
             FSDataOutputStream out = fs.create(decompPath)) {
            
            IOUtils.copyBytes(decompressedIn, out, conf);
        }
        
        System.out.println("✓ 解压完成: " + decompressedFile);
        System.out.println("✓ 解压后大小: " + fs.getFileStatus(decompPath).getLen() + " bytes\n");
    }
    
    /**
     * 4. 文件合并
     * 合并小文件以提高性能
     */
    private static void mergeSmallFiles() throws Exception {
        System.out.println("========== 4. 文件合并 ==========");
        
        String smallFilesDir = "/user/test/small-files";
        String mergedFile = "/user/test/merged/result.txt";
        
        // 创建多个小文件
        Path dirPath = new Path(smallFilesDir);
        fs.mkdirs(dirPath);
        
        for (int i = 1; i <= 5; i++) {
            Path filePath = new Path(smallFilesDir + "/file" + i + ".txt");
            try (FSDataOutputStream out = fs.create(filePath)) {
                out.writeBytes("Content of file " + i + "\n");
            }
        }
        System.out.println("✓ 创建5个小文件");
        
        // 合并文件
        Path mergedPath = new Path(mergedFile);
        fs.mkdirs(mergedPath.getParent());
        
        try (FSDataOutputStream out = fs.create(mergedPath)) {
            FileStatus[] files = fs.listStatus(dirPath);
            Arrays.sort(files); // 按名称排序
            
            for (FileStatus file : files) {
                if (file.isFile()) {
                    try (FSDataInputStream in = fs.open(file.getPath())) {
                        IOUtils.copyBytes(in, out, conf, false);
                    }
                }
            }
        }
        
        System.out.println("✓ 合并完成: " + mergedFile);
        System.out.println("✓ 合并后大小: " + fs.getFileStatus(mergedPath).getLen() + " bytes\n");
    }
    
    /**
     * 5. 副本因子调整
     */
    private static void replicationFactorManagement() throws Exception {
        System.out.println("========== 5. 副本因子调整 ==========");
        
        String filePath = "/user/test/replication-demo.txt";
        Path path = new Path(filePath);
        
        // 创建文件
        try (FSDataOutputStream out = fs.create(path)) {
            out.writeBytes("Replication test data");
        }
        
        // 查询当前副本因子
        FileStatus status = fs.getFileStatus(path);
        System.out.println("✓ 当前副本因子: " + status.getReplication());
        
        // 设置副本因子为5
        fs.setReplication(path, (short) 5);
        System.out.println("✓ 设置副本因子为 5");
        
        // 等待复制完成
        Thread.sleep(1000);
        
        status = fs.getFileStatus(path);
        System.out.println("✓ 新的副本因子: " + status.getReplication());
        
        // 恢复默认副本因子
        fs.setReplication(path, (short) 3);
        System.out.println("✓ 恢复副本因子为 3\n");
    }
    
    /**
     * 6. 块信息查询
     */
    private static void blockLocationInfo() throws Exception {
        System.out.println("========== 6. 块信息查询 ==========");
        
        String filePath = "/user/test/block-demo.txt";
        Path path = new Path(filePath);
        
        // 创建一个大文件
        try (FSDataOutputStream out = fs.create(path)) {
            byte[] data = new byte[1024 * 1024]; // 1MB
            Arrays.fill(data, (byte) 'A');
            for (int i = 0; i < 150; i++) { // 写入150MB
                out.write(data);
            }
        }
        
        FileStatus status = fs.getFileStatus(path);
        System.out.println("✓ 文件大小: " + status.getLen() / (1024 * 1024) + " MB");
        System.out.println("✓ 块大小: " + status.getBlockSize() / (1024 * 1024) + " MB");
        
        // 获取块位置信息
        BlockLocation[] blocks = fs.getFileBlockLocations(status, 0, status.getLen());
        System.out.println("✓ 块数量: " + blocks.length);
        
        for (int i = 0; i < Math.min(blocks.length, 3); i++) {
            BlockLocation block = blocks[i];
            System.out.println("\n  块 " + (i + 1) + ":");
            System.out.println("    偏移量: " + block.getOffset());
            System.out.println("    长度: " + block.getLength() / (1024 * 1024) + " MB");
            System.out.println("    主机: " + Arrays.toString(block.getHosts()));
            System.out.println("    副本: " + Arrays.toString(block.getNames()));
        }
        System.out.println();
    }
    
    /**
     * 7. DataNode 管理
     */
    private static void datanodeInfo() throws Exception {
        System.out.println("========== 7. DataNode 管理 ==========");
        
        if (fs instanceof DistributedFileSystem) {
            DistributedFileSystem dfs = (DistributedFileSystem) fs;
            
            // 获取所有 DataNode 信息
            DatanodeInfo[] datanodes = dfs.getDataNodeStats();
            System.out.println("✓ DataNode 总数: " + datanodes.length);
            
            for (DatanodeInfo node : datanodes) {
                System.out.println("\nDataNode: " + node.getHostName());
                System.out.println("  状态: " + node.getAdminState());
                System.out.println("  容量: " + node.getCapacity() / (1024 * 1024 * 1024) + " GB");
                System.out.println("  已用: " + node.getDfsUsed() / (1024 * 1024 * 1024) + " GB");
                System.out.println("  剩余: " + node.getRemaining() / (1024 * 1024 * 1024) + " GB");
                System.out.println("  使用率: " + String.format("%.2f%%", node.getDfsUsedPercent()));
            }
        }
        System.out.println();
    }
    
    /**
     * 8. 存储策略管理
     */
    private static void storagePolicyManagement() throws Exception {
        System.out.println("========== 8. 存储策略管理 ==========");
        
        String filePath = "/user/test/storage-policy.txt";
        Path path = new Path(filePath);
        
        // 创建文件
        try (FSDataOutputStream out = fs.create(path)) {
            out.writeBytes("Storage policy test");
        }
        
        if (fs instanceof DistributedFileSystem) {
            DistributedFileSystem dfs = (DistributedFileSystem) fs;
            
            // 获取所有存储策略
            BlockStoragePolicy[] policies = dfs.getStoragePolicies();
            System.out.println("✓ 可用存储策略:");
            for (BlockStoragePolicy policy : policies) {
                System.out.println("  - " + policy.getName() + ": " + policy);
            }
            
            // 设置存储策略
            dfs.setStoragePolicy(path, "COLD");
            System.out.println("\n✓ 设置存储策略为 COLD (归档存储)");
            
            // 获取存储策略
            BlockStoragePolicy policy = dfs.getStoragePolicy(path);
            System.out.println("✓ 当前存储策略: " + (policy != null ? policy.getName() : "默认"));
            
            // 取消存储策略
            dfs.unsetStoragePolicy(path);
            System.out.println("✓ 取消存储策略,恢复默认\n");
        }
    }
    
    /**
     * 9. 垃圾回收机制
     */
    private static void trashManagement() throws Exception {
        System.out.println("========== 9. 垃圾回收机制 ==========");
        
        // 启用垃圾回收
        Configuration trashConf = new Configuration(conf);
        trashConf.set("fs.trash.interval", "1440"); // 1天
        trashConf.set("fs.trash.checkpoint.interval", "60"); // 60分钟
        
        Trash trash = new Trash(trashConf);
        System.out.println("✓ 垃圾回收配置:");
        System.out.println("  回收间隔: 1440分钟 (1天)");
        System.out.println("  检查点间隔: 60分钟");
        
        String filePath = "/user/test/trash-demo.txt";
        Path path = new Path(filePath);
        
        // 创建文件
        try (FSDataOutputStream out = fs.create(path)) {
            out.writeBytes("File to be moved to trash");
        }
        System.out.println("\n✓ 创建测试文件: " + filePath);
        
        // 移动到垃圾箱
        boolean moved = trash.moveToTrash(path);
        System.out.println("✓ 移动到垃圾箱: " + (moved ? "成功" : "失败"));
        
        if (moved) {
            String trashPath = "/user/hadoop/.Trash/Current" + filePath;
            System.out.println("✓ 垃圾箱位置: " + trashPath);
        }
        
        System.out.println("\n提示: 文件在垃圾箱保留时间后会被永久删除\n");
    }
    
    /**
     * 10. ACL 权限管理
     */
    private static void aclManagement() throws Exception {
        System.out.println("========== 10. ACL 权限管理 ==========");
        
        String filePath = "/user/test/acl-demo.txt";
        Path path = new Path(filePath);
        
        // 创建文件
        try (FSDataOutputStream out = fs.create(path)) {
            out.writeBytes("ACL test file");
        }
        
        // 设置基本权限
        fs.setPermission(path, new FsPermission((short) 0644));
        System.out.println("✓ 设置基本权限: rw-r--r--");
        
        if (fs instanceof DistributedFileSystem) {
            DistributedFileSystem dfs = (DistributedFileSystem) fs;
            
            // 设置 ACL
            AclEntry.Builder builder = new AclEntry.Builder();
            builder.setType(AclEntryType.USER);
            builder.setName("testuser");
            builder.setPermission(FsAction.READ_WRITE);
            
            AclEntry entry = builder.build();
            dfs.modifyAclEntries(path, Arrays.asList(entry));
            System.out.println("✓ 添加 ACL: user:testuser:rw-");
            
            // 查看 ACL
            AclStatus aclStatus = dfs.getAclStatus(path);
            System.out.println("✓ 当前 ACL 列表:");
            for (AclEntry e : aclStatus.getEntries()) {
                System.out.println("  - " + e);
            }
            
            // 删除 ACL
            dfs.removeAclEntries(path, Arrays.asList(entry));
            System.out.println("✓ 删除 ACL 条目");
            
            // 完全删除所有 ACL
            dfs.removeAcl(path);
            System.out.println("✓ 删除所有 ACL\n");
        }
    }
    
    /**
     * 11. 缓存管理
     */
    private static void cacheManagement() throws Exception {
        System.out.println("========== 11. 缓存管理 ==========");
        
        String filePath = "/user/test/cache-demo.txt";
        Path path = new Path(filePath);
        
        // 创建文件
        try (FSDataOutputStream out = fs.create(path)) {
            byte[] data = new byte[1024 * 1024]; // 1MB
            Arrays.fill(data, (byte) 'C');
            out.write(data);
        }
        
        if (fs instanceof DistributedFileSystem) {
            DistributedFileSystem dfs = (DistributedFileSystem) fs;
            
            // 创建缓存池
            String poolName = "testpool";
            try {
                dfs.addCachePool(new CachePoolInfo(poolName)
                    .setLimit(100 * 1024 * 1024L) // 100MB
                    .setMode(new FsPermission((short) 0755)));
                System.out.println("✓ 创建缓存池: " + poolName + " (100MB)");
            } catch (Exception e) {
                System.out.println("✓ 缓存池已存在: " + poolName);
            }
            
            // 添加缓存指令
            CacheDirectiveInfo directive = new CacheDirectiveInfo.Builder()
                .setPath(path)
                .setPool(poolName)
                .setReplication((short) 1)
                .build();
            
            try {
                long id = dfs.addCacheDirective(directive);
                System.out.println("✓ 添加缓存指令: " + id);
                
                // 查询缓存指令
                RemoteIterator<CacheDirectiveEntry> iter = 
                    dfs.listCacheDirectives(new CacheDirectiveInfo.Builder().setPool(poolName).build());
                
                System.out.println("✓ 缓存指令列表:");
                while (iter.hasNext()) {
                    CacheDirectiveEntry entry = iter.next();
                    System.out.println("  - ID: " + entry.getInfo().getId());
                    System.out.println("    路径: " + entry.getInfo().getPath());
                    System.out.println("    副本: " + entry.getInfo().getReplication());
                }
                
                // 删除缓存指令
                dfs.removeCacheDirective(id);
                System.out.println("✓ 删除缓存指令: " + id);
                
            } catch (Exception e) {
                System.out.println("注意: 缓存功能需要 HDFS 2.3+ 版本支持");
            }
        }
        System.out.println();
    }
    
    /**
     * 12. 文件校验和
     */
    private static void checksumDemo() throws Exception {
        System.out.println("========== 12. 文件校验和 ==========");
        
        String filePath = "/user/test/checksum-demo.txt";
        Path path = new Path(filePath);
        
        // 创建文件
        try (FSDataOutputStream out = fs.create(path)) {
            out.writeBytes("Checksum test data");
        }
        
        // 获取校验和
        FileChecksum checksum = fs.getFileChecksum(path);
        System.out.println("✓ 校验和信息:");
        System.out.println("  算法: " + checksum.getAlgorithmName());
        System.out.println("  长度: " + checksum.getLength());
        System.out.println("  值: " + bytesToHex(checksum.getBytes()));
        
        // 创建相同内容的另一个文件
        String filePath2 = "/user/test/checksum-demo2.txt";
        Path path2 = new Path(filePath2);
        try (FSDataOutputStream out = fs.create(path2)) {
            out.writeBytes("Checksum test data");
        }
        
        FileChecksum checksum2 = fs.getFileChecksum(path2);
        
        // 比较校验和
        boolean same = Arrays.equals(checksum.getBytes(), checksum2.getBytes());
        System.out.println("\n✓ 两个文件校验和" + (same ? "相同" : "不同"));
        System.out.println("  说明: 内容相同的文件具有相同的校验和\n");
    }
    
    // 辅助方法
    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
    
    private static void closeFileSystem() {
        try {
            if (fs != null) {
                fs.close();
                System.out.println("HDFS 连接已关闭");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
