package com.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;

/**
 * HDFS 文件操作 - Java 实现
 * 演示 HDFS 文件系统的各种操作
 */
public class HDFSJavaApp {
    
    private static final Logger logger = LoggerFactory.getLogger(HDFSJavaApp.class);
    private static final String HDFS_URI = "hdfs://localhost:9000";
    
    private Configuration conf;
    private FileSystem fs;
    
    public HDFSJavaApp() throws IOException {
        conf = new Configuration();
        fs = FileSystem.get(URI.create(HDFS_URI), conf);
    }
    
    public static void main(String[] args) {
        try {
            HDFSJavaApp app = new HDFSJavaApp();
            
            // 1. 创建目录
            app.createDirectory("/bigdata/test");
            
            // 2. 上传文件
            app.uploadFile("test.txt", "/bigdata/test/test.txt");
            
            // 3. 下载文件
            app.downloadFile("/bigdata/test/test.txt", "downloaded.txt");
            
            // 4. 列出文件
            app.listFiles("/bigdata");
            
            // 5. 读取文件内容
            app.readFile("/bigdata/test/test.txt");
            
            // 6. 追加内容
            app.appendToFile("/bigdata/test/test.txt", "Appended content\n");
            
            // 7. 重命名文件
            app.renameFile("/bigdata/test/test.txt", "/bigdata/test/renamed.txt");
            
            // 8. 删除文件
            app.deleteFile("/bigdata/test/renamed.txt");
            
            // 9. 获取文件状态
            app.getFileStatus("/bigdata");
            
            // 10. 复制文件
            app.copyFile("/bigdata/source.txt", "/bigdata/target.txt");
            
            app.close();
            
        } catch (Exception e) {
            logger.error("Error in HDFS application: ", e);
        }
    }
    
    /**
     * 创建目录
     */
    public void createDirectory(String path) throws IOException {
        Path hdfsPath = new Path(path);
        
        if (fs.exists(hdfsPath)) {
            logger.info("Directory {} already exists", path);
        } else {
            fs.mkdirs(hdfsPath);
            logger.info("Directory {} created successfully", path);
        }
    }
    
    /**
     * 上传文件到 HDFS
     */
    public void uploadFile(String localPath, String hdfsPath) throws IOException {
        File localFile = new File(localPath);
        
        // 如果本地文件不存在,创建一个示例文件
        if (!localFile.exists()) {
            try (FileWriter writer = new FileWriter(localFile)) {
                writer.write("This is a test file for HDFS operations.\n");
                writer.write("Line 2: Hello, Hadoop!\n");
                writer.write("Line 3: Big Data Processing\n");
            }
            logger.info("Created local test file: {}", localPath);
        }
        
        Path srcPath = new Path(localPath);
        Path dstPath = new Path(hdfsPath);
        
        fs.copyFromLocalFile(false, true, srcPath, dstPath);
        logger.info("File uploaded from {} to {}", localPath, hdfsPath);
    }
    
    /**
     * 从 HDFS 下载文件
     */
    public void downloadFile(String hdfsPath, String localPath) throws IOException {
        Path srcPath = new Path(hdfsPath);
        Path dstPath = new Path(localPath);
        
        if (fs.exists(srcPath)) {
            fs.copyToLocalFile(false, srcPath, dstPath);
            logger.info("File downloaded from {} to {}", hdfsPath, localPath);
        } else {
            logger.warn("HDFS file {} does not exist", hdfsPath);
        }
    }
    
    /**
     * 列出目录中的文件
     */
    public void listFiles(String path) throws IOException {
        Path hdfsPath = new Path(path);
        
        if (!fs.exists(hdfsPath)) {
            logger.warn("Path {} does not exist", path);
            return;
        }
        
        logger.info("=== Listing files in {} ===", path);
        
        FileStatus[] fileStatuses = fs.listStatus(hdfsPath);
        
        for (FileStatus status : fileStatuses) {
            String type = status.isDirectory() ? "DIR" : "FILE";
            String name = status.getPath().getName();
            long size = status.getLen();
            String permission = status.getPermission().toString();
            
            logger.info("{} | {} | Size: {} bytes | Permission: {}", 
                    type, name, size, permission);
        }
    }
    
    /**
     * 递归列出所有文件
     */
    public void listFilesRecursive(String path) throws IOException {
        Path hdfsPath = new Path(path);
        
        RemoteIterator<LocatedFileStatus> fileIterator = fs.listFiles(hdfsPath, true);
        
        logger.info("=== Listing all files recursively in {} ===", path);
        
        while (fileIterator.hasNext()) {
            LocatedFileStatus status = fileIterator.next();
            logger.info("File: {} | Size: {} bytes", 
                    status.getPath(), status.getLen());
        }
    }
    
    /**
     * 读取文件内容
     */
    public void readFile(String path) throws IOException {
        Path hdfsPath = new Path(path);
        
        if (!fs.exists(hdfsPath)) {
            logger.warn("File {} does not exist", path);
            return;
        }
        
        logger.info("=== Reading file: {} ===", path);
        
        FSDataInputStream inputStream = fs.open(hdfsPath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        
        String line;
        while ((line = reader.readLine()) != null) {
            logger.info(line);
        }
        
        reader.close();
    }
    
    /**
     * 追加内容到文件
     */
    public void appendToFile(String hdfsPath, String content) throws IOException {
        Path path = new Path(hdfsPath);
        
        if (!fs.exists(path)) {
            logger.warn("File {} does not exist", hdfsPath);
            return;
        }
        
        // 注意: HDFS 需要配置 dfs.support.append=true
        FSDataOutputStream outputStream = fs.append(path);
        outputStream.write(content.getBytes());
        outputStream.close();
        
        logger.info("Content appended to {}", hdfsPath);
    }
    
    /**
     * 重命名文件
     */
    public void renameFile(String oldPath, String newPath) throws IOException {
        Path oldHdfsPath = new Path(oldPath);
        Path newHdfsPath = new Path(newPath);
        
        if (fs.exists(oldHdfsPath)) {
            boolean success = fs.rename(oldHdfsPath, newHdfsPath);
            if (success) {
                logger.info("File renamed from {} to {}", oldPath, newPath);
            } else {
                logger.warn("Failed to rename file from {} to {}", oldPath, newPath);
            }
        } else {
            logger.warn("File {} does not exist", oldPath);
        }
    }
    
    /**
     * 删除文件或目录
     */
    public void deleteFile(String path) throws IOException {
        Path hdfsPath = new Path(path);
        
        if (fs.exists(hdfsPath)) {
            boolean success = fs.delete(hdfsPath, true); // recursive delete
            if (success) {
                logger.info("File/Directory {} deleted successfully", path);
            } else {
                logger.warn("Failed to delete {}", path);
            }
        } else {
            logger.warn("File/Directory {} does not exist", path);
        }
    }
    
    /**
     * 获取文件状态
     */
    public void getFileStatus(String path) throws IOException {
        Path hdfsPath = new Path(path);
        
        if (!fs.exists(hdfsPath)) {
            logger.warn("Path {} does not exist", path);
            return;
        }
        
        FileStatus status = fs.getFileStatus(hdfsPath);
        
        logger.info("=== File Status for {} ===", path);
        logger.info("Path: {}", status.getPath());
        logger.info("Is Directory: {}", status.isDirectory());
        logger.info("Length: {} bytes", status.getLen());
        logger.info("Replication: {}", status.getReplication());
        logger.info("Block Size: {} bytes", status.getBlockSize());
        logger.info("Modification Time: {}", status.getModificationTime());
        logger.info("Owner: {}", status.getOwner());
        logger.info("Group: {}", status.getGroup());
        logger.info("Permission: {}", status.getPermission());
    }
    
    /**
     * 复制文件
     */
    public void copyFile(String srcPath, String dstPath) throws IOException {
        Path src = new Path(srcPath);
        Path dst = new Path(dstPath);
        
        if (!fs.exists(src)) {
            logger.warn("Source file {} does not exist", srcPath);
            return;
        }
        
        FSDataInputStream inputStream = fs.open(src);
        FSDataOutputStream outputStream = fs.create(dst);
        
        IOUtils.copyBytes(inputStream, outputStream, conf, true);
        
        logger.info("File copied from {} to {}", srcPath, dstPath);
    }
    
    /**
     * 关闭文件系统
     */
    public void close() throws IOException {
        if (fs != null) {
            fs.close();
            logger.info("HDFS FileSystem closed");
        }
    }
}
