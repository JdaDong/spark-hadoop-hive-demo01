package com.bigdata.hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils
import org.slf4j.{Logger, LoggerFactory}

import java.io.{BufferedReader, InputStreamReader}
import java.net.URI
import scala.collection.JavaConverters._

/**
 * HDFS 文件操作 - Scala 实现
 * 演示 HDFS 高级操作和性能优化
 */
object HDFSScalaApp {
  
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val HDFS_URI = "hdfs://localhost:9000"
  
  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(URI.create(HDFS_URI), conf)
    
    try {
      // 1. 目录操作
      directoryOperations(fs)
      
      // 2. 批量文件操作
      batchFileOperations(fs)
      
      // 3. 文件块信息
      getBlockLocations(fs, "/bigdata/data.txt")
      
      // 4. 文件校验和
      getFileChecksum(fs, "/bigdata/data.txt")
      
      // 5. 设置副本数
      setReplication(fs, "/bigdata/data.txt", 3)
      
      // 6. 合并小文件
      mergeSmallFiles(fs, "/bigdata/small_files", "/bigdata/merged.txt")
      
      // 7. 并行读取文件
      parallelReadFiles(fs, "/bigdata/test")
      
    } catch {
      case e: Exception =>
        logger.error("Error in HDFS Scala application", e)
    } finally {
      fs.close()
      logger.info("HDFS FileSystem closed")
    }
  }
  
  /**
   * 目录操作
   */
  def directoryOperations(fs: FileSystem): Unit = {
    logger.info("=== Directory Operations ===")
    
    val baseDir = new Path("/bigdata/scala_test")
    
    // 创建多级目录
    if (!fs.exists(baseDir)) {
      fs.mkdirs(baseDir)
      logger.info(s"Created directory: $baseDir")
    }
    
    // 创建子目录
    val subDirs = Seq("input", "output", "temp")
    subDirs.foreach { dir =>
      val path = new Path(baseDir, dir)
      if (!fs.exists(path)) {
        fs.mkdirs(path)
        logger.info(s"Created subdirectory: $path")
      }
    }
    
    // 列出目录树
    listDirectoryTree(fs, baseDir, 0)
  }
  
  /**
   * 递归列出目录树
   */
  def listDirectoryTree(fs: FileSystem, path: Path, level: Int): Unit = {
    val indent = "  " * level
    val status = fs.getFileStatus(path)
    
    val typeIcon = if (status.isDirectory) "📁" else "📄"
    logger.info(s"$indent$typeIcon ${path.getName}")
    
    if (status.isDirectory) {
      fs.listStatus(path).foreach { fileStatus =>
        listDirectoryTree(fs, fileStatus.getPath, level + 1)
      }
    }
  }
  
  /**
   * 批量文件操作
   */
  def batchFileOperations(fs: FileSystem): Unit = {
    logger.info("=== Batch File Operations ===")
    
    val baseDir = new Path("/bigdata/batch_test")
    fs.mkdirs(baseDir)
    
    // 批量创建文件
    (1 to 5).foreach { i =>
      val filePath = new Path(baseDir, s"file$i.txt")
      val outputStream = fs.create(filePath)
      outputStream.writeBytes(s"This is test file number $i\n")
      outputStream.writeBytes(s"Created at: ${System.currentTimeMillis()}\n")
      outputStream.close()
    }
    logger.info("Created 5 test files")
    
    // 批量读取文件大小
    val totalSize = fs.listStatus(baseDir)
      .filter(_.isFile)
      .map(_.getLen)
      .sum
    
    logger.info(s"Total size of files: $totalSize bytes")
    
    // 查找大文件
    val largeFiles = fs.listStatus(baseDir)
      .filter(status => status.isFile && status.getLen > 50)
      .map(_.getPath.getName)
    
    logger.info(s"Large files (>50 bytes): ${largeFiles.mkString(", ")}")
  }
  
  /**
   * 获取文件块位置信息
   */
  def getBlockLocations(fs: FileSystem, path: String): Unit = {
    val hdfsPath = new Path(path)
    
    if (!fs.exists(hdfsPath)) {
      logger.warn(s"File $path does not exist")
      return
    }
    
    logger.info(s"=== Block Locations for $path ===")
    
    val fileStatus = fs.getFileStatus(hdfsPath)
    val blockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen)
    
    blockLocations.zipWithIndex.foreach { case (block, index) =>
      logger.info(s"Block $index:")
      logger.info(s"  Offset: ${block.getOffset}")
      logger.info(s"  Length: ${block.getLength}")
      logger.info(s"  Hosts: ${block.getHosts.mkString(", ")}")
      logger.info(s"  Names: ${block.getNames.mkString(", ")}")
    }
  }
  
  /**
   * 获取文件校验和
   */
  def getFileChecksum(fs: FileSystem, path: String): Unit = {
    val hdfsPath = new Path(path)
    
    if (!fs.exists(hdfsPath)) {
      logger.warn(s"File $path does not exist")
      return
    }
    
    val checksum = fs.getFileChecksum(hdfsPath)
    logger.info(s"=== File Checksum for $path ===")
    logger.info(s"Algorithm: ${checksum.getAlgorithmName}")
    logger.info(s"Length: ${checksum.getLength}")
    logger.info(s"Bytes: ${checksum.getBytes.map("%02x".format(_)).mkString}")
  }
  
  /**
   * 设置文件副本数
   */
  def setReplication(fs: FileSystem, path: String, replication: Short): Unit = {
    val hdfsPath = new Path(path)
    
    if (!fs.exists(hdfsPath)) {
      logger.warn(s"File $path does not exist")
      return
    }
    
    val success = fs.setReplication(hdfsPath, replication)
    if (success) {
      logger.info(s"Set replication of $path to $replication")
    } else {
      logger.warn(s"Failed to set replication for $path")
    }
  }
  
  /**
   * 合并小文件
   */
  def mergeSmallFiles(fs: FileSystem, sourceDir: String, targetFile: String): Unit = {
    val srcPath = new Path(sourceDir)
    val dstPath = new Path(targetFile)
    
    if (!fs.exists(srcPath)) {
      logger.warn(s"Source directory $sourceDir does not exist")
      return
    }
    
    logger.info(s"=== Merging files from $sourceDir to $targetFile ===")
    
    val outputStream = fs.create(dstPath, true)
    
    try {
      val files = fs.listStatus(srcPath).filter(_.isFile)
      
      files.foreach { fileStatus =>
        logger.info(s"Merging: ${fileStatus.getPath.getName}")
        val inputStream = fs.open(fileStatus.getPath)
        IOUtils.copyBytes(inputStream, outputStream, 4096, false)
        inputStream.close()
        outputStream.writeBytes("\n") // 添加分隔符
      }
      
      logger.info(s"Merged ${files.length} files into $targetFile")
      
    } finally {
      outputStream.close()
    }
  }
  
  /**
   * 并行读取文件内容
   */
  def parallelReadFiles(fs: FileSystem, dirPath: String): Unit = {
    val path = new Path(dirPath)
    
    if (!fs.exists(path)) {
      logger.warn(s"Directory $dirPath does not exist")
      return
    }
    
    logger.info(s"=== Parallel reading files from $dirPath ===")
    
    val files = fs.listStatus(path).filter(_.isFile)
    
    // 使用并行集合读取文件
    val results = files.par.map { fileStatus =>
      val filePath = fileStatus.getPath
      val inputStream = fs.open(filePath)
      val reader = new BufferedReader(new InputStreamReader(inputStream))
      
      try {
        val lines = Iterator.continually(reader.readLine())
          .takeWhile(_ != null)
          .toList
        (filePath.getName, lines.size)
      } finally {
        reader.close()
      }
    }
    
    results.seq.foreach { case (fileName, lineCount) =>
      logger.info(s"File: $fileName, Lines: $lineCount")
    }
  }
  
  /**
   * 获取目录大小
   */
  def getDirectorySize(fs: FileSystem, path: Path): Long = {
    if (!fs.exists(path)) return 0L
    
    val status = fs.getFileStatus(path)
    
    if (status.isFile) {
      status.getLen
    } else {
      fs.listStatus(path)
        .map(s => getDirectorySize(fs, s.getPath))
        .sum
    }
  }
  
  /**
   * 查找文件
   */
  def findFiles(fs: FileSystem, baseDir: Path, pattern: String): Seq[Path] = {
    if (!fs.exists(baseDir)) return Seq.empty
    
    fs.listStatus(baseDir).flatMap { status =>
      if (status.isDirectory) {
        findFiles(fs, status.getPath, pattern)
      } else if (status.getPath.getName.matches(pattern)) {
        Seq(status.getPath)
      } else {
        Seq.empty
      }
    }
  }
}
