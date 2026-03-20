package com.bigdata.hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.{AclEntry, AclEntryScope, AclEntryType, FsAction, FsPermission}
import org.apache.hadoop.hdfs.{DistributedFileSystem, HdfsConfiguration}
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.compress.{CompressionCodecFactory, GzipCodec}

import java.net.URI
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
 * HDFS 高级特性 Scala 示例
 * 
 * 函数式编程风格实现
 */
object HDFSAdvancedScalaApp {
  
  private val HDFS_URI = "hdfs://localhost:9000"
  private var fs: FileSystem = _
  private var conf: Configuration = _
  
  def main(args: Array[String]): Unit = {
    try {
      initFileSystem()
      
      println("========== HDFS 高级特性 Scala 示例 ==========\n")
      
      // 使用函数式编程风格执行各个功能
      val features = List(
        ("快照管理", snapshotManagement _),
        ("配额管理", quotaManagement _),
        ("文件压缩与合并", compressionAndMerge _),
        ("副本与块管理", replicationAndBlock _),
        ("存储策略", storagePolicyDemo _),
        ("权限与ACL", permissionManagement _),
        ("批量操作", batchOperations _),
        ("文件监控", fileWatch _)
      )
      
      features.foreach { case (name, func) =>
        executeFeature(name, func)
      }
      
      println("\n✓ 所有功能演示完成!")
      
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      closeFileSystem()
    }
  }
  
  /**
   * 初始化文件系统
   */
  private def initFileSystem(): Unit = {
    conf = new HdfsConfiguration()
    conf.set("fs.defaultFS", HDFS_URI)
    conf.set("dfs.replication", "3")
    fs = FileSystem.get(new URI(HDFS_URI), conf, "hadoop")
  }
  
  /**
   * 执行功能并捕获异常
   */
  private def executeFeature(name: String, func: () => Unit): Unit = {
    Try(func()) match {
      case Success(_) => // 成功
      case Failure(e) => println(s"✗ $name 执行失败: ${e.getMessage}")
    }
  }
  
  /**
   * 1. 快照管理
   */
  private def snapshotManagement(): Unit = {
    println("========== 1. 快照管理 (Scala) ==========")
    
    val snapshotDir = "/user/test/snapshot-scala"
    val dirPath = new Path(snapshotDir)
    
    fs.mkdirs(dirPath)
    
    // 使用函数式风格创建文件
    withOutputStream(new Path(snapshotDir, "data.txt")) { out =>
      out.writeUTF("Scala functional style data")
    }
    
    fs match {
      case dfs: DistributedFileSystem =>
        // 启用快照
        dfs.allowSnapshot(dirPath)
        println(s"✓ 启用快照: $snapshotDir")
        
        // 创建快照
        val snap1 = dfs.createSnapshot(dirPath, "scala_snap1")
        println(s"✓ 创建快照: $snap1")
        
        // 快照操作的函数式封装
        val snapshots = Option(dfs.getFileStatus(dirPath).getSnapshots)
          .map(_.toList)
          .getOrElse(List.empty)
        
        snapshots.foreach { snap =>
          println(s"  - ${snap.getPath.getName}")
        }
        
        // 清理
        dfs.deleteSnapshot(dirPath, "scala_snap1")
        dfs.disallowSnapshot(dirPath)
        
      case _ => println("非 DistributedFileSystem")
    }
    println()
  }
  
  /**
   * 2. 配额管理
   */
  private def quotaManagement(): Unit = {
    println("========== 2. 配额管理 (Scala) ==========")
    
    val quotaDir = new Path("/user/test/quota-scala")
    fs.mkdirs(quotaDir)
    
    fs match {
      case dfs: DistributedFileSystem =>
        // 设置配额
        val nameQuota = 100L
        val spaceQuota = 1024L * 1024 * 1024 // 1GB
        
        dfs.setQuota(quotaDir, nameQuota, spaceQuota)
        println(s"✓ 设置配额 - 文件数: $nameQuota, 空间: ${spaceQuota / (1024*1024)} MB")
        
        // 查询配额 (函数式风格)
        val summary = dfs.getContentSummary(quotaDir)
        val quotaInfo = Map(
          "名称配额" -> summary.getQuota,
          "空间配额" -> s"${summary.getSpaceQuota / (1024*1024)} MB",
          "已用文件数" -> summary.getFileAndDirectoryCount,
          "已用空间" -> s"${summary.getSpaceConsumed / 1024} KB"
        )
        
        quotaInfo.foreach { case (k, v) => println(s"  $k: $v") }
        
        // 清除配额
        dfs.setQuota(quotaDir, -1, -1)
        
      case _ => println("非 DistributedFileSystem")
    }
    println()
  }
  
  /**
   * 3. 文件压缩与合并
   */
  private def compressionAndMerge(): Unit = {
    println("========== 3. 文件压缩与合并 (Scala) ==========")
    
    val srcDir = "/user/test/compress-scala"
    val srcPath = new Path(srcDir)
    fs.mkdirs(srcPath)
    
    // 函数式创建多个文件
    val fileContents = (1 to 5).map(i => s"file$i.txt" -> s"Content of Scala file $i\n")
    
    fileContents.foreach { case (name, content) =>
      withOutputStream(new Path(srcDir, name)) { out =>
        out.writeBytes(content * 100) // 重复100次
      }
    }
    println(s"✓ 创建 ${fileContents.size} 个文件")
    
    // 压缩合并
    val mergedFile = new Path("/user/test/merged-scala.txt.gz")
    val codec = new GzipCodec()
    codec.setConf(conf)
    
    withOutputStream(mergedFile) { out =>
      val compressedOut = codec.createOutputStream(out)
      
      // 使用函数式风格读取和合并
      fs.listStatus(srcPath)
        .filter(_.isFile)
        .sortBy(_.getPath.getName)
        .foreach { file =>
          withInputStream(file.getPath) { in =>
            IOUtils.copyBytes(in, compressedOut, conf, false)
          }
        }
      
      compressedOut.close()
    }
    
    val compressedSize = fs.getFileStatus(mergedFile).getLen
    println(s"✓ 合并并压缩完成: ${compressedSize / 1024} KB")
    println()
  }
  
  /**
   * 4. 副本与块管理
   */
  private def replicationAndBlock(): Unit = {
    println("========== 4. 副本与块管理 (Scala) ==========")
    
    val filePath = new Path("/user/test/replication-scala.txt")
    
    // 创建大文件
    withOutputStream(filePath) { out =>
      val data = Array.fill[Byte](1024 * 1024)('S'.toByte)
      (1 to 100).foreach(_ => out.write(data)) // 100MB
    }
    
    val status = fs.getFileStatus(filePath)
    println(s"✓ 文件大小: ${status.getLen / (1024*1024)} MB")
    println(s"✓ 块大小: ${status.getBlockSize / (1024*1024)} MB")
    
    // 获取块信息 (函数式风格)
    val blocks = fs.getFileBlockLocations(status, 0, status.getLen)
    println(s"✓ 块数量: ${blocks.length}")
    
    blocks.take(3).zipWithIndex.foreach { case (block, idx) =>
      println(s"\n  块 ${idx + 1}:")
      println(s"    偏移: ${block.getOffset}")
      println(s"    长度: ${block.getLength / (1024*1024)} MB")
      println(s"    主机: ${block.getHosts.mkString(", ")}")
    }
    
    // 调整副本因子
    val replications = List(5, 2, 3)
    replications.foreach { rep =>
      fs.setReplication(filePath, rep.toShort)
      println(s"\n✓ 设置副本因子为 $rep")
      Thread.sleep(500)
    }
    
    println()
  }
  
  /**
   * 5. 存储策略
   */
  private def storagePolicyDemo(): Unit = {
    println("========== 5. 存储策略 (Scala) ==========")
    
    val filePath = new Path("/user/test/storage-scala.txt")
    withOutputStream(filePath)(_.writeBytes("Storage policy test"))
    
    fs match {
      case dfs: DistributedFileSystem =>
        // 获取所有策略 (函数式风格)
        val policies = dfs.getStoragePolicies
          .map(p => p.getName -> p.toString)
          .toMap
        
        println("✓ 可用存储策略:")
        policies.foreach { case (name, desc) =>
          println(s"  - $name")
        }
        
        // 设置和查询策略
        List("HOT", "WARM", "COLD").foreach { policy =>
          dfs.setStoragePolicy(filePath, policy)
          val current = Option(dfs.getStoragePolicy(filePath))
            .map(_.getName)
            .getOrElse("默认")
          println(s"✓ 设置为 $policy, 当前: $current")
        }
        
        dfs.unsetStoragePolicy(filePath)
        
      case _ => println("非 DistributedFileSystem")
    }
    println()
  }
  
  /**
   * 6. 权限与ACL管理
   */
  private def permissionManagement(): Unit = {
    println("========== 6. 权限与ACL管理 (Scala) ==========")
    
    val filePath = new Path("/user/test/permission-scala.txt")
    withOutputStream(filePath)(_.writeBytes("Permission test"))
    
    // 设置权限 (函数式风格)
    val permissions = List(
      ("Owner Full", new FsPermission(0x1ff.toShort)), // 777
      ("Owner RW", new FsPermission(0x1a4.toShort)),   // 644
      ("Read Only", new FsPermission(0x124.toShort))   // 444
    )
    
    permissions.foreach { case (desc, perm) =>
      fs.setPermission(filePath, perm)
      println(s"✓ 设置权限: $desc (${perm.toString})")
    }
    
    fs match {
      case dfs: DistributedFileSystem =>
        // ACL 操作 (函数式风格)
        val aclEntry = new AclEntry.Builder()
          .setType(AclEntryType.USER)
          .setName("scalauser")
          .setPermission(FsAction.READ_WRITE)
          .build()
        
        Try {
          dfs.modifyAclEntries(filePath, List(aclEntry).asJava)
          println("✓ 添加 ACL: user:scalauser:rw-")
          
          val aclStatus = dfs.getAclStatus(filePath)
          println("✓ ACL 列表:")
          aclStatus.getEntries.asScala.foreach(e => println(s"  - $e"))
          
          dfs.removeAcl(filePath)
        } match {
          case Success(_) => println("✓ ACL 管理成功")
          case Failure(e) => println(s"注意: ACL 功能需要启用 (${e.getMessage})")
        }
        
      case _ => println("非 DistributedFileSystem")
    }
    println()
  }
  
  /**
   * 7. 批量操作
   */
  private def batchOperations(): Unit = {
    println("========== 7. 批量操作 (Scala) ==========")
    
    val batchDir = "/user/test/batch-scala"
    val dirPath = new Path(batchDir)
    fs.mkdirs(dirPath)
    
    // 批量创建文件 (函数式风格)
    val files = (1 to 10).map { i =>
      val path = new Path(batchDir, f"batch_$i%03d.txt")
      withOutputStream(path)(_.writeBytes(s"Batch file $i"))
      path
    }
    println(s"✓ 批量创建 ${files.size} 个文件")
    
    // 批量查询文件信息
    val fileInfos = fs.listStatus(dirPath)
      .filter(_.isFile)
      .map(status => (
        status.getPath.getName,
        status.getLen,
        status.getModificationTime
      ))
    
    println("✓ 文件列表:")
    fileInfos.sortBy(_._1).take(5).foreach { case (name, size, time) =>
      println(f"  - $name: $size bytes, modified: $time")
    }
    
    // 批量删除 (函数式风格)
    val deleted = files.filter(_.getName.endsWith("5.txt"))
      .map(fs.delete(_, false))
      .count(identity)
    
    println(s"✓ 批量删除 $deleted 个文件")
    
    // 统计信息
    val summary = fs.getContentSummary(dirPath)
    println(s"✓ 目录统计:")
    println(s"  文件数: ${summary.getFileCount}")
    println(s"  总大小: ${summary.getLength} bytes")
    println()
  }
  
  /**
   * 8. 文件监控
   */
  private def fileWatch(): Unit = {
    println("========== 8. 文件监控 (Scala) ==========")
    
    val watchDir = new Path("/user/test/watch-scala")
    fs.mkdirs(watchDir)
    
    // 记录初始状态
    val initialFiles = fs.listStatus(watchDir).map(_.getPath.getName).toSet
    println(s"✓ 初始文件数: ${initialFiles.size}")
    
    // 创建新文件
    val newFiles = (1 to 3).map { i =>
      val path = new Path(watchDir, s"new_$i.txt")
      withOutputStream(path)(_.writeBytes(s"New file $i"))
      path.getName
    }
    
    // 检测变化 (函数式风格)
    val currentFiles = fs.listStatus(watchDir).map(_.getPath.getName).toSet
    val added = currentFiles.diff(initialFiles)
    
    println(s"✓ 检测到新增文件:")
    added.foreach(name => println(s"  + $name"))
    
    // 监控文件修改时间
    val modTimes = fs.listStatus(watchDir)
      .map(s => s.getPath.getName -> s.getModificationTime)
      .sortBy(-_._2)
    
    println(s"✓ 最近修改的文件:")
    modTimes.take(3).foreach { case (name, time) =>
      val date = new java.util.Date(time)
      println(f"  - $name: $date")
    }
    println()
  }
  
  // ========== 辅助函数 ==========
  
  /**
   * 资源管理 - OutputStream
   */
  private def withOutputStream[T](path: Path)(f: FSDataOutputStream => T): T = {
    val out = fs.create(path, true)
    try {
      f(out)
    } finally {
      out.close()
    }
  }
  
  /**
   * 资源管理 - InputStream
   */
  private def withInputStream[T](path: Path)(f: FSDataInputStream => T): T = {
    val in = fs.open(path)
    try {
      f(in)
    } finally {
      in.close()
    }
  }
  
  /**
   * 关闭文件系统
   */
  private def closeFileSystem(): Unit = {
    if (fs != null) {
      fs.close()
      println("✓ HDFS 连接已关闭")
    }
  }
}
