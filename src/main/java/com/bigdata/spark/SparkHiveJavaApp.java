package com.bigdata.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark + Hive 数据处理 - Java 实现
 * 演示如何使用 Spark SQL 操作 Hive 表
 */
public class SparkHiveJavaApp {
    
    private static final Logger logger = LoggerFactory.getLogger(SparkHiveJavaApp.class);
    
    public static void main(String[] args) {
        // 创建 SparkSession with Hive 支持
        SparkSession spark = SparkSession.builder()
                .appName("Spark Hive Java Application")
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                .enableHiveSupport()
                .getOrCreate();
        
        try {
            // 1. 创建 Hive 数据库
            createDatabase(spark);
            
            // 2. 创建表并插入数据
            createTableAndInsertData(spark);
            
            // 3. 查询数据
            queryData(spark);
            
            // 4. 数据聚合分析
            aggregateAnalysis(spark);
            
            // 5. 分区表操作
            partitionedTableOperations(spark);
            
        } catch (Exception e) {
            logger.error("Error in Spark Hive application: ", e);
        } finally {
            spark.stop();
        }
    }
    
    /**
     * 创建 Hive 数据库
     */
    private static void createDatabase(SparkSession spark) {
        logger.info("Creating Hive database...");
        spark.sql("CREATE DATABASE IF NOT EXISTS bigdata_db");
        spark.sql("USE bigdata_db");
        logger.info("Database 'bigdata_db' created/selected successfully");
    }
    
    /**
     * 创建表并插入数据
     */
    private static void createTableAndInsertData(SparkSession spark) {
        logger.info("Creating table and inserting data...");
        
        // 创建员工表
        spark.sql("DROP TABLE IF EXISTS employees");
        spark.sql("CREATE TABLE IF NOT EXISTS employees (" +
                "id INT, " +
                "name STRING, " +
                "age INT, " +
                "department STRING, " +
                "salary DOUBLE) " +
                "STORED AS PARQUET");
        
        // 插入示例数据
        spark.sql("INSERT INTO employees VALUES " +
                "(1, 'Alice', 28, 'Engineering', 85000), " +
                "(2, 'Bob', 35, 'Sales', 65000), " +
                "(3, 'Charlie', 32, 'Engineering', 95000), " +
                "(4, 'David', 45, 'Management', 120000), " +
                "(5, 'Eve', 29, 'Sales', 70000), " +
                "(6, 'Frank', 38, 'Engineering', 105000), " +
                "(7, 'Grace', 42, 'Management', 130000), " +
                "(8, 'Henry', 31, 'Sales', 68000)");
        
        logger.info("Table 'employees' created and data inserted successfully");
    }
    
    /**
     * 查询数据
     */
    private static void queryData(SparkSession spark) {
        logger.info("Querying employee data...");
        
        // 查询所有员工
        Dataset<Row> allEmployees = spark.sql("SELECT * FROM employees ORDER BY id");
        logger.info("All Employees:");
        allEmployees.show();
        
        // 查询工程部门员工
        Dataset<Row> engineeringEmployees = spark.sql(
                "SELECT name, age, salary FROM employees " +
                "WHERE department = 'Engineering' " +
                "ORDER BY salary DESC");
        logger.info("Engineering Department Employees:");
        engineeringEmployees.show();
        
        // 高薪员工
        Dataset<Row> highSalaryEmployees = spark.sql(
                "SELECT * FROM employees WHERE salary > 90000");
        logger.info("High Salary Employees (> 90000):");
        highSalaryEmployees.show();
    }
    
    /**
     * 数据聚合分析
     */
    private static void aggregateAnalysis(SparkSession spark) {
        logger.info("Performing aggregate analysis...");
        
        // 按部门统计
        Dataset<Row> deptStats = spark.sql(
                "SELECT department, " +
                "COUNT(*) as employee_count, " +
                "AVG(age) as avg_age, " +
                "AVG(salary) as avg_salary, " +
                "MAX(salary) as max_salary, " +
                "MIN(salary) as min_salary " +
                "FROM employees " +
                "GROUP BY department " +
                "ORDER BY avg_salary DESC");
        logger.info("Department Statistics:");
        deptStats.show();
        
        // 总体统计
        Dataset<Row> overallStats = spark.sql(
                "SELECT COUNT(*) as total_employees, " +
                "AVG(salary) as avg_salary, " +
                "SUM(salary) as total_salary " +
                "FROM employees");
        logger.info("Overall Statistics:");
        overallStats.show();
    }
    
    /**
     * 分区表操作
     */
    private static void partitionedTableOperations(SparkSession spark) {
        logger.info("Creating partitioned table...");
        
        // 创建分区表
        spark.sql("DROP TABLE IF EXISTS sales_data");
        spark.sql("CREATE TABLE IF NOT EXISTS sales_data (" +
                "order_id INT, " +
                "product_name STRING, " +
                "amount DOUBLE, " +
                "order_date STRING) " +
                "PARTITIONED BY (year INT, month INT) " +
                "STORED AS PARQUET");
        
        // 动态分区插入
        spark.sql("SET hive.exec.dynamic.partition=true");
        spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict");
        
        // 插入分区数据
        spark.sql("INSERT INTO sales_data PARTITION(year, month) " +
                "SELECT 1001, 'Laptop', 1200.00, '2026-01-15', 2026, 1 " +
                "UNION ALL SELECT 1002, 'Mouse', 25.00, '2026-01-20', 2026, 1 " +
                "UNION ALL SELECT 1003, 'Keyboard', 75.00, '2026-02-10', 2026, 2 " +
                "UNION ALL SELECT 1004, 'Monitor', 300.00, '2026-02-15', 2026, 2");
        
        // 查询分区数据
        Dataset<Row> salesData = spark.sql(
                "SELECT * FROM sales_data WHERE year=2026 AND month=1");
        logger.info("Sales Data for 2026-01:");
        salesData.show();
        
        // 显示分区
        spark.sql("SHOW PARTITIONS sales_data").show();
    }
}
