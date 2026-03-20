package com.bigdata.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hive.jdbc.HiveDriver;

import java.sql.*;
import java.util.Properties;

/**
 * Hive 高级特性 Java 示例
 * 
 * 功能包括:
 * 1. 分区表管理
 * 2. 分桶表(Bucketing)
 * 3. 动态分区
 * 4. 复杂数据类型(Array, Map, Struct)
 * 5. 窗口函数
 * 6. UDF(用户自定义函数)
 * 7. 事务表(ACID)
 * 8. 表优化(Compaction)
 * 9. 视图和物化视图
 * 10. 索引
 * 11. 性能优化
 * 12. 查询优化器
 */
public class HiveAdvancedJavaApp {
    
    private static final String JDBC_URL = "jdbc:hive2://localhost:10000/default";
    private static final String USERNAME = "hadoop";
    private static final String PASSWORD = "";
    
    private static Connection connection;
    
    public static void main(String[] args) {
        try {
            // 加载 Hive JDBC 驱动
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            
            // 建立连接
            connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
            System.out.println("========== Hive 高级特性示例 ==========\n");
            
            // 1. 分区表管理
            partitionedTableDemo();
            
            // 2. 分桶表
            bucketedTableDemo();
            
            // 3. 动态分区
            dynamicPartitionDemo();
            
            // 4. 复杂数据类型
            complexDataTypesDemo();
            
            // 5. 窗口函数
            windowFunctionsDemo();
            
            // 6. UDF 使用
            udfDemo();
            
            // 7. 事务表(ACID)
            transactionalTableDemo();
            
            // 8. 表优化
            tableOptimizationDemo();
            
            // 9. 视图和物化视图
            viewsDemo();
            
            // 10. 性能优化
            performanceOptimizationDemo();
            
            // 11. 查询优化
            queryOptimizationDemo();
            
            // 12. 高级查询
            advancedQueriesDemo();
            
            System.out.println("\n✓ 所有高级特性演示完成!");
            
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeConnection();
        }
    }
    
    /**
     * 1. 分区表管理
     */
    private static void partitionedTableDemo() throws SQLException {
        System.out.println("========== 1. 分区表管理 ==========");
        
        Statement stmt = connection.createStatement();
        
        // 创建分区表
        String createTable = "CREATE TABLE IF NOT EXISTS sales_partitioned (\n" +
            "    sale_id INT,\n" +
            "    product_name STRING,\n" +
            "    amount DOUBLE\n" +
            ") PARTITIONED BY (year INT, month INT)\n" +
            "STORED AS ORC\n" +
            "TBLPROPERTIES ('orc.compress'='SNAPPY')";
        
        stmt.execute(createTable);
        System.out.println("✓ 创建分区表: sales_partitioned");
        
        // 添加分区
        stmt.execute("ALTER TABLE sales_partitioned ADD IF NOT EXISTS PARTITION (year=2024, month=1)");
        stmt.execute("ALTER TABLE sales_partitioned ADD IF NOT EXISTS PARTITION (year=2024, month=2)");
        System.out.println("✓ 添加分区: 2024-01, 2024-02");
        
        // 插入数据到分区
        stmt.execute("INSERT INTO sales_partitioned PARTITION (year=2024, month=1) " +
            "VALUES (1, 'Laptop', 1200.00), (2, 'Mouse', 25.50)");
        
        stmt.execute("INSERT INTO sales_partitioned PARTITION (year=2024, month=2) " +
            "VALUES (3, 'Keyboard', 75.00), (4, 'Monitor', 350.00)");
        
        System.out.println("✓ 插入数据到各分区");
        
        // 查询特定分区
        ResultSet rs = stmt.executeQuery(
            "SELECT * FROM sales_partitioned WHERE year=2024 AND month=1");
        
        System.out.println("✓ 查询 2024-01 分区数据:");
        while (rs.next()) {
            System.out.printf("  - ID: %d, Product: %s, Amount: $%.2f%n",
                rs.getInt("sale_id"),
                rs.getString("product_name"),
                rs.getDouble("amount"));
        }
        rs.close();
        
        // 查看分区信息
        rs = stmt.executeQuery("SHOW PARTITIONS sales_partitioned");
        System.out.println("✓ 分区列表:");
        while (rs.next()) {
            System.out.println("  - " + rs.getString(1));
        }
        rs.close();
        
        // 删除分区
        stmt.execute("ALTER TABLE sales_partitioned DROP IF EXISTS PARTITION (year=2024, month=2)");
        System.out.println("✓ 删除分区: 2024-02");
        
        stmt.close();
        System.out.println();
    }
    
    /**
     * 2. 分桶表(Bucketing)
     */
    private static void bucketedTableDemo() throws SQLException {
        System.out.println("========== 2. 分桶表 ==========");
        
        Statement stmt = connection.createStatement();
        
        // 创建分桶表
        String createTable = "CREATE TABLE IF NOT EXISTS users_bucketed (\n" +
            "    user_id INT,\n" +
            "    username STRING,\n" +
            "    email STRING,\n" +
            "    age INT\n" +
            ") CLUSTERED BY (user_id) INTO 4 BUCKETS\n" +
            "STORED AS ORC";
        
        stmt.execute(createTable);
        System.out.println("✓ 创建分桶表: users_bucketed (4个桶)");
        
        // 启用分桶
        stmt.execute("SET hive.enforce.bucketing = true");
        
        // 插入数据
        stmt.execute("INSERT INTO users_bucketed VALUES " +
            "(1, 'alice', 'alice@example.com', 28), " +
            "(2, 'bob', 'bob@example.com', 32), " +
            "(3, 'charlie', 'charlie@example.com', 25), " +
            "(4, 'david', 'david@example.com', 30), " +
            "(5, 'eve', 'eve@example.com', 27)");
        
        System.out.println("✓ 插入数据到分桶表");
        
        // 分桶表的优势 - 高效采样
        ResultSet rs = stmt.executeQuery(
            "SELECT * FROM users_bucketed TABLESAMPLE(BUCKET 1 OUT OF 4) LIMIT 5");
        
        System.out.println("✓ 采样查询(1/4桶):");
        while (rs.next()) {
            System.out.printf("  - User: %s (ID: %d)%n",
                rs.getString("username"),
                rs.getInt("user_id"));
        }
        rs.close();
        
        // 分桶表的优势 - 高效JOIN
        System.out.println("✓ 分桶表适用于大表JOIN优化 (Map-side Join)");
        
        stmt.close();
        System.out.println();
    }
    
    /**
     * 3. 动态分区
     */
    private static void dynamicPartitionDemo() throws SQLException {
        System.out.println("========== 3. 动态分区 ==========");
        
        Statement stmt = connection.createStatement();
        
        // 创建源表
        stmt.execute("CREATE TABLE IF NOT EXISTS sales_source (\n" +
            "    sale_id INT, product STRING, amount DOUBLE, year INT, month INT)");
        
        stmt.execute("INSERT INTO sales_source VALUES " +
            "(1, 'ProductA', 100, 2024, 1), " +
            "(2, 'ProductB', 200, 2024, 2), " +
            "(3, 'ProductC', 150, 2024, 1), " +
            "(4, 'ProductD', 300, 2024, 3)");
        
        // 创建目标分区表
        stmt.execute("CREATE TABLE IF NOT EXISTS sales_dynamic (\n" +
            "    sale_id INT, product STRING, amount DOUBLE) " +
            "PARTITIONED BY (year INT, month INT)");
        
        System.out.println("✓ 创建源表和目标分区表");
        
        // 启用动态分区
        stmt.execute("SET hive.exec.dynamic.partition = true");
        stmt.execute("SET hive.exec.dynamic.partition.mode = nonstrict");
        stmt.execute("SET hive.exec.max.dynamic.partitions = 1000");
        
        System.out.println("✓ 启用动态分区");
        
        // 动态分区插入
        stmt.execute("INSERT OVERWRITE TABLE sales_dynamic PARTITION (year, month) " +
            "SELECT sale_id, product, amount, year, month FROM sales_source");
        
        System.out.println("✓ 动态分区插入完成");
        
        // 查看自动创建的分区
        ResultSet rs = stmt.executeQuery("SHOW PARTITIONS sales_dynamic");
        System.out.println("✓ 自动创建的分区:");
        while (rs.next()) {
            System.out.println("  - " + rs.getString(1));
        }
        rs.close();
        
        stmt.close();
        System.out.println();
    }
    
    /**
     * 4. 复杂数据类型
     */
    private static void complexDataTypesDemo() throws SQLException {
        System.out.println("========== 4. 复杂数据类型 ==========");
        
        Statement stmt = connection.createStatement();
        
        // 创建包含复杂类型的表
        String createTable = "CREATE TABLE IF NOT EXISTS employees_complex (\n" +
            "    emp_id INT,\n" +
            "    name STRING,\n" +
            "    skills ARRAY<STRING>,\n" +
            "    salary_history MAP<INT, DOUBLE>,\n" +
            "    address STRUCT<street:STRING, city:STRING, zipcode:STRING>\n" +
            ") STORED AS ORC";
        
        stmt.execute(createTable);
        System.out.println("✓ 创建包含复杂类型的表");
        
        // 插入复杂数据
        stmt.execute("INSERT INTO employees_complex VALUES (\n" +
            "    1, 'Alice',\n" +
            "    array('Java', 'Python', 'Scala'),\n" +
            "    map(2022, 80000.0, 2023, 90000.0, 2024, 100000.0),\n" +
            "    named_struct('street', '123 Main St', 'city', 'Beijing', 'zipcode', '100000')\n" +
            ")");
        
        stmt.execute("INSERT INTO employees_complex VALUES (\n" +
            "    2, 'Bob',\n" +
            "    array('JavaScript', 'React', 'Node.js'),\n" +
            "    map(2022, 75000.0, 2023, 85000.0),\n" +
            "    named_struct('street', '456 Park Ave', 'city', 'Shanghai', 'zipcode', '200000')\n" +
            ")");
        
        System.out.println("✓ 插入复杂类型数据");
        
        // 查询 Array 类型
        ResultSet rs = stmt.executeQuery(
            "SELECT name, skills[0] as primary_skill, size(skills) as skill_count " +
            "FROM employees_complex");
        
        System.out.println("✓ Array 查询:");
        while (rs.next()) {
            System.out.printf("  - %s: 主技能=%s, 技能数=%d%n",
                rs.getString("name"),
                rs.getString("primary_skill"),
                rs.getInt("skill_count"));
        }
        rs.close();
        
        // 查询 Map 类型
        rs = stmt.executeQuery(
            "SELECT name, salary_history[2024] as current_salary, " +
            "size(salary_history) as years_worked FROM employees_complex");
        
        System.out.println("✓ Map 查询:");
        while (rs.next()) {
            System.out.printf("  - %s: 当前薪资=$%.2f, 工作年限=%d%n",
                rs.getString("name"),
                rs.getDouble("current_salary"),
                rs.getInt("years_worked"));
        }
        rs.close();
        
        // 查询 Struct 类型
        rs = stmt.executeQuery(
            "SELECT name, address.city, address.zipcode FROM employees_complex");
        
        System.out.println("✓ Struct 查询:");
        while (rs.next()) {
            System.out.printf("  - %s: %s (%s)%n",
                rs.getString("name"),
                rs.getString("city"),
                rs.getString("zipcode"));
        }
        rs.close();
        
        // Array 展开
        rs = stmt.executeQuery(
            "SELECT name, skill FROM employees_complex " +
            "LATERAL VIEW explode(skills) skillTable AS skill");
        
        System.out.println("✓ Array 展开(LATERAL VIEW):");
        while (rs.next()) {
            System.out.printf("  - %s: %s%n",
                rs.getString("name"),
                rs.getString("skill"));
        }
        rs.close();
        
        stmt.close();
        System.out.println();
    }
    
    /**
     * 5. 窗口函数
     */
    private static void windowFunctionsDemo() throws SQLException {
        System.out.println("========== 5. 窗口函数 ==========");
        
        Statement stmt = connection.createStatement();
        
        // 创建测试表
        stmt.execute("CREATE TABLE IF NOT EXISTS employee_salaries (\n" +
            "    emp_id INT, name STRING, department STRING, salary DOUBLE)");
        
        stmt.execute("INSERT INTO employee_salaries VALUES " +
            "(1, 'Alice', 'IT', 80000), " +
            "(2, 'Bob', 'IT', 75000), " +
            "(3, 'Charlie', 'IT', 90000), " +
            "(4, 'David', 'HR', 60000), " +
            "(5, 'Eve', 'HR', 65000), " +
            "(6, 'Frank', 'Sales', 70000), " +
            "(7, 'Grace', 'Sales', 85000)");
        
        System.out.println("✓ 创建员工薪资表");
        
        // ROW_NUMBER
        ResultSet rs = stmt.executeQuery(
            "SELECT name, department, salary, " +
            "ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank " +
            "FROM employee_salaries");
        
        System.out.println("✓ ROW_NUMBER - 部门内薪资排名:");
        while (rs.next()) {
            System.out.printf("  - %s (%s): $%.2f, 排名=%d%n",
                rs.getString("name"),
                rs.getString("department"),
                rs.getDouble("salary"),
                rs.getInt("rank"));
        }
        rs.close();
        
        // RANK 和 DENSE_RANK
        rs = stmt.executeQuery(
            "SELECT name, department, salary, " +
            "RANK() OVER (ORDER BY salary DESC) as rank, " +
            "DENSE_RANK() OVER (ORDER BY salary DESC) as dense_rank " +
            "FROM employee_salaries");
        
        System.out.println("✓ RANK vs DENSE_RANK:");
        while (rs.next()) {
            System.out.printf("  - %s: $%.2f, RANK=%d, DENSE_RANK=%d%n",
                rs.getString("name"),
                rs.getDouble("salary"),
                rs.getInt("rank"),
                rs.getInt("dense_rank"));
        }
        rs.close();
        
        // 聚合窗口函数
        rs = stmt.executeQuery(
            "SELECT name, department, salary, " +
            "AVG(salary) OVER (PARTITION BY department) as dept_avg, " +
            "SUM(salary) OVER (PARTITION BY department) as dept_total, " +
            "COUNT(*) OVER (PARTITION BY department) as dept_count " +
            "FROM employee_salaries");
        
        System.out.println("✓ 聚合窗口函数 - 部门统计:");
        while (rs.next()) {
            System.out.printf("  - %s (%s): 薪资=$%.2f, 部门平均=$%.2f, 部门总计=$%.2f%n",
                rs.getString("name"),
                rs.getString("department"),
                rs.getDouble("salary"),
                rs.getDouble("dept_avg"),
                rs.getDouble("dept_total"));
        }
        rs.close();
        
        // LEAD 和 LAG
        rs = stmt.executeQuery(
            "SELECT name, salary, " +
            "LAG(salary, 1) OVER (ORDER BY salary) as prev_salary, " +
            "LEAD(salary, 1) OVER (ORDER BY salary) as next_salary " +
            "FROM employee_salaries");
        
        System.out.println("✓ LEAD/LAG - 前后薪资:");
        while (rs.next()) {
            System.out.printf("  - %s: 当前=$%.2f, 上一位=$%.2f, 下一位=$%.2f%n",
                rs.getString("name"),
                rs.getDouble("salary"),
                rs.getDouble("prev_salary"),
                rs.getDouble("next_salary"));
        }
        rs.close();
        
        stmt.close();
        System.out.println();
    }
    
    /**
     * 6. UDF 使用示例
     */
    private static void udfDemo() throws SQLException {
        System.out.println("========== 6. UDF 使用 ==========");
        
        Statement stmt = connection.createStatement();
        
        // 内置函数示例
        System.out.println("✓ Hive 内置函数:");
        
        // 字符串函数
        ResultSet rs = stmt.executeQuery(
            "SELECT " +
            "upper('hello') as upper_case, " +
            "concat('Hello', ' ', 'World') as concatenated, " +
            "substr('Hive SQL', 1, 4) as substring, " +
            "length('Hive') as str_length");
        
        if (rs.next()) {
            System.out.println("  字符串函数:");
            System.out.println("    UPPER: " + rs.getString("upper_case"));
            System.out.println("    CONCAT: " + rs.getString("concatenated"));
            System.out.println("    SUBSTR: " + rs.getString("substring"));
            System.out.println("    LENGTH: " + rs.getInt("str_length"));
        }
        rs.close();
        
        // 数学函数
        rs = stmt.executeQuery(
            "SELECT " +
            "round(3.14159, 2) as rounded, " +
            "ceil(3.2) as ceiling, " +
            "floor(3.8) as floor_val, " +
            "abs(-10) as absolute");
        
        if (rs.next()) {
            System.out.println("  数学函数:");
            System.out.println("    ROUND: " + rs.getDouble("rounded"));
            System.out.println("    CEIL: " + rs.getInt("ceiling"));
            System.out.println("    FLOOR: " + rs.getInt("floor_val"));
            System.out.println("    ABS: " + rs.getInt("absolute"));
        }
        rs.close();
        
        // 日期函数
        rs = stmt.executeQuery(
            "SELECT " +
            "current_date() as today, " +
            "current_timestamp() as now, " +
            "year(current_date()) as year, " +
            "month(current_date()) as month, " +
            "day(current_date()) as day");
        
        if (rs.next()) {
            System.out.println("  日期函数:");
            System.out.println("    TODAY: " + rs.getDate("today"));
            System.out.println("    NOW: " + rs.getTimestamp("now"));
            System.out.println("    YEAR: " + rs.getInt("year"));
        }
        rs.close();
        
        // 条件函数
        rs = stmt.executeQuery(
            "SELECT " +
            "if(1 > 0, 'true', 'false') as if_result, " +
            "coalesce(null, null, 'default') as coalesce_result, " +
            "nvl(null, 'default') as nvl_result");
        
        if (rs.next()) {
            System.out.println("  条件函数:");
            System.out.println("    IF: " + rs.getString("if_result"));
            System.out.println("    COALESCE: " + rs.getString("coalesce_result"));
            System.out.println("    NVL: " + rs.getString("nvl_result"));
        }
        rs.close();
        
        System.out.println("\n✓ 自定义 UDF 需要:");
        System.out.println("  1. 创建 Java 类继承 org.apache.hadoop.hive.ql.exec.UDF");
        System.out.println("  2. 实现 evaluate() 方法");
        System.out.println("  3. 打包成 JAR 并添加到 Hive");
        System.out.println("  4. 使用 CREATE FUNCTION 注册");
        
        stmt.close();
        System.out.println();
    }
    
    /**
     * 7. 事务表(ACID)
     */
    private static void transactionalTableDemo() throws SQLException {
        System.out.println("========== 7. 事务表(ACID) ==========");
        
        Statement stmt = connection.createStatement();
        
        // 创建事务表
        String createTable = "CREATE TABLE IF NOT EXISTS accounts_transactional (\n" +
            "    account_id INT,\n" +
            "    account_name STRING,\n" +
            "    balance DOUBLE\n" +
            ") STORED AS ORC\n" +
            "TBLPROPERTIES (\n" +
            "    'transactional'='true',\n" +
            "    'orc.compress'='SNAPPY'\n" +
            ")";
        
        stmt.execute(createTable);
        System.out.println("✓ 创建事务表: accounts_transactional");
        
        // 插入数据
        stmt.execute("INSERT INTO accounts_transactional VALUES " +
            "(1, 'Alice', 1000.00), " +
            "(2, 'Bob', 1500.00), " +
            "(3, 'Charlie', 2000.00)");
        
        System.out.println("✓ 插入初始数据");
        
        // UPDATE 操作
        stmt.execute("UPDATE accounts_transactional SET balance = balance + 500 " +
            "WHERE account_id = 1");
        System.out.println("✓ UPDATE: Alice 账户增加 $500");
        
        // DELETE 操作
        stmt.execute("DELETE FROM accounts_transactional WHERE account_id = 3");
        System.out.println("✓ DELETE: 删除 Charlie 账户");
        
        // 查询结果
        ResultSet rs = stmt.executeQuery(
            "SELECT * FROM accounts_transactional ORDER BY account_id");
        
        System.out.println("✓ 当前账户状态:");
        while (rs.next()) {
            System.out.printf("  - %s (ID: %d): $%.2f%n",
                rs.getString("account_name"),
                rs.getInt("account_id"),
                rs.getDouble("balance"));
        }
        rs.close();
        
        System.out.println("\n✓ ACID 特性:");
        System.out.println("  - Atomicity: 原子性");
        System.out.println("  - Consistency: 一致性");
        System.out.println("  - Isolation: 隔离性");
        System.out.println("  - Durability: 持久性");
        
        stmt.close();
        System.out.println();
    }
    
    /**
     * 8. 表优化
     */
    private static void tableOptimizationDemo() throws SQLException {
        System.out.println("========== 8. 表优化 ==========");
        
        Statement stmt = connection.createStatement();
        
        // 创建需要压缩的事务表
        stmt.execute("CREATE TABLE IF NOT EXISTS logs_optimize (\n" +
            "    log_id INT, message STRING, timestamp BIGINT) " +
            "STORED AS ORC TBLPROPERTIES ('transactional'='true')");
        
        // 插入数据
        for (int i = 1; i <= 100; i++) {
            stmt.execute("INSERT INTO logs_optimize VALUES " +
                "(" + i + ", 'Log message " + i + "', " + System.currentTimeMillis() + ")");
        }
        System.out.println("✓ 插入100条日志记录");
        
        // 查看文件数量(会产生很多小文件)
        System.out.println("✓ 产生了大量小文件(Delta 文件)");
        
        // Major Compaction - 合并所有文件
        System.out.println("✓ 执行 Major Compaction:");
        stmt.execute("ALTER TABLE logs_optimize COMPACT 'major'");
        System.out.println("  - 合并所有 Delta 文件到 Base 文件");
        
        // Minor Compaction - 合并 Delta 文件
        System.out.println("✓ 执行 Minor Compaction:");
        stmt.execute("ALTER TABLE logs_optimize COMPACT 'minor'");
        System.out.println("  - 合并小的 Delta 文件");
        
        // 分析表统计信息
        stmt.execute("ANALYZE TABLE logs_optimize COMPUTE STATISTICS");
        System.out.println("✓ 分析表统计信息");
        
        stmt.execute("ANALYZE TABLE logs_optimize COMPUTE STATISTICS FOR COLUMNS");
        System.out.println("✓ 分析列统计信息");
        
        // 查看统计信息
        ResultSet rs = stmt.executeQuery("DESCRIBE FORMATTED logs_optimize");
        System.out.println("✓ 表详细信息:");
        while (rs.next()) {
            String colName = rs.getString(1);
            if (colName != null && colName.contains("numRows")) {
                System.out.println("  - " + colName + ": " + rs.getString(2));
            }
        }
        rs.close();
        
        stmt.close();
        System.out.println();
    }
    
    /**
     * 9. 视图和物化视图
     */
    private static void viewsDemo() throws SQLException {
        System.out.println("========== 9. 视图和物化视图 ==========");
        
        Statement stmt = connection.createStatement();
        
        // 创建普通视图
        stmt.execute("CREATE VIEW IF NOT EXISTS high_salary_employees AS " +
            "SELECT name, department, salary FROM employee_salaries " +
            "WHERE salary > 75000");
        
        System.out.println("✓ 创建视图: high_salary_employees");
        
        // 查询视图
        ResultSet rs = stmt.executeQuery("SELECT * FROM high_salary_employees");
        System.out.println("✓ 查询视图结果:");
        while (rs.next()) {
            System.out.printf("  - %s (%s): $%.2f%n",
                rs.getString("name"),
                rs.getString("department"),
                rs.getDouble("salary"));
        }
        rs.close();
        
        // 创建物化视图(需要 Hive 3.0+)
        try {
            stmt.execute("CREATE MATERIALIZED VIEW IF NOT EXISTS dept_salary_stats AS " +
                "SELECT department, " +
                "COUNT(*) as emp_count, " +
                "AVG(salary) as avg_salary, " +
                "MAX(salary) as max_salary " +
                "FROM employee_salaries GROUP BY department");
            
            System.out.println("✓ 创建物化视图: dept_salary_stats");
            
            // 查询物化视图
            rs = stmt.executeQuery("SELECT * FROM dept_salary_stats");
            System.out.println("✓ 物化视图结果:");
            while (rs.next()) {
                System.out.printf("  - %s: 员工数=%d, 平均薪资=$%.2f%n",
                    rs.getString("department"),
                    rs.getInt("emp_count"),
                    rs.getDouble("avg_salary"));
            }
            rs.close();
            
            // 刷新物化视图
            stmt.execute("ALTER MATERIALIZED VIEW dept_salary_stats REBUILD");
            System.out.println("✓ 刷新物化视图");
            
        } catch (SQLException e) {
            System.out.println("注意: 物化视图需要 Hive 3.0+ 版本");
        }
        
        stmt.close();
        System.out.println();
    }
    
    /**
     * 10. 性能优化
     */
    private static void performanceOptimizationDemo() throws SQLException {
        System.out.println("========== 10. 性能优化 ==========");
        
        Statement stmt = connection.createStatement();
        
        System.out.println("✓ 性能优化配置:");
        
        // Map-side JOIN
        stmt.execute("SET hive.auto.convert.join = true");
        stmt.execute("SET hive.mapjoin.smalltable.filesize = 25000000");
        System.out.println("  ✓ 启用 Map-side JOIN (小表 < 25MB)");
        
        // 并行执行
        stmt.execute("SET hive.exec.parallel = true");
        stmt.execute("SET hive.exec.parallel.thread.number = 8");
        System.out.println("  ✓ 启用并行执行 (8个线程)");
        
        // 推测执行
        stmt.execute("SET hive.mapred.reduce.tasks.speculative.execution = true");
        System.out.println("  ✓ 启用推测执行");
        
        // 向量化查询
        stmt.execute("SET hive.vectorized.execution.enabled = true");
        stmt.execute("SET hive.vectorized.execution.reduce.enabled = true");
        System.out.println("  ✓ 启用向量化查询");
        
        // CBO(基于成本的优化器)
        stmt.execute("SET hive.cbo.enable = true");
        stmt.execute("SET hive.compute.query.using.stats = true");
        stmt.execute("SET hive.stats.fetch.column.stats = true");
        System.out.println("  ✓ 启用 CBO 优化器");
        
        // 动态分区裁剪
        stmt.execute("SET hive.optimize.ppd = true");
        System.out.println("  ✓ 启用谓词下推");
        
        // 合并小文件
        stmt.execute("SET hive.merge.mapfiles = true");
        stmt.execute("SET hive.merge.mapredfiles = true");
        stmt.execute("SET hive.merge.size.per.task = 256000000");
        stmt.execute("SET hive.merge.smallfiles.avgsize = 16000000");
        System.out.println("  ✓ 启用小文件合并");
        
        // 压缩
        stmt.execute("SET hive.exec.compress.output = true");
        stmt.execute("SET mapreduce.output.fileoutputformat.compress.codec = " +
            "org.apache.hadoop.io.compress.SnappyCodec");
        System.out.println("  ✓ 启用输出压缩 (Snappy)");
        
        stmt.close();
        System.out.println();
    }
    
    /**
     * 11. 查询优化
     */
    private static void queryOptimizationDemo() throws SQLException {
        System.out.println("========== 11. 查询优化 ==========");
        
        Statement stmt = connection.createStatement();
        
        System.out.println("✓ 查询优化技巧:");
        
        // 使用 EXPLAIN 分析查询计划
        System.out.println("\n1. 使用 EXPLAIN 分析查询:");
        ResultSet rs = stmt.executeQuery(
            "EXPLAIN SELECT department, COUNT(*) FROM employee_salaries GROUP BY department");
        
        System.out.println("  查询计划:");
        while (rs.next()) {
            System.out.println("  " + rs.getString(1));
        }
        rs.close();
        
        // 分区裁剪
        System.out.println("\n2. 分区裁剪:");
        System.out.println("  ✓ WHERE 子句中使用分区列");
        System.out.println("    SELECT * FROM sales_partitioned WHERE year=2024 AND month=1");
        
        // 列裁剪
        System.out.println("\n3. 列裁剪:");
        System.out.println("  ✓ 只查询需要的列,避免 SELECT *");
        System.out.println("    SELECT name, salary FROM employee_salaries");
        
        // JOIN 优化
        System.out.println("\n4. JOIN 优化:");
        System.out.println("  ✓ 小表放在 JOIN 左侧(Map-side JOIN)");
        System.out.println("  ✓ 使用分桶表进行 Bucket Map Join");
        System.out.println("  ✓ 使用 STREAMTABLE hint");
        
        // 去重优化
        System.out.println("\n5. 去重优化:");
        System.out.println("  ✓ 使用 GROUP BY 代替 DISTINCT");
        System.out.println("    SELECT department, COUNT(*) FROM employee_salaries GROUP BY department");
        
        // 数据倾斜处理
        System.out.println("\n6. 数据倾斜处理:");
        stmt.execute("SET hive.groupby.skewindata = true");
        System.out.println("  ✓ 启用数据倾斜处理");
        
        stmt.close();
        System.out.println();
    }
    
    /**
     * 12. 高级查询
     */
    private static void advancedQueriesDemo() throws SQLException {
        System.out.println("========== 12. 高级查询 ==========");
        
        Statement stmt = connection.createStatement();
        
        // CTE (Common Table Expression)
        System.out.println("✓ CTE (公共表表达式):");
        ResultSet rs = stmt.executeQuery(
            "WITH dept_stats AS (\n" +
            "    SELECT department, AVG(salary) as avg_salary\n" +
            "    FROM employee_salaries\n" +
            "    GROUP BY department\n" +
            ")\n" +
            "SELECT e.name, e.department, e.salary, d.avg_salary\n" +
            "FROM employee_salaries e\n" +
            "JOIN dept_stats d ON e.department = d.department\n" +
            "WHERE e.salary > d.avg_salary");
        
        while (rs.next()) {
            System.out.printf("  - %s (%s): 薪资=$%.2f, 部门平均=$%.2f%n",
                rs.getString("name"),
                rs.getString("department"),
                rs.getDouble("salary"),
                rs.getDouble("avg_salary"));
        }
        rs.close();
        
        // UNION / UNION ALL
        System.out.println("\n✓ UNION 查询:");
        rs = stmt.executeQuery(
            "SELECT 'High' as category, name, salary FROM employee_salaries WHERE salary > 80000\n" +
            "UNION ALL\n" +
            "SELECT 'Low' as category, name, salary FROM employee_salaries WHERE salary <= 80000\n" +
            "ORDER BY salary DESC");
        
        int count = 0;
        while (rs.next() && count++ < 5) {
            System.out.printf("  - %s: %s ($%.2f)%n",
                rs.getString("category"),
                rs.getString("name"),
                rs.getDouble("salary"));
        }
        rs.close();
        
        // 子查询
        System.out.println("\n✓ 相关子查询:");
        rs = stmt.executeQuery(
            "SELECT name, department, salary\n" +
            "FROM employee_salaries e1\n" +
            "WHERE salary = (\n" +
            "    SELECT MAX(salary) FROM employee_salaries e2\n" +
            "    WHERE e1.department = e2.department\n" +
            ")");
        
        System.out.println("  各部门最高薪资员工:");
        while (rs.next()) {
            System.out.printf("  - %s (%s): $%.2f%n",
                rs.getString("name"),
                rs.getString("department"),
                rs.getDouble("salary"));
        }
        rs.close();
        
        stmt.close();
        System.out.println();
    }
    
    /**
     * 关闭连接
     */
    private static void closeConnection() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
                System.out.println("✓ Hive 连接已关闭");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
