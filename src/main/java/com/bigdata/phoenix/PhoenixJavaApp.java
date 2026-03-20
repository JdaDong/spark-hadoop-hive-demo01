package com.bigdata.phoenix;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;

/**
 * Apache Phoenix - HBase SQL 访问层
 * 
 * Phoenix 是构建在 HBase 之上的 SQL 层，提供：
 * 1. 标准 SQL 查询（SELECT、JOIN、GROUP BY 等）
 * 2. 二级索引支持
 * 3. 视图和序列
 * 4. JDBC 驱动，易于集成
 * 5. 事务支持
 * 
 * 优势：
 * - 将 HBase 的 NoSQL 转换为 SQL
 * - 查询性能优于直接使用 HBase API
 * - 自动创建二级索引
 * - 支持复杂查询和聚合
 */
public class PhoenixJavaApp {
    
    private static final Logger LOG = LoggerFactory.getLogger(PhoenixJavaApp.class);
    
    // Phoenix JDBC URL
    private static final String PHOENIX_URL = "jdbc:phoenix:localhost:2181";
    
    public static void main(String[] args) {
        try {
            // 加载 Phoenix JDBC 驱动
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            
            LOG.info("=== Apache Phoenix 示例开始 ===");
            
            try (Connection conn = DriverManager.getConnection(PHOENIX_URL)) {
                // 1. 创建表
                createTable(conn);
                
                // 2. 插入数据
                insertData(conn);
                
                // 3. 查询数据
                queryData(conn);
                
                // 4. 更新数据
                updateData(conn);
                
                // 5. 使用二级索引
                createAndUseIndex(conn);
                
                // 6. 复杂查询
                complexQueries(conn);
                
                // 7. 聚合查询
                aggregateQueries(conn);
                
                // 8. JOIN 查询
                joinQueries(conn);
                
                // 9. 视图操作
                createAndUseView(conn);
                
                // 10. 序列操作
                sequenceOperations(conn);
                
                // 11. 事务操作
                transactionOperations(conn);
                
                // 12. 性能优化
                performanceOptimization(conn);
            }
            
            LOG.info("=== Apache Phoenix 示例完成 ===");
            
        } catch (Exception e) {
            LOG.error("Phoenix error", e);
        }
    }
    
    /**
     * 1. 创建 Phoenix 表
     * Phoenix 会自动在 HBase 中创建对应的表
     */
    private static void createTable(Connection conn) throws SQLException {
        LOG.info("\n=== 1. 创建表 ===");
        
        String createTableSQL = """
            CREATE TABLE IF NOT EXISTS users (
                user_id VARCHAR PRIMARY KEY,
                name VARCHAR,
                email VARCHAR,
                age INTEGER,
                city VARCHAR,
                salary DOUBLE,
                create_time TIMESTAMP
            )
            """;
        
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(createTableSQL);
            LOG.info("Table 'users' created");
            
            // 创建订单表用于 JOIN 演示
            String createOrderTableSQL = """
                CREATE TABLE IF NOT EXISTS orders (
                    order_id VARCHAR PRIMARY KEY,
                    user_id VARCHAR,
                    product_name VARCHAR,
                    amount DOUBLE,
                    order_date TIMESTAMP
                )
                """;
            
            stmt.execute(createOrderTableSQL);
            LOG.info("Table 'orders' created");
        }
    }
    
    /**
     * 2. 插入数据
     * Phoenix 支持标准 SQL INSERT 和批量 UPSERT
     */
    private static void insertData(Connection conn) throws SQLException {
        LOG.info("\n=== 2. 插入数据 ===");
        
        // UPSERT = UPDATE + INSERT（如果不存在则插入，存在则更新）
        String upsertSQL = """
            UPSERT INTO users (user_id, name, email, age, city, salary, create_time)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """;
        
        try (PreparedStatement pstmt = conn.prepareStatement(upsertSQL)) {
            // 批量插入
            Object[][] data = {
                {"user001", "张三", "zhangsan@example.com", 28, "北京", 15000.0, new Timestamp(System.currentTimeMillis())},
                {"user002", "李四", "lisi@example.com", 32, "上海", 18000.0, new Timestamp(System.currentTimeMillis())},
                {"user003", "王五", "wangwu@example.com", 25, "深圳", 16000.0, new Timestamp(System.currentTimeMillis())},
                {"user004", "赵六", "zhaoliu@example.com", 35, "北京", 20000.0, new Timestamp(System.currentTimeMillis())},
                {"user005", "孙七", "sunqi@example.com", 29, "上海", 17000.0, new Timestamp(System.currentTimeMillis())}
            };
            
            for (Object[] row : data) {
                pstmt.setString(1, (String) row[0]);
                pstmt.setString(2, (String) row[1]);
                pstmt.setString(3, (String) row[2]);
                pstmt.setInt(4, (Integer) row[3]);
                pstmt.setString(5, (String) row[4]);
                pstmt.setDouble(6, (Double) row[5]);
                pstmt.setTimestamp(7, (Timestamp) row[6]);
                pstmt.addBatch();
            }
            
            pstmt.executeBatch();
            conn.commit();
            LOG.info("Inserted {} users", data.length);
        }
        
        // 插入订单数据
        String orderSQL = "UPSERT INTO orders VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(orderSQL)) {
            Object[][] orders = {
                {"order001", "user001", "Laptop", 5999.0, new Timestamp(System.currentTimeMillis())},
                {"order002", "user001", "Mouse", 99.0, new Timestamp(System.currentTimeMillis())},
                {"order003", "user002", "Keyboard", 299.0, new Timestamp(System.currentTimeMillis())},
                {"order004", "user003", "Monitor", 1999.0, new Timestamp(System.currentTimeMillis())}
            };
            
            for (Object[] row : orders) {
                for (int i = 0; i < row.length; i++) {
                    if (row[i] instanceof String) pstmt.setString(i + 1, (String) row[i]);
                    else if (row[i] instanceof Double) pstmt.setDouble(i + 1, (Double) row[i]);
                    else if (row[i] instanceof Timestamp) pstmt.setTimestamp(i + 1, (Timestamp) row[i]);
                }
                pstmt.addBatch();
            }
            
            pstmt.executeBatch();
            conn.commit();
            LOG.info("Inserted orders");
        }
    }
    
    /**
     * 3. 查询数据
     */
    private static void queryData(Connection conn) throws SQLException {
        LOG.info("\n=== 3. 查询数据 ===");
        
        String query = "SELECT * FROM users WHERE city = ?";
        
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, "北京");
            
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    LOG.info("User: {} | Name: {} | Age: {} | City: {} | Salary: {}",
                            rs.getString("user_id"),
                            rs.getString("name"),
                            rs.getInt("age"),
                            rs.getString("city"),
                            rs.getDouble("salary"));
                }
            }
        }
    }
    
    /**
     * 4. 更新数据
     */
    private static void updateData(Connection conn) throws SQLException {
        LOG.info("\n=== 4. 更新数据 ===");
        
        String updateSQL = "UPSERT INTO users (user_id, salary) VALUES (?, ?)";
        
        try (PreparedStatement pstmt = conn.prepareStatement(updateSQL)) {
            pstmt.setString(1, "user001");
            pstmt.setDouble(2, 16000.0);
            pstmt.executeUpdate();
            conn.commit();
            LOG.info("User salary updated");
        }
    }
    
    /**
     * 5. 创建和使用二级索引
     * Phoenix 的二级索引非常强大
     */
    private static void createAndUseIndex(Connection conn) throws SQLException {
        LOG.info("\n=== 5. 二级索引 ===");
        
        try (Statement stmt = conn.createStatement()) {
            // 创建覆盖索引（包含查询所需的所有列）
            String createIndexSQL = """
                CREATE INDEX IF NOT EXISTS idx_users_city 
                ON users (city) INCLUDE (name, age, salary)
                """;
            
            stmt.execute(createIndexSQL);
            LOG.info("Index 'idx_users_city' created");
            
            // 创建全局索引
            String globalIndexSQL = """
                CREATE INDEX IF NOT EXISTS idx_users_email 
                ON users (email)
                """;
            
            stmt.execute(globalIndexSQL);
            LOG.info("Global index 'idx_users_email' created");
        }
        
        // 使用索引查询
        String indexQuery = "SELECT name, age, salary FROM users WHERE city = '上海'";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(indexQuery)) {
            
            LOG.info("Query using index:");
            while (rs.next()) {
                LOG.info("  {} | Age: {} | Salary: {}", 
                        rs.getString("name"), 
                        rs.getInt("age"), 
                        rs.getDouble("salary"));
            }
        }
    }
    
    /**
     * 6. 复杂查询
     */
    private static void complexQueries(Connection conn) throws SQLException {
        LOG.info("\n=== 6. 复杂查询 ===");
        
        // WHERE 条件组合
        String query1 = """
            SELECT * FROM users 
            WHERE city IN ('北京', '上海') 
            AND age > 30 
            ORDER BY salary DESC
            """;
        
        LOG.info("Query: Users in Beijing/Shanghai, age > 30, ordered by salary:");
        executeQuery(conn, query1);
        
        // LIKE 查询
        String query2 = "SELECT * FROM users WHERE email LIKE '%@example.com'";
        LOG.info("\nQuery: Users with example.com email:");
        executeQuery(conn, query2);
        
        // LIMIT 查询
        String query3 = "SELECT * FROM users ORDER BY salary DESC LIMIT 3";
        LOG.info("\nTop 3 highest salary users:");
        executeQuery(conn, query3);
    }
    
    /**
     * 7. 聚合查询
     */
    private static void aggregateQueries(Connection conn) throws SQLException {
        LOG.info("\n=== 7. 聚合查询 ===");
        
        // COUNT, AVG, MAX, MIN
        String aggregateSQL = """
            SELECT 
                city,
                COUNT(*) as user_count,
                AVG(age) as avg_age,
                AVG(salary) as avg_salary,
                MAX(salary) as max_salary,
                MIN(salary) as min_salary
            FROM users
            GROUP BY city
            HAVING COUNT(*) >= 1
            ORDER BY user_count DESC
            """;
        
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(aggregateSQL)) {
            
            LOG.info("Aggregation by city:");
            while (rs.next()) {
                LOG.info("City: {} | Users: {} | Avg Age: {:.1f} | Avg Salary: {:.2f} | Max: {:.2f}",
                        rs.getString("city"),
                        rs.getInt("user_count"),
                        rs.getDouble("avg_age"),
                        rs.getDouble("avg_salary"),
                        rs.getDouble("max_salary"));
            }
        }
    }
    
    /**
     * 8. JOIN 查询
     */
    private static void joinQueries(Connection conn) throws SQLException {
        LOG.info("\n=== 8. JOIN 查询 ===");
        
        String joinSQL = """
            SELECT 
                u.user_id, 
                u.name, 
                o.order_id, 
                o.product_name, 
                o.amount
            FROM users u
            INNER JOIN orders o ON u.user_id = o.user_id
            ORDER BY u.user_id
            """;
        
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(joinSQL)) {
            
            LOG.info("User orders:");
            while (rs.next()) {
                LOG.info("User: {} ({}) | Order: {} | Product: {} | Amount: {}",
                        rs.getString("name"),
                        rs.getString("user_id"),
                        rs.getString("order_id"),
                        rs.getString("product_name"),
                        rs.getDouble("amount"));
            }
        }
        
        // 聚合 JOIN
        String aggJoinSQL = """
            SELECT 
                u.user_id,
                u.name,
                COUNT(o.order_id) as order_count,
                SUM(o.amount) as total_amount
            FROM users u
            LEFT JOIN orders o ON u.user_id = o.user_id
            GROUP BY u.user_id, u.name
            ORDER BY total_amount DESC
            """;
        
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(aggJoinSQL)) {
            
            LOG.info("\nUser order summary:");
            while (rs.next()) {
                LOG.info("User: {} | Orders: {} | Total: {:.2f}",
                        rs.getString("name"),
                        rs.getInt("order_count"),
                        rs.getDouble("total_amount"));
            }
        }
    }
    
    /**
     * 9. 视图操作
     */
    private static void createAndUseView(Connection conn) throws SQLException {
        LOG.info("\n=== 9. 视图 ===");
        
        try (Statement stmt = conn.createStatement()) {
            // 创建视图
            String createViewSQL = """
                CREATE VIEW IF NOT EXISTS high_salary_users AS
                SELECT user_id, name, city, salary
                FROM users
                WHERE salary >= 17000
                """;
            
            stmt.execute(createViewSQL);
            LOG.info("View 'high_salary_users' created");
        }
        
        // 查询视图
        String viewQuery = "SELECT * FROM high_salary_users ORDER BY salary DESC";
        LOG.info("High salary users:");
        executeQuery(conn, viewQuery);
    }
    
    /**
     * 10. 序列操作（自增 ID）
     */
    private static void sequenceOperations(Connection conn) throws SQLException {
        LOG.info("\n=== 10. 序列 ===");
        
        try (Statement stmt = conn.createStatement()) {
            // 创建序列
            String createSeqSQL = "CREATE SEQUENCE IF NOT EXISTS user_seq START WITH 1000 INCREMENT BY 1";
            stmt.execute(createSeqSQL);
            LOG.info("Sequence 'user_seq' created");
            
            // 使用序列
            for (int i = 0; i < 3; i++) {
                ResultSet rs = stmt.executeQuery("SELECT NEXT VALUE FOR user_seq");
                if (rs.next()) {
                    LOG.info("Next sequence value: {}", rs.getLong(1));
                }
                rs.close();
            }
            
            // 获取当前值
            ResultSet rs = stmt.executeQuery("SELECT CURRENT VALUE FOR user_seq");
            if (rs.next()) {
                LOG.info("Current sequence value: {}", rs.getLong(1));
            }
            rs.close();
        }
    }
    
    /**
     * 11. 事务操作
     * Phoenix 4.7+ 支持事务
     */
    private static void transactionOperations(Connection conn) throws SQLException {
        LOG.info("\n=== 11. 事务 ===");
        
        // 创建事务表
        String createTxTableSQL = """
            CREATE TABLE IF NOT EXISTS account (
                account_id VARCHAR PRIMARY KEY,
                balance DOUBLE
            ) TRANSACTIONAL=true
            """;
        
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(createTxTableSQL);
            LOG.info("Transactional table 'account' created");
        }
        
        // 插入初始数据
        String upsertSQL = "UPSERT INTO account VALUES (?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(upsertSQL)) {
            pstmt.setString(1, "ACC001");
            pstmt.setDouble(2, 1000.0);
            pstmt.executeUpdate();
            
            pstmt.setString(1, "ACC002");
            pstmt.setDouble(2, 2000.0);
            pstmt.executeUpdate();
            
            conn.commit();
            LOG.info("Initial account data inserted");
        }
        
        // 事务示例：转账
        try {
            conn.setAutoCommit(false);
            
            // 扣款
            String debitSQL = "UPSERT INTO account (account_id, balance) " +
                            "SELECT account_id, balance - 500 FROM account WHERE account_id = 'ACC001'";
            
            // 加款
            String creditSQL = "UPSERT INTO account (account_id, balance) " +
                             "SELECT account_id, balance + 500 FROM account WHERE account_id = 'ACC002'";
            
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(debitSQL);
                stmt.executeUpdate(creditSQL);
                conn.commit();
                LOG.info("Transaction committed: Transfer 500 from ACC001 to ACC002");
            }
            
        } catch (SQLException e) {
            conn.rollback();
            LOG.error("Transaction rolled back", e);
        } finally {
            conn.setAutoCommit(true);
        }
    }
    
    /**
     * 12. 性能优化技巧
     */
    private static void performanceOptimization(Connection conn) throws SQLException {
        LOG.info("\n=== 12. 性能优化 ===");
        
        // 1. 使用批量操作
        LOG.info("1. 批量 UPSERT 性能最佳");
        
        // 2. 盐表（Salt Buckets）避免热点
        String saltedTableSQL = """
            CREATE TABLE IF NOT EXISTS salted_table (
                id VARCHAR PRIMARY KEY,
                data VARCHAR
            ) SALT_BUCKETS=10
            """;
        
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(saltedTableSQL);
            LOG.info("2. Salted table created with 10 buckets");
        }
        
        // 3. 预分区
        LOG.info("3. 预分区: CREATE TABLE ... SPLIT ON ('A','M','Z')");
        
        // 4. 列族数量
        LOG.info("4. 列族数量建议 1-2 个");
        
        // 5. 查询优化
        LOG.info("5. 使用 EXPLAIN 查看执行计划:");
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("EXPLAIN SELECT * FROM users WHERE city = '北京'")) {
            while (rs.next()) {
                LOG.info("   {}", rs.getString(1));
            }
        }
        
        // 6. 统计信息
        LOG.info("6. 更新统计信息: UPDATE STATISTICS users");
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("UPDATE STATISTICS users");
        }
    }
    
    /**
     * 辅助方法：执行查询并打印结果
     */
    private static void executeQuery(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();
            
            while (rs.next()) {
                StringBuilder sb = new StringBuilder("  ");
                for (int i = 1; i <= columnCount; i++) {
                    sb.append(meta.getColumnName(i))
                      .append(": ")
                      .append(rs.getString(i))
                      .append(" | ");
                }
                LOG.info(sb.toString());
            }
        }
    }
}
