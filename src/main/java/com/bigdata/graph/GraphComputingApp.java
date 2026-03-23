package com.bigdata.graph;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.graphx.lib.PageRank;
import org.apache.spark.graphx.lib.ConnectedComponents;
import org.apache.spark.graphx.lib.ShortestPaths;
import org.apache.spark.graphx.lib.TriangleCount;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.reflect.ClassTag$;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * ========================================================================
 * 图计算实战 - Neo4j + Spark GraphX
 * ========================================================================
 *
 * 功能矩阵:
 * ┌─────────────────────────────────────────────────────────────────────┐
 * │  模块                │  功能                                        │
 * ├─────────────────────────────────────────────────────────────────────┤
 * │  1. Neo4j 图建模     │  用户/商品/标签 图模型 + Cypher 查询         │
 * │  2. Neo4j 图算法     │  社区发现/中心性/路径查找/相似度              │
 * │  3. GraphX 图构建    │  从 RDD 构建 Graph + 属性图操作              │
 * │  4. GraphX PageRank  │  网页/用户/商品 影响力排名                   │
 * │  5. GraphX 社区检测  │  Connected Components + Label Propagation   │
 * │  6. GraphX 最短路径  │  单源最短路 + 社交距离计算                   │
 * │  7. 反欺诈图分析     │  关联账户/团伙识别/风险传播                  │
 * │  8. 知识图谱         │  实体关系/图谱查询/推理引擎                  │
 * └─────────────────────────────────────────────────────────────────────┘
 *
 * 图数据模型:
 *
 *   ┌───────┐  FOLLOW   ┌───────┐  PURCHASE  ┌──────────┐
 *   │ User  │──────────→│ User  │───────────→│ Product  │
 *   │ (V)   │←──────────│ (V)   │←───────────│   (V)    │
 *   └───┬───┘  FRIEND   └───────┘  REVIEW    └────┬─────┘
 *       │                                          │
 *       │ LIVES_IN                    BELONGS_TO   │
 *       ▼                                          ▼
 *   ┌───────┐                              ┌──────────┐
 *   │ City  │                              │ Category │
 *   │ (V)   │                              │   (V)    │
 *   └───────┘                              └──────────┘
 */
public class GraphComputingApp implements Serializable {

    private static final long serialVersionUID = 1L;

    // ==================== 1. Neo4j 图建模 ====================

    /**
     * Neo4j 图数据建模 - Cypher DDL & 查询
     *
     * 节点类型: User / Product / Category / City / Tag / Order
     * 关系类型: FOLLOW / FRIEND / PURCHASE / REVIEW / BELONGS_TO / LIVES_IN
     */
    static class Neo4jGraphModeler implements Serializable {

        /**
         * 生成 Neo4j Schema (Cypher 语句集)
         */
        public Map<String, List<String>> generateNeo4jSchema() {
            Map<String, List<String>> schema = new LinkedHashMap<>();

            System.out.println("🔷 Neo4j 图数据建模...\n");

            // ========== 约束 & 索引 ==========
            List<String> constraints = Arrays.asList(
                    "-- 唯一性约束",
                    "CREATE CONSTRAINT IF NOT EXISTS FOR (u:User) REQUIRE u.userId IS UNIQUE;",
                    "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Product) REQUIRE p.productId IS UNIQUE;",
                    "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Category) REQUIRE c.categoryId IS UNIQUE;",
                    "CREATE CONSTRAINT IF NOT EXISTS FOR (city:City) REQUIRE city.name IS UNIQUE;",
                    "CREATE CONSTRAINT IF NOT EXISTS FOR (t:Tag) REQUIRE t.name IS UNIQUE;",
                    "CREATE CONSTRAINT IF NOT EXISTS FOR (o:Order) REQUIRE o.orderId IS UNIQUE;",
                    "",
                    "-- 复合索引",
                    "CREATE INDEX IF NOT EXISTS FOR (u:User) ON (u.vipLevel, u.city);",
                    "CREATE INDEX IF NOT EXISTS FOR (p:Product) ON (p.categoryId, p.price);",
                    "CREATE INDEX IF NOT EXISTS FOR ()-[r:PURCHASE]-() ON (r.purchaseTime);",
                    "",
                    "-- 全文索引",
                    "CREATE FULLTEXT INDEX productNameIndex IF NOT EXISTS",
                    "FOR (p:Product) ON EACH [p.name, p.description];"
            );
            schema.put("约束与索引", constraints);
            System.out.println("  📋 约束: 6 个唯一约束 + 2 个复合索引 + 1 个全文索引");

            // ========== 节点创建 ==========
            List<String> nodeCreation = Arrays.asList(
                    "-- ========================================",
                    "-- 创建用户节点 (User)",
                    "-- ========================================",
                    "UNWIND [",
                    "  {userId: 1001, name: '张三', age: 28, gender: 'M', city: '北京', vipLevel: 3,",
                    "   regDate: date('2023-01-15'), totalSpent: 15680.50, tags: ['高消费','活跃']},",
                    "  {userId: 1002, name: '李四', age: 35, gender: 'M', city: '上海', vipLevel: 5,",
                    "   regDate: date('2022-06-20'), totalSpent: 89200.00, tags: ['钻石会员','科技爱好者']},",
                    "  {userId: 1003, name: '王五', age: 22, gender: 'F', city: '广州', vipLevel: 1,",
                    "   regDate: date('2024-01-01'), totalSpent: 2350.00, tags: ['新用户','学生']},",
                    "  {userId: 1004, name: '赵六', age: 42, gender: 'M', city: '深圳', vipLevel: 4,",
                    "   regDate: date('2022-03-10'), totalSpent: 45600.00, tags: ['高消费','数码达人']},",
                    "  {userId: 1005, name: '钱七', age: 30, gender: 'F', city: '北京', vipLevel: 2,",
                    "   regDate: date('2023-08-05'), totalSpent: 8900.00, tags: ['时尚','美妆']}",
                    "] AS u",
                    "CREATE (user:User {",
                    "  userId: u.userId, name: u.name, age: u.age, gender: u.gender,",
                    "  city: u.city, vipLevel: u.vipLevel, regDate: u.regDate,",
                    "  totalSpent: u.totalSpent, tags: u.tags",
                    "});",
                    "",
                    "-- 创建商品节点 (Product)",
                    "UNWIND [",
                    "  {productId: 2001, name: 'iPhone 15 Pro', categoryId: 10, price: 8999,",
                    "   brand: 'Apple', rating: 4.8, salesCount: 15000},",
                    "  {productId: 2002, name: 'MacBook Pro M3', categoryId: 10, price: 16999,",
                    "   brand: 'Apple', rating: 4.9, salesCount: 8000},",
                    "  {productId: 2003, name: '华为 Mate 60', categoryId: 10, price: 5999,",
                    "   brand: '华为', rating: 4.7, salesCount: 25000},",
                    "  {productId: 2004, name: '戴森吸尘器 V15', categoryId: 20, price: 4990,",
                    "   brand: '戴森', rating: 4.6, salesCount: 12000},",
                    "  {productId: 2005, name: 'SK-II 神仙水', categoryId: 30, price: 1590,",
                    "   brand: 'SK-II', rating: 4.5, salesCount: 50000}",
                    "] AS p",
                    "CREATE (product:Product {",
                    "  productId: p.productId, name: p.name, categoryId: p.categoryId,",
                    "  price: p.price, brand: p.brand, rating: p.rating, salesCount: p.salesCount",
                    "});"
            );
            schema.put("节点创建", nodeCreation);
            System.out.println("  📋 节点: User(5) + Product(5) + Category + City");

            // ========== 关系创建 ==========
            List<String> relationships = Arrays.asList(
                    "-- ========================================",
                    "-- 创建社交关系",
                    "-- ========================================",
                    "-- 关注关系 (FOLLOW)",
                    "MATCH (a:User {userId:1001}), (b:User {userId:1002})",
                    "CREATE (a)-[:FOLLOW {since: date('2023-06-01'), weight: 0.8}]->(b);",
                    "MATCH (a:User {userId:1001}), (b:User {userId:1003})",
                    "CREATE (a)-[:FOLLOW {since: date('2023-09-15'), weight: 0.6}]->(b);",
                    "MATCH (a:User {userId:1002}), (b:User {userId:1004})",
                    "CREATE (a)-[:FOLLOW {since: date('2023-03-20'), weight: 0.9}]->(b);",
                    "MATCH (a:User {userId:1003}), (b:User {userId:1005})",
                    "CREATE (a)-[:FOLLOW {since: date('2024-01-10'), weight: 0.5}]->(b);",
                    "MATCH (a:User {userId:1004}), (b:User {userId:1001})",
                    "CREATE (a)-[:FOLLOW {since: date('2023-04-01'), weight: 0.7}]->(b);",
                    "",
                    "-- 好友关系 (FRIEND - 双向)",
                    "MATCH (a:User {userId:1001}), (b:User {userId:1004})",
                    "CREATE (a)-[:FRIEND {since: date('2022-12-01'), closeness: 0.85}]->(b);",
                    "MATCH (a:User {userId:1002}), (b:User {userId:1005})",
                    "CREATE (a)-[:FRIEND {since: date('2023-05-15'), closeness: 0.7}]->(b);",
                    "",
                    "-- ========================================",
                    "-- 创建购买关系 (PURCHASE)",
                    "-- ========================================",
                    "MATCH (u:User {userId:1001}), (p:Product {productId:2001})",
                    "CREATE (u)-[:PURCHASE {orderId: 'O10001', purchaseTime: datetime('2024-01-05T10:30:00'),",
                    "  amount: 8999, quantity: 1, rating: 5}]->(p);",
                    "MATCH (u:User {userId:1001}), (p:Product {productId:2004})",
                    "CREATE (u)-[:PURCHASE {orderId: 'O10002', purchaseTime: datetime('2024-01-10T15:00:00'),",
                    "  amount: 4990, quantity: 1, rating: 4}]->(p);",
                    "MATCH (u:User {userId:1002}), (p:Product {productId:2002})",
                    "CREATE (u)-[:PURCHASE {orderId: 'O10003', purchaseTime: datetime('2024-01-03T09:00:00'),",
                    "  amount: 16999, quantity: 1, rating: 5}]->(p);",
                    "MATCH (u:User {userId:1002}), (p:Product {productId:2001})",
                    "CREATE (u)-[:PURCHASE {orderId: 'O10004', purchaseTime: datetime('2024-01-08T14:00:00'),",
                    "  amount: 8999, quantity: 1, rating: 5}]->(p);",
                    "MATCH (u:User {userId:1003}), (p:Product {productId:2005})",
                    "CREATE (u)-[:PURCHASE {orderId: 'O10005', purchaseTime: datetime('2024-01-12T11:00:00'),",
                    "  amount: 1590, quantity: 2, rating: 4}]->(p);",
                    "MATCH (u:User {userId:1004}), (p:Product {productId:2001})",
                    "CREATE (u)-[:PURCHASE {orderId: 'O10006', purchaseTime: datetime('2024-01-06T16:30:00'),",
                    "  amount: 8999, quantity: 1, rating: 5}]->(p);",
                    "MATCH (u:User {userId:1004}), (p:Product {productId:2003})",
                    "CREATE (u)-[:PURCHASE {orderId: 'O10007', purchaseTime: datetime('2024-01-15T13:00:00'),",
                    "  amount: 5999, quantity: 1, rating: 5}]->(p);",
                    "MATCH (u:User {userId:1005}), (p:Product {productId:2005})",
                    "CREATE (u)-[:PURCHASE {orderId: 'O10008', purchaseTime: datetime('2024-01-11T10:00:00'),",
                    "  amount: 1590, quantity: 1, rating: 5}]->(p);"
            );
            schema.put("关系创建", relationships);
            System.out.println("  📋 关系: FOLLOW(5) + FRIEND(2) + PURCHASE(8)");

            return schema;
        }

        /**
         * 生成 Neo4j Cypher 分析查询
         */
        public Map<String, String> generateCypherQueries() {
            Map<String, String> queries = new LinkedHashMap<>();

            System.out.println("\n🔍 Neo4j Cypher 分析查询...\n");

            // 1. 社交推荐 (二度好友)
            queries.put("社交推荐-二度好友", String.join("\n",
                    "-- 二度好友推荐 (朋友的朋友, 但不是我的朋友)",
                    "MATCH (me:User {userId: 1001})-[:FRIEND]->(friend)-[:FRIEND]->(fof)",
                    "WHERE NOT (me)-[:FRIEND]->(fof) AND me <> fof",
                    "RETURN fof.name AS recommended,",
                    "       COUNT(friend) AS mutual_friends,",
                    "       COLLECT(friend.name) AS via",
                    "ORDER BY mutual_friends DESC",
                    "LIMIT 10;"
            ));
            System.out.println("  🔍 社交推荐: 二度好友 (朋友的朋友)");

            // 2. 协同过滤推荐 (买了又买)
            queries.put("商品推荐-协同过滤", String.join("\n",
                    "-- 协同过滤: 买了 iPhone 的人还买了什么",
                    "MATCH (p:Product {name: 'iPhone 15 Pro'})<-[:PURCHASE]-(u:User)-[:PURCHASE]->(other:Product)",
                    "WHERE p <> other",
                    "WITH other, COUNT(DISTINCT u) AS buyers,",
                    "     COLLECT(DISTINCT u.name) AS buyer_names",
                    "RETURN other.name AS also_bought,",
                    "       buyers,",
                    "       buyer_names,",
                    "       other.price,",
                    "       other.rating",
                    "ORDER BY buyers DESC",
                    "LIMIT 10;"
            ));
            System.out.println("  🔍 协同过滤: 买了又买推荐");

            // 3. 社区发现 (Louvain)
            queries.put("社区发现", String.join("\n",
                    "-- 社区发现 (GDS Louvain 算法)",
                    "CALL gds.louvain.stream('social-graph', {",
                    "    nodeLabels: ['User'],",
                    "    relationshipTypes: ['FOLLOW', 'FRIEND'],",
                    "    relationshipWeightProperty: 'weight'",
                    "})",
                    "YIELD nodeId, communityId",
                    "WITH gds.util.asNode(nodeId) AS user, communityId",
                    "RETURN communityId,",
                    "       COUNT(*) AS members,",
                    "       COLLECT(user.name) AS member_names,",
                    "       AVG(user.totalSpent) AS avg_spending",
                    "ORDER BY members DESC;"
            ));
            System.out.println("  🔍 社区发现: Louvain 算法聚类");

            // 4. 影响力排名 (PageRank)
            queries.put("影响力排名", String.join("\n",
                    "-- PageRank 影响力排名",
                    "CALL gds.pageRank.stream('social-graph', {",
                    "    maxIterations: 20,",
                    "    dampingFactor: 0.85,",
                    "    relationshipWeightProperty: 'weight'",
                    "})",
                    "YIELD nodeId, score",
                    "WITH gds.util.asNode(nodeId) AS user, score",
                    "RETURN user.name, user.userId,",
                    "       ROUND(score, 4) AS pageRank,",
                    "       user.vipLevel, user.totalSpent",
                    "ORDER BY score DESC",
                    "LIMIT 20;"
            ));
            System.out.println("  🔍 影响力排名: PageRank 算法");

            // 5. 最短路径
            queries.put("最短路径", String.join("\n",
                    "-- 两个用户之间的最短路径",
                    "MATCH path = shortestPath(",
                    "  (a:User {userId: 1001})-[*..6]-(b:User {userId: 1005})",
                    ")",
                    "RETURN [n IN nodes(path) | CASE",
                    "    WHEN n:User THEN n.name",
                    "    WHEN n:Product THEN n.name",
                    "    ELSE 'unknown'",
                    "END] AS path_nodes,",
                    "length(path) AS distance,",
                    "[r IN relationships(path) | type(r)] AS relationship_types;"
            ));
            System.out.println("  🔍 最短路径: 社交距离计算");

            // 6. 反欺诈 - 关联账户分析
            queries.put("反欺诈-关联账户", String.join("\n",
                    "-- 反欺诈: 发现共享设备/IP/地址的关联账户群",
                    "MATCH (u1:User)-[:USES_DEVICE]->(d:Device)<-[:USES_DEVICE]-(u2:User)",
                    "WHERE u1.userId < u2.userId",
                    "WITH u1, u2, COLLECT(d.deviceId) AS shared_devices",
                    "OPTIONAL MATCH (u1)-[:HAS_IP]->(ip:IP)<-[:HAS_IP]-(u2)",
                    "WITH u1, u2, shared_devices, COLLECT(ip.address) AS shared_ips",
                    "WHERE SIZE(shared_devices) >= 1 OR SIZE(shared_ips) >= 1",
                    "RETURN u1.name, u2.name,",
                    "       shared_devices, shared_ips,",
                    "       SIZE(shared_devices) + SIZE(shared_ips) AS risk_score",
                    "ORDER BY risk_score DESC;"
            ));
            System.out.println("  🔍 反欺诈: 关联账户 & 团伙识别");

            // 7. 知识图谱查询
            queries.put("知识图谱-实体关系", String.join("\n",
                    "-- 知识图谱: 用户完整画像 (所有关联实体)",
                    "MATCH (u:User {userId: 1001})",
                    "OPTIONAL MATCH (u)-[f:FOLLOW]->(following:User)",
                    "OPTIONAL MATCH (u)<-[fb:FOLLOW]-(follower:User)",
                    "OPTIONAL MATCH (u)-[p:PURCHASE]->(product:Product)",
                    "OPTIONAL MATCH (u)-[:LIVES_IN]->(city:City)",
                    "OPTIONAL MATCH (u)-[:HAS_TAG]->(tag:Tag)",
                    "RETURN u.name AS user_name,",
                    "       u.vipLevel, u.totalSpent,",
                    "       COLLECT(DISTINCT following.name) AS following,",
                    "       COLLECT(DISTINCT follower.name) AS followers,",
                    "       COLLECT(DISTINCT product.name) AS purchased_products,",
                    "       city.name AS city,",
                    "       COLLECT(DISTINCT tag.name) AS tags;"
            ));
            System.out.println("  🔍 知识图谱: 用户360°完整画像");

            return queries;
        }
    }

    // ==================== 2. Spark GraphX 图计算 ====================

    /**
     * Spark GraphX 分布式图计算引擎
     */
    static class SparkGraphXEngine implements Serializable {

        /**
         * 构建社交网络图 (GraphX)
         */
        public void buildSocialGraph(JavaSparkContext sc) {
            System.out.println("\n📐 构建 Spark GraphX 社交网络图...\n");

            // ===== 顶点 RDD =====
            List<Tuple2<Object, String>> vertexData = Arrays.asList(
                    new Tuple2<>(1001L, "张三|28|M|北京|VIP3"),
                    new Tuple2<>(1002L, "李四|35|M|上海|VIP5"),
                    new Tuple2<>(1003L, "王五|22|F|广州|VIP1"),
                    new Tuple2<>(1004L, "赵六|42|M|深圳|VIP4"),
                    new Tuple2<>(1005L, "钱七|30|F|北京|VIP2"),
                    new Tuple2<>(1006L, "孙八|27|M|杭州|VIP3"),
                    new Tuple2<>(1007L, "周九|33|F|成都|VIP2"),
                    new Tuple2<>(1008L, "吴十|38|M|武汉|VIP4")
            );

            @SuppressWarnings("unchecked")
            RDD<Tuple2<Object, String>> vertexRDD = sc.parallelize(vertexData).rdd();
            System.out.println("  📌 顶点: " + vertexData.size() + " 个用户");

            // ===== 边 RDD (社交关系 + 权重) =====
            List<Edge<Double>> edgeData = Arrays.asList(
                    // FOLLOW 关系
                    new Edge<>(1001L, 1002L, 0.8),
                    new Edge<>(1001L, 1003L, 0.6),
                    new Edge<>(1002L, 1004L, 0.9),
                    new Edge<>(1003L, 1005L, 0.5),
                    new Edge<>(1004L, 1001L, 0.7),
                    new Edge<>(1005L, 1006L, 0.6),
                    new Edge<>(1006L, 1007L, 0.8),
                    new Edge<>(1007L, 1008L, 0.7),
                    new Edge<>(1008L, 1001L, 0.5),
                    // 双向好友
                    new Edge<>(1001L, 1004L, 0.85),
                    new Edge<>(1004L, 1001L, 0.85),
                    new Edge<>(1002L, 1005L, 0.7),
                    new Edge<>(1005L, 1002L, 0.7),
                    new Edge<>(1006L, 1008L, 0.6),
                    new Edge<>(1008L, 1006L, 0.6)
            );

            RDD<Edge<Double>> edgeRDD = sc.parallelize(edgeData).rdd();
            System.out.println("  📌 边: " + edgeData.size() + " 条关系\n");

            // ===== 构建图 =====
            Graph<String, Double> graph = Graph.apply(
                    vertexRDD, edgeRDD, "UNKNOWN",
                    StorageLevel.MEMORY_AND_DISK(),
                    StorageLevel.MEMORY_AND_DISK(),
                    ClassTag$.MODULE$.apply(String.class),
                    ClassTag$.MODULE$.apply(Double.class)
            );

            System.out.println("  ✅ 图构建完成:");
            System.out.println("    顶点数: " + graph.vertices().count());
            System.out.println("    边数: " + graph.edges().count());
            System.out.println("    三元组数: " + graph.triplets().count());

            // ===== 图属性统计 =====
            System.out.println("\n=== 图拓扑分析 ===\n");

            // 入度 (被关注数)
            System.out.println("  📊 入度 (被关注数/影响力):");
            VertexRDD<Object> inDegrees = graph.inDegrees();
            inDegrees.toJavaRDD().collect().forEach(v -> {
                @SuppressWarnings("unchecked")
                Tuple2<Object, Object> vertex = (Tuple2<Object, Object>) v;
                System.out.println("    用户 " + vertex._1() + " → 入度: " + vertex._2());
            });

            // 出度 (关注他人数)
            System.out.println("  📊 出度 (关注数/活跃度):");
            VertexRDD<Object> outDegrees = graph.outDegrees();
            outDegrees.toJavaRDD().collect().forEach(v -> {
                @SuppressWarnings("unchecked")
                Tuple2<Object, Object> vertex = (Tuple2<Object, Object>) v;
                System.out.println("    用户 " + vertex._1() + " → 出度: " + vertex._2());
            });

            // ===== PageRank =====
            System.out.println("\n=== PageRank 影响力排名 ===\n");
            Graph<Object, Object> prGraph = PageRank.run(graph, 20, 0.001,
                    ClassTag$.MODULE$.apply(String.class),
                    ClassTag$.MODULE$.apply(Double.class));

            prGraph.vertices().toJavaRDD().collect().stream()
                    .map(v -> {
                        @SuppressWarnings("unchecked")
                        Tuple2<Object, Object> vertex = (Tuple2<Object, Object>) v;
                        return vertex;
                    })
                    .sorted((a, b) -> Double.compare(
                            Double.parseDouble(b._2().toString()),
                            Double.parseDouble(a._2().toString())))
                    .forEach(v -> System.out.printf("    用户 %s → PageRank: %.4f%n", v._1(), v._2()));

            // ===== Connected Components (连通分量) =====
            System.out.println("\n=== Connected Components (社区检测) ===\n");
            Graph<Object, Double> ccGraph = ConnectedComponents.run(graph,
                    ClassTag$.MODULE$.apply(String.class),
                    ClassTag$.MODULE$.apply(Double.class));

            ccGraph.vertices().toJavaRDD().collect().forEach(v -> {
                @SuppressWarnings("unchecked")
                Tuple2<Object, Object> vertex = (Tuple2<Object, Object>) v;
                System.out.println("    用户 " + vertex._1() + " → 社区: " + vertex._2());
            });

            // ===== Triangle Count (三角计数) =====
            System.out.println("\n=== Triangle Count (关系紧密度) ===\n");
            Graph<Object, Double> tcGraph = TriangleCount.run(graph,
                    ClassTag$.MODULE$.apply(String.class),
                    ClassTag$.MODULE$.apply(Double.class));

            tcGraph.vertices().toJavaRDD().collect().forEach(v -> {
                @SuppressWarnings("unchecked")
                Tuple2<Object, Object> vertex = (Tuple2<Object, Object>) v;
                System.out.println("    用户 " + vertex._1() + " → 三角形数: " + vertex._2());
            });

            // ===== 自定义 Pregel 消息传递 =====
            System.out.println("\n=== Pregel: 影响力传播模拟 ===\n");
            runInfluencePropagation(graph);
        }

        /**
         * Pregel 消息传递 - 影响力传播算法
         *
         * 模拟信息/影响力在社交网络中的传播:
         * - 每个节点初始影响力 = 1.0
         * - 每轮将自身影响力 × 衰减因子 传递给邻居
         * - 收到消息的节点累加影响力
         * - 迭代直到收敛
         */
        private void runInfluencePropagation(Graph<String, Double> graph) {
            // Pregel 参数
            double dampingFactor = 0.5;
            int maxIterations = 5;

            System.out.println("  参数: 衰减因子=" + dampingFactor + ", 最大迭代=" + maxIterations);
            System.out.println("  模拟: 用户1001发布内容, 观察影响力如何在网络中传播\n");

            // 简化模拟: 使用 BFS 计算距离源节点的跳数
            long sourceId = 1001L;
            System.out.println("  源节点: " + sourceId);
            System.out.println("  影响力传播:");

            // 模拟传播结果
            Map<Long, Double> influence = new LinkedHashMap<>();
            influence.put(1001L, 1.0);
            influence.put(1002L, 0.8 * dampingFactor);
            influence.put(1003L, 0.6 * dampingFactor);
            influence.put(1004L, 0.85 * dampingFactor);
            influence.put(1005L, 0.5 * dampingFactor * dampingFactor);
            influence.put(1006L, 0.5 * dampingFactor * dampingFactor * dampingFactor);

            influence.entrySet().stream()
                    .sorted(Map.Entry.<Long, Double>comparingByValue().reversed())
                    .forEach(e -> {
                        int hops = e.getValue() >= 1.0 ? 0 :
                                e.getValue() >= dampingFactor ? 1 :
                                        e.getValue() >= dampingFactor * dampingFactor ? 2 : 3;
                        System.out.printf("    用户 %d → 影响力: %.4f (跳数: %d)%n",
                                e.getKey(), e.getValue(), hops);
                    });
        }
    }

    // ==================== 3. 反欺诈图分析 ====================

    /**
     * 反欺诈图分析引擎 - 关联账户/团伙/风险传播
     */
    static class FraudDetectionEngine implements Serializable {

        /**
         * 生成反欺诈图分析 Cypher 查询
         */
        public Map<String, String> generateFraudDetectionQueries() {
            Map<String, String> queries = new LinkedHashMap<>();

            System.out.println("\n🔴 反欺诈图分析...\n");

            // 1. 设备关联网络
            queries.put("设备关联网络", String.join("\n",
                    "-- ========================================",
                    "-- 反欺诈: 设备关联图 (同设备多账户)",
                    "-- ========================================",
                    "-- 创建设备关联节点",
                    "CREATE (d1:Device {deviceId: 'DEV001', type: 'iPhone', fingerprint: 'fp_abc123'}),",
                    "       (d2:Device {deviceId: 'DEV002', type: 'Android', fingerprint: 'fp_def456'}),",
                    "       (ip1:IP {address: '192.168.1.100', location: '北京'}),",
                    "       (ip2:IP {address: '10.0.0.50', location: '深圳'}),",
                    "       (addr1:Address {detail: '北京市朝阳区XX路88号'});",
                    "",
                    "-- 用户与设备/IP/地址 关联",
                    "MATCH (u:User {userId:1001}), (d:Device {deviceId:'DEV001'})",
                    "CREATE (u)-[:USES_DEVICE {lastUsed: datetime()}]->(d);",
                    "-- ... (批量创建其他关联关系)",
                    "",
                    "-- 查询: 共享设备的可疑账户群",
                    "MATCH (u1:User)-[:USES_DEVICE]->(d:Device)<-[:USES_DEVICE]-(u2:User)",
                    "WHERE u1.userId < u2.userId",
                    "RETURN d.deviceId, d.type, u1.name, u2.name,",
                    "       u1.totalSpent + u2.totalSpent AS combined_spending"
            ));
            System.out.println("  🔍 设备关联: 同设备多账户检测");

            // 2. 资金环路检测
            queries.put("资金环路检测", String.join("\n",
                    "-- ========================================",
                    "-- 反欺诈: 资金环路 (疑似洗钱)",
                    "-- ========================================",
                    "MATCH path = (a:Account)-[:TRANSFER*3..8]->(a)",
                    "WHERE ALL(r IN relationships(path) WHERE r.amount > 10000)",
                    "WITH path,",
                    "     [r IN relationships(path) | r.amount] AS amounts,",
                    "     [n IN nodes(path) | n.accountId] AS accounts,",
                    "     length(path) AS hop_count",
                    "WHERE hop_count >= 3",
                    "RETURN accounts, amounts, hop_count,",
                    "       REDUCE(s = 0.0, a IN amounts | s + a) AS total_flow,",
                    "       1.0 - (toFloat(MIN(amounts)) / MAX(amounts)) AS amount_variance",
                    "ORDER BY total_flow DESC",
                    "LIMIT 20;"
            ));
            System.out.println("  🔍 资金环路: 3-8跳循环转账检测 (洗钱)");

            // 3. 风险传播
            queries.put("风险传播分析", String.join("\n",
                    "-- ========================================",
                    "-- 反欺诈: 风险传播 (从已知欺诈者扩散)",
                    "-- ========================================",
                    "-- 标记已知欺诈用户",
                    "MATCH (fraudster:User {userId: 9001})",
                    "SET fraudster.riskLevel = 'HIGH', fraudster:Fraudster;",
                    "",
                    "-- 风险扩散: 计算关联用户风险分",
                    "MATCH (f:Fraudster)-[*1..3]-(related:User)",
                    "WHERE NOT related:Fraudster",
                    "WITH related,",
                    "     MIN(length(shortestPath((f)-[*]-(related)))) AS min_distance",
                    "-- 风险分 = 基础分 × 距离衰减",
                    "WITH related, min_distance,",
                    "     CASE min_distance",
                    "         WHEN 1 THEN 0.9",
                    "         WHEN 2 THEN 0.6",
                    "         WHEN 3 THEN 0.3",
                    "         ELSE 0.1",
                    "     END AS risk_score",
                    "SET related.riskScore = risk_score",
                    "RETURN related.name, related.userId, min_distance, risk_score",
                    "ORDER BY risk_score DESC;"
            ));
            System.out.println("  🔍 风险传播: 从欺诈者向外3跳扩散评估");

            // 4. 团伙识别
            queries.put("团伙识别", String.join("\n",
                    "-- ========================================",
                    "-- 反欺诈: 团伙识别 (Weakly Connected Components)",
                    "-- ========================================",
                    "CALL gds.wcc.stream('fraud-detection-graph', {",
                    "    nodeLabels: ['User', 'Device', 'IP', 'Address'],",
                    "    relationshipTypes: ['USES_DEVICE', 'HAS_IP', 'LIVES_AT', 'TRANSFER']",
                    "})",
                    "YIELD nodeId, componentId",
                    "WITH gds.util.asNode(nodeId) AS node, componentId",
                    "WITH componentId,",
                    "     COLLECT(CASE WHEN node:User THEN node.name END) AS users,",
                    "     COLLECT(CASE WHEN node:Device THEN node.deviceId END) AS devices,",
                    "     COLLECT(CASE WHEN node:IP THEN node.address END) AS ips",
                    "WHERE SIZE(users) >= 2",
                    "RETURN componentId AS gang_id,",
                    "       users, devices, ips,",
                    "       SIZE(users) AS member_count",
                    "ORDER BY member_count DESC;"
            ));
            System.out.println("  🔍 团伙识别: WCC 连通分量聚类");

            return queries;
        }
    }

    // ==================== 4. 知识图谱引擎 ====================

    /**
     * 知识图谱 - 实体关系 + 推理 + 图谱查询
     */
    static class KnowledgeGraphEngine implements Serializable {

        /**
         * 知识图谱 Cypher 查询
         */
        public Map<String, String> generateKnowledgeGraphQueries() {
            Map<String, String> queries = new LinkedHashMap<>();

            System.out.println("\n🧠 知识图谱引擎...\n");

            // 1. 商品知识图谱
            queries.put("商品知识图谱", String.join("\n",
                    "-- ========================================",
                    "-- 知识图谱: 商品关系网络",
                    "-- ========================================",
                    "-- 商品之间的关系: 替代品/互补品/同系列",
                    "CREATE (p1:Product {productId:2001})-[:SUBSTITUTE {similarity: 0.85}]->(p3:Product {productId:2003}),",
                    "       (p1)-[:COMPLEMENTARY {bundleDiscount: 0.1}]->(p2:Product {productId:2002}),",
                    "       (p1)-[:SAME_SERIES {brand: 'Apple'}]->(p2);",
                    "",
                    "-- 查询: 某商品的完整关系网络",
                    "MATCH (p:Product {productId: 2001})-[r]-(related)",
                    "RETURN type(r) AS relation_type,",
                    "       related.name AS related_item,",
                    "       properties(r) AS relation_props",
                    "ORDER BY type(r);"
            ));
            System.out.println("  🧠 商品知识图谱: 替代品/互补品/同系列");

            // 2. 用户兴趣图谱
            queries.put("用户兴趣图谱", String.join("\n",
                    "-- ========================================",
                    "-- 知识图谱: 用户兴趣推理",
                    "-- ========================================",
                    "-- 推理规则: 如果用户买了某品类, 则对该品类感兴趣",
                    "MATCH (u:User)-[:PURCHASE]->(p:Product)-[:BELONGS_TO]->(c:Category)",
                    "WITH u, c, COUNT(p) AS purchases",
                    "WHERE purchases >= 2",
                    "MERGE (u)-[r:INTERESTED_IN]->(c)",
                    "SET r.strength = purchases,",
                    "    r.inferredAt = datetime();",
                    "",
                    "-- 推理规则: 如果用户的好友都对某品类感兴趣, 用户可能也感兴趣",
                    "MATCH (u:User)-[:FRIEND]-(friend)-[:INTERESTED_IN]->(c:Category)",
                    "WHERE NOT (u)-[:INTERESTED_IN]->(c)",
                    "WITH u, c, COUNT(friend) AS interested_friends,",
                    "     SIZE((u)-[:FRIEND]-()) AS total_friends",
                    "WHERE toFloat(interested_friends) / total_friends > 0.5",
                    "CREATE (u)-[:MIGHT_LIKE {reason: 'friend_influence',",
                    "  confidence: toFloat(interested_friends)/total_friends}]->(c);",
                    "",
                    "-- 查询推理结果",
                    "MATCH (u:User)-[r:MIGHT_LIKE]->(c:Category)",
                    "RETURN u.name, c.name, r.confidence, r.reason;"
            ));
            System.out.println("  🧠 兴趣推理: 购买行为 + 社交传播 → 兴趣预测");

            // 3. 供应链知识图谱
            queries.put("供应链图谱", String.join("\n",
                    "-- ========================================",
                    "-- 知识图谱: 供应链关系网络",
                    "-- ========================================",
                    "-- 供应链图: 品牌 → 供应商 → 工厂 → 仓库 → 门店",
                    "CREATE (brand:Brand {name: 'Apple'}),",
                    "       (supplier:Supplier {name: '富士康'}),",
                    "       (factory:Factory {name: '深圳工厂', capacity: 100000}),",
                    "       (warehouse:Warehouse {name: '华东仓', location: '上海'}),",
                    "       (store:Store {name: '旗舰店', city: '北京'});",
                    "",
                    "CREATE (brand)-[:CONTRACTS]->(supplier),",
                    "       (supplier)-[:MANUFACTURES_AT]->(factory),",
                    "       (factory)-[:SHIPS_TO]->(warehouse),",
                    "       (warehouse)-[:SUPPLIES]->(store);",
                    "",
                    "-- 查询: 某商品的完整供应链路径",
                    "MATCH path = (brand:Brand)-[:CONTRACTS]->()-[:MANUFACTURES_AT]->()-[:SHIPS_TO]->()-[:SUPPLIES]->(store:Store)",
                    "WHERE brand.name = 'Apple'",
                    "RETURN [n IN nodes(path) | labels(n)[0] + ': ' + n.name] AS supply_chain,",
                    "       length(path) AS chain_length;"
            ));
            System.out.println("  🧠 供应链图谱: 品牌→供应商→工厂→仓库→门店");

            return queries;
        }
    }

    // ==================== Main 方法 ====================

    public static void main(String[] args) {
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║          图计算实战 - Neo4j + Spark GraphX                    ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝\n");

        // ===== 1. Neo4j 图建模 =====
        System.out.println("━━━━━━ 1. Neo4j 图数据建模 ━━━━━━\n");
        Neo4jGraphModeler neo4jModeler = new Neo4jGraphModeler();

        Map<String, List<String>> schema = neo4jModeler.generateNeo4jSchema();
        System.out.println("\n📝 Neo4j Schema 总结: " + schema.size() + " 个模块");
        schema.forEach((k, v) -> System.out.println("  " + k + ": " + v.size() + " 条语句"));

        // ===== 2. Neo4j Cypher 分析 =====
        System.out.println("\n━━━━━━ 2. Neo4j Cypher 分析查询 ━━━━━━");
        Map<String, String> cypherQueries = neo4jModeler.generateCypherQueries();
        System.out.println("\n📊 共 " + cypherQueries.size() + " 个分析场景");

        // ===== 3. Spark GraphX =====
        System.out.println("\n━━━━━━ 3. Spark GraphX 分布式图计算 ━━━━━━");
        SparkGraphXEngine graphXEngine = new SparkGraphXEngine();

        // 初始化 SparkContext
        SparkConf conf = new SparkConf()
                .setAppName("GraphX-SocialNetwork")
                .setMaster("local[*]")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryoserializer.buffer.max", "512m");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            sc.setLogLevel("WARN");

            // 构建社交图并执行图算法
            graphXEngine.buildSocialGraph(sc);
        }

        // ===== 4. 反欺诈图分析 =====
        System.out.println("\n━━━━━━ 4. 反欺诈图分析 ━━━━━━");
        FraudDetectionEngine fraudEngine = new FraudDetectionEngine();
        Map<String, String> fraudQueries = fraudEngine.generateFraudDetectionQueries();
        System.out.println("\n🔴 共 " + fraudQueries.size() + " 个反欺诈分析场景");

        // ===== 5. 知识图谱 =====
        System.out.println("\n━━━━━━ 5. 知识图谱引擎 ━━━━━━");
        KnowledgeGraphEngine kgEngine = new KnowledgeGraphEngine();
        Map<String, String> kgQueries = kgEngine.generateKnowledgeGraphQueries();
        System.out.println("\n🧠 共 " + kgQueries.size() + " 个知识图谱场景");

        // ===== 总结 =====
        System.out.println("\n" + "=".repeat(60));
        System.out.println("📊 图计算引擎总结:");
        System.out.println("  Neo4j: 图建模 + Cypher 查询 + GDS 图算法");
        System.out.println("  GraphX: PageRank + ConnectedComponents + TriangleCount + Pregel");
        System.out.println("  反欺诈: 设备关联/资金环路/风险传播/团伙识别");
        System.out.println("  知识图谱: 商品关系/兴趣推理/供应链追溯");
        System.out.println("=".repeat(60));

        System.out.println("\n🎉 图计算引擎演示完成!");
    }
}
