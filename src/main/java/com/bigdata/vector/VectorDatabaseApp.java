package com.bigdata.vector;

/**
 * ============================================================
 * 向量数据库 & LLM RAG Pipeline 实战
 * ============================================================
 *
 * AI 时代的数据基础设施：
 * - 向量数据库是 LLM 应用的核心存储层
 * - RAG (Retrieval-Augmented Generation) 是企业级 AI 的标准范式
 * - Embedding 是连接结构化数据与 AI 的桥梁
 *
 * 本模块涵盖：
 * 1. 向量数据库 Milvus 架构与操作
 * 2. Embedding 模型选型与生成
 * 3. RAG Pipeline 端到端实现
 * 4. 混合搜索 (Dense + Sparse + Metadata)
 * 5. 向量索引策略 (IVF_FLAT/HNSW/DiskANN)
 * 6. 大数据 + AI 融合架构
 *
 * @author bigdata-team
 */
public class VectorDatabaseApp {

    public static void main(String[] args) {
        System.out.println("╔═══════════════════════════════════════════════════════════╗");
        System.out.println("║   向量数据库 & LLM RAG Pipeline 深度实战                    ║");
        System.out.println("╚═══════════════════════════════════════════════════════════╝");

        // ============================================================
        // 第1部分: Milvus 向量数据库架构
        // ============================================================
        demonstrateMilvusArchitecture();

        // ============================================================
        // 第2部分: Embedding 模型与向量生成
        // ============================================================
        demonstrateEmbedding();

        // ============================================================
        // 第3部分: RAG Pipeline 端到端
        // ============================================================
        demonstrateRAGPipeline();

        // ============================================================
        // 第4部分: 混合搜索策略
        // ============================================================
        demonstrateHybridSearch();

        // ============================================================
        // 第5部分: 向量索引深度
        // ============================================================
        demonstrateVectorIndex();

        // ============================================================
        // 第6部分: 大数据 + AI 融合架构
        // ============================================================
        demonstrateBigDataAIFusion();

        System.out.println("\n✅ 向量数据库 & LLM RAG Pipeline 全部演示完成!");
    }

    // ============================================================
    //  第1部分: Milvus 向量数据库架构
    // ============================================================
    private static void demonstrateMilvusArchitecture() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🔮 第1部分: Milvus 向量数据库架构");
        System.out.println("=".repeat(60));

        // 1.1 Milvus 分布式架构
        System.out.println("\n🏗️ 1.1 Milvus 分布式架构:");
        String milvusArch = """
            ┌─────────────────────────────────────────────────────────────────┐
            │                    Milvus 2.x 分布式架构                         │
            ├─────────────────────────────────────────────────────────────────┤
            │                                                                 │
            │  Client SDK (Python / Java / Go / Node.js)                     │
            │        │                                                        │
            │        ▼                                                        │
            │  ┌─────────────┐     ┌─────────────────────────────────────┐   │
            │  │   Proxy     │ ←──→│           Meta Store (etcd)         │   │
            │  │  (接入层)    │     │  集合/分区/段/索引 元数据             │   │
            │  └──────┬──────┘     └─────────────────────────────────────┘   │
            │         │                                                       │
            │  ┌──────┴───────────────────────────────────────────────┐      │
            │  │              Coordinator (协调层)                      │      │
            │  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌─────────┐│      │
            │  │  │ RootCoord│ │QueryCoord│ │DataCoord │ │IndexCoord││      │
            │  │  │ DDL/权限  │ │ 查询调度  │ │ 写入调度  │ │ 索引调度 ││      │
            │  │  └──────────┘ └──────────┘ └──────────┘ └─────────┘│      │
            │  └──────────────────────┬───────────────────────────────┘      │
            │                         │                                       │
            │  ┌──────────────────────┴───────────────────────────────┐      │
            │  │               Worker (执行层)                         │      │
            │  │  ┌──────────┐ ┌──────────┐ ┌──────────────┐        │      │
            │  │  │QueryNode │ │DataNode  │ │IndexNode     │        │      │
            │  │  │          │ │          │ │              │        │      │
            │  │  │ 加载段   │ │ 写入WAL  │ │ 构建向量索引  │        │      │
            │  │  │ 向量检索 │ │ Flush段  │ │ IVF/HNSW    │        │      │
            │  │  │ 标量过滤 │ │ Compact  │ │ DiskANN     │        │      │
            │  │  └──────────┘ └──────────┘ └──────────────┘        │      │
            │  └──────────────────────────────────────────────────────┘      │
            │                                                                 │
            │  ┌──────────────────────────────────────────────────────┐      │
            │  │              Storage (存储层)                         │      │
            │  │  ┌──────────┐ ┌──────────────────┐                  │      │
            │  │  │ Log      │ │ Object Storage   │                  │      │
            │  │  │ (Pulsar/ │ │ (S3/MinIO/HDFS)  │                  │      │
            │  │  │  Kafka)  │ │ 段文件+索引文件    │                  │      │
            │  │  └──────────┘ └──────────────────┘                  │      │
            │  └──────────────────────────────────────────────────────┘      │
            └─────────────────────────────────────────────────────────────────┘
            
            关键概念:
            ┌────────────┬──────────────────────────────────────────┐
            │ 概念        │ 说明                                     │
            ├────────────┼──────────────────────────────────────────┤
            │ Collection  │ 类似表，包含 Schema (字段+向量维度)       │
            │ Partition   │ 集合内的逻辑分区 (按类目/日期)            │
            │ Segment     │ 数据存储最小单元 (Growing → Sealed)       │
            │ Index       │ 向量索引 (IVF_FLAT/HNSW/DiskANN)        │
            │ Shard       │ 写入通道 (并行写入)                      │
            │ Replica     │ 查询副本 (提高查询吞吐)                  │
            └────────────┴──────────────────────────────────────────┘
            """;
        System.out.println(milvusArch);

        // 1.2 Milvus Docker 部署
        System.out.println("\n🐳 1.2 Milvus Docker 部署:");
        String milvusDeploy = """
            # ============================================================
            # Milvus Standalone 部署 (开发环境)
            # ============================================================
            # docker-compose 配置:
            milvus-etcd:
              image: quay.io/coreos/etcd:v3.5.11
              container_name: milvus-etcd
              environment:
                ETCD_AUTO_COMPACTION_MODE: revision
                ETCD_AUTO_COMPACTION_RETENTION: "1000"
                ETCD_QUOTA_BACKEND_BYTES: "4294967296"
              volumes:
                - milvus-etcd-data:/etcd
              command: >
                etcd -advertise-client-urls=http://127.0.0.1:2379
                -listen-client-urls http://0.0.0.0:2379
                --data-dir /etcd
            
            milvus-minio:
              image: minio/minio:RELEASE.2023-12-20T01-00-02Z
              container_name: milvus-minio
              environment:
                MINIO_ACCESS_KEY: minioadmin
                MINIO_SECRET_KEY: minioadmin
              volumes:
                - milvus-minio-data:/minio_data
              command: minio server /minio_data
              healthcheck:
                test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
            
            milvus-standalone:
              image: milvusdb/milvus:v2.4.1
              container_name: milvus-standalone
              hostname: milvus
              ports:
                - "19530:19530"                    # gRPC 端口
                - "9091:9091"                      # 管理端口
              environment:
                ETCD_ENDPOINTS: milvus-etcd:2379
                MINIO_ADDRESS: milvus-minio:9000
              volumes:
                - milvus-data:/var/lib/milvus
              depends_on:
                - milvus-etcd
                - milvus-minio
            
            # Attu (Milvus Web UI 管理工具)
            milvus-attu:
              image: zilliz/attu:v2.4
              container_name: milvus-attu
              ports:
                - "8003:3000"
              environment:
                MILVUS_URL: milvus-standalone:19530
            """;
        System.out.println(milvusDeploy);

        // 1.3 Milvus Java SDK 操作
        System.out.println("\n☕ 1.3 Milvus Java SDK 操作:");
        String milvusJava = """
            // ============================================================
            // Milvus Java SDK 核心操作
            // ============================================================
            
            // 1. 连接 Milvus
            MilvusServiceClient milvusClient = new MilvusServiceClient(
                ConnectParam.newBuilder()
                    .withHost("localhost")
                    .withPort(19530)
                    .withDatabaseName("ecommerce")
                    .build()
            );
            
            // 2. 创建 Collection (商品向量表)
            FieldType fieldId = FieldType.newBuilder()
                .withName("product_id")
                .withDataType(DataType.Int64)
                .withPrimaryKey(true)
                .withAutoID(false)
                .build();
            
            FieldType fieldName = FieldType.newBuilder()
                .withName("product_name")
                .withDataType(DataType.VarChar)
                .withMaxLength(256)
                .build();
            
            FieldType fieldCategory = FieldType.newBuilder()
                .withName("category")
                .withDataType(DataType.VarChar)
                .withMaxLength(64)
                .build();
            
            FieldType fieldPrice = FieldType.newBuilder()
                .withName("price")
                .withDataType(DataType.Double)
                .build();
            
            FieldType fieldEmbedding = FieldType.newBuilder()
                .withName("embedding")
                .withDataType(DataType.FloatVector)
                .withDimension(768)                   // BGE-M3 维度
                .build();
            
            FieldType fieldSparseEmbedding = FieldType.newBuilder()
                .withName("sparse_embedding")
                .withDataType(DataType.SparseFloatVector)  // 稀疏向量
                .build();
            
            CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName("product_vectors")
                .withDescription("商品向量集合 - 支持语义搜索")
                .withShardsNum(2)
                .addFieldType(fieldId)
                .addFieldType(fieldName)
                .addFieldType(fieldCategory)
                .addFieldType(fieldPrice)
                .addFieldType(fieldEmbedding)
                .addFieldType(fieldSparseEmbedding)
                .build();
            
            milvusClient.createCollection(createParam);
            
            // 3. 创建向量索引
            // IVF_FLAT: 适合小数据量 (< 1M)，高召回率
            milvusClient.createIndex(CreateIndexParam.newBuilder()
                .withCollectionName("product_vectors")
                .withFieldName("embedding")
                .withIndexType(IndexType.HNSW)        // 推荐生产使用
                .withMetricType(MetricType.COSINE)    // 余弦相似度
                .withExtraParam("{\"M\": 16, \"efConstruction\": 256}")
                .build());
            
            // 4. 插入数据
            List<InsertParam.Field> fields = Arrays.asList(
                new InsertParam.Field("product_id", productIds),
                new InsertParam.Field("product_name", productNames),
                new InsertParam.Field("category", categories),
                new InsertParam.Field("price", prices),
                new InsertParam.Field("embedding", embeddings),
                new InsertParam.Field("sparse_embedding", sparseEmbeddings)
            );
            
            milvusClient.insert(InsertParam.newBuilder()
                .withCollectionName("product_vectors")
                .withFields(fields)
                .build());
            
            // 5. 向量相似性搜索
            List<List<Float>> searchVectors = Arrays.asList(queryEmbedding);
            
            SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName("product_vectors")
                .withMetricType(MetricType.COSINE)
                .withTopK(10)
                .withVectors(searchVectors)
                .withVectorFieldName("embedding")
                .withParams("{\"ef\": 128}")           // HNSW 搜索参数
                .withOutFields(Arrays.asList(
                    "product_name", "category", "price"))
                .withExpr("category == 'Electronics' and price < 5000")
                .build();
            
            SearchResults results = milvusClient.search(searchParam);
            
            // 6. 混合搜索 (Dense + Sparse)
            // 见第4部分详细说明
            """;
        System.out.println(milvusJava);
    }

    // ============================================================
    //  第2部分: Embedding 模型与向量生成
    // ============================================================
    private static void demonstrateEmbedding() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🧠 第2部分: Embedding 模型与向量生成");
        System.out.println("=".repeat(60));

        // 2.1 Embedding 模型选型
        System.out.println("\n📊 2.1 Embedding 模型选型对比:");
        String embeddingModels = """
            ┌──────────────────┬───────┬────────┬──────────┬──────────────────┐
            │ 模型              │ 维度   │ 语言   │ 速度     │ 适用场景          │
            ├──────────────────┼───────┼────────┼──────────┼──────────────────┤
            │ BGE-M3           │ 1024  │ 多语言 │ 中等     │ 多语言通用 (推荐)  │
            │ BGE-Large-zh     │ 1024  │ 中文   │ 中等     │ 中文专项          │
            │ text-embedding-3 │ 3072  │ 多语言 │ API调用  │ OpenAI 生态       │
            │ Jina-v2          │ 768   │ 多语言 │ 快       │ 长文本 (8K tokens) │
            │ GTE-Large        │ 1024  │ 多语言 │ 快       │ 阿里云生态         │
            │ E5-Large-v2      │ 1024  │ 英文   │ 快       │ 英文为主           │
            │ Cohere-v3        │ 1024  │ 多语言 │ API调用  │ 企业级 (高质量)    │
            │ BAAI-bge-reranker│ -     │ 多语言 │ 慢       │ 重排序 (二阶段)    │
            └──────────────────┴───────┴────────┴──────────┴──────────────────┘
            
            选型建议:
            ┌────────────────────────────────────────────────────────────────┐
            │ 场景                    │ 推荐模型              │ 理由          │
            ├────────────────────────┼──────────────────────┼──────────────┤
            │ 中文电商商品搜索          │ BGE-Large-zh         │ 中文最优      │
            │ 多语言客服知识库          │ BGE-M3               │ 稀疏+稠密    │
            │ 代码搜索                │ CodeBERT / Jina-Code │ 代码专用      │
            │ 长文档检索              │ Jina-v2 (8K)         │ 长上下文      │
            │ 预算充足+最高质量        │ text-embedding-3-large│ 效果最好     │
            └────────────────────────┴──────────────────────┴──────────────┘
            """;
        System.out.println(embeddingModels);

        // 2.2 大数据向量化 Pipeline
        System.out.println("\n⚙️ 2.2 Spark 批量向量化 Pipeline:");
        String sparkEmbedding = """
            // ============================================================
            // Spark 批量 Embedding 生成 Pipeline
            // 适用于: 将 Hive/Doris 中的业务数据批量向量化
            // ============================================================
            
            // 架构:
            // Hive/Doris 业务数据 → Spark ETL → Embedding API → Milvus
            
            SparkSession spark = SparkSession.builder()
                .appName("ProductEmbeddingPipeline")
                .config("spark.sql.adaptive.enabled", "true")
                .getOrCreate();
            
            // 1. 读取商品数据
            Dataset<Row> products = spark.sql(\"\"\"
                SELECT product_id, product_name, description, category,
                       brand, price, tags,
                       CONCAT_WS(' ',
                           product_name, description, category,
                           brand, tags
                       ) AS text_for_embedding
                FROM dwd.dwd_product_info
                WHERE dt = '${yesterday}'
                  AND status = 'active'
            \"\"\");
            
            // 2. 分批调用 Embedding API
            Dataset<Row> embeddings = products
                .repartition(100)                      // 控制并发
                .mapPartitions((MapPartitionsFunction<Row, Row>) rows -> {
                    // 每个 Partition 内批量调用
                    OkHttpClient client = new OkHttpClient.Builder()
                        .connectTimeout(30, TimeUnit.SECONDS)
                        .readTimeout(60, TimeUnit.SECONDS)
                        .build();
                    
                    List<Row> results = new ArrayList<>();
                    List<Row> batch = new ArrayList<>();
                    
                    while (rows.hasNext()) {
                        batch.add(rows.next());
                        if (batch.size() >= 32) {          // 批大小 32
                            List<float[]> vectors = callEmbeddingAPI(
                                client,
                                batch.stream()
                                    .map(r -> r.getString(7))  // text_for_embedding
                                    .collect(Collectors.toList())
                            );
                            for (int i = 0; i < batch.size(); i++) {
                                results.add(RowFactory.create(
                                    batch.get(i).getLong(0),    // product_id
                                    batch.get(i).getString(1),  // product_name
                                    vectors.get(i)              // embedding
                                ));
                            }
                            batch.clear();
                        }
                    }
                    // 处理剩余
                    if (!batch.isEmpty()) { /* 同上 */ }
                    return results.iterator();
                }, outputEncoder);
            
            // 3. 写入 Milvus
            embeddings.foreachPartition(partition -> {
                MilvusServiceClient milvus = new MilvusServiceClient(
                    ConnectParam.newBuilder()
                        .withHost("milvus")
                        .withPort(19530)
                        .build()
                );
                // 批量 insert
                List<Long> ids = new ArrayList<>();
                List<List<Float>> vectors = new ArrayList<>();
                
                partition.forEachRemaining(row -> {
                    ids.add(row.getLong(0));
                    vectors.add(Arrays.asList(row.<Float>getList(2)));
                });
                
                milvus.insert(InsertParam.newBuilder()
                    .withCollectionName("product_vectors")
                    .withFields(Arrays.asList(
                        new InsertParam.Field("product_id", ids),
                        new InsertParam.Field("embedding", vectors)
                    ))
                    .build());
                milvus.close();
            });
            
            // 4. 调度: Airflow 每天凌晨增量更新向量
            """;
        System.out.println(sparkEmbedding);
    }

    // ============================================================
    //  第3部分: RAG Pipeline 端到端
    // ============================================================
    private static void demonstrateRAGPipeline() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🤖 第3部分: RAG Pipeline 端到端");
        System.out.println("=".repeat(60));

        // 3.1 RAG 架构
        System.out.println("\n🏗️ 3.1 企业级 RAG 架构:");
        String ragArchitecture = """
            ┌─────────────────────────────────────────────────────────────────┐
            │                 企业级 RAG Pipeline 架构                         │
            ├─────────────────────────────────────────────────────────────────┤
            │                                                                 │
            │  用户提问: "最近3个月销量前10的手机有哪些？价格趋势如何？"           │
            │       │                                                         │
            │       ▼                                                         │
            │  ┌─────────────────────────────────────────────────────────┐    │
            │  │ ① Query Understanding (查询理解)                       │    │
            │  │   • 意图识别: 商品分析查询                              │    │
            │  │   • 实体抽取: 手机, 3个月, 销量Top10, 价格趋势          │    │
            │  │   • Query 改写: HyDE (生成假设文档) / Multi-Query       │    │
            │  └────────────────────────┬────────────────────────────────┘    │
            │                           │                                     │
            │       ┌───────────────────┼───────────────────┐                │
            │       ▼                   ▼                   ▼                │
            │  ┌─────────┐       ┌─────────┐         ┌─────────┐           │
            │  │ ② Dense │       │ ③ Sparse│         │④ SQL Gen│           │
            │  │  Search │       │  Search │         │  (NL2SQL)│           │
            │  │ (Milvus)│       │ (BM25)  │         │  (Doris) │           │
            │  └────┬────┘       └────┬────┘         └────┬────┘           │
            │       │                 │                    │                 │
            │       └─────────────────┤────────────────────┘                │
            │                         ▼                                      │
            │  ┌─────────────────────────────────────────────────────────┐   │
            │  │ ⑤ Fusion & Reranking (融合重排)                        │   │
            │  │   • RRF (Reciprocal Rank Fusion) 合并结果              │   │
            │  │   • Cross-Encoder Reranker (BGE-Reranker) 精排         │   │
            │  │   • 去重 + 相关性过滤 (score > threshold)               │   │
            │  └────────────────────────┬────────────────────────────────┘   │
            │                           ▼                                     │
            │  ┌─────────────────────────────────────────────────────────┐   │
            │  │ ⑥ Context Assembly (上下文组装)                         │   │
            │  │   • Chunk 合并 + 窗口扩展 (前后各1段)                   │   │
            │  │   • Metadata 注入 (数据源/更新时间/置信度)               │   │
            │  │   • Token 预算控制 (max 4096 tokens)                    │   │
            │  └────────────────────────┬────────────────────────────────┘   │
            │                           ▼                                     │
            │  ┌─────────────────────────────────────────────────────────┐   │
            │  │ ⑦ LLM Generation (大模型生成)                          │   │
            │  │   • System Prompt (角色 + 规则 + 格式)                  │   │
            │  │   • Context + Question → Answer                        │   │
            │  │   • Citation (引用来源标注)                              │   │
            │  │   • Hallucination Guard (幻觉检测)                      │   │
            │  └────────────────────────┬────────────────────────────────┘   │
            │                           ▼                                     │
            │  ┌─────────────────────────────────────────────────────────┐   │
            │  │ ⑧ Post-Processing (后处理)                              │   │
            │  │   • 事实核验 (与检索结果对比)                            │   │
            │  │   • 安全过滤 (PII/敏感信息)                              │   │
            │  │   • 反馈收集 (thumbs up/down → 优化)                    │   │
            │  └─────────────────────────────────────────────────────────┘   │
            └─────────────────────────────────────────────────────────────────┘
            """;
        System.out.println(ragArchitecture);

        // 3.2 RAG 核心代码实现
        System.out.println("\n💻 3.2 RAG Pipeline Java 实现:");
        String ragCode = """
            // ============================================================
            // 企业级 RAG Pipeline 核心实现
            // ============================================================
            
            public class EnterpriseRAGPipeline {
                
                private final MilvusServiceClient milvusClient;
                private final EmbeddingService embeddingService;
                private final LLMService llmService;
                private final RerankerService rerankerService;
                
                // ---- Step 1: Query 预处理 ----
                public QueryContext preprocessQuery(String userQuery) {
                    QueryContext ctx = new QueryContext();
                    ctx.originalQuery = userQuery;
                    
                    // HyDE: Hypothetical Document Embeddings
                    // 先让 LLM 生成假设性答案，用答案做检索（提升召回）
                    String hypotheticalDoc = llmService.generate(
                        "请基于以下问题，生成一段可能的答案文本（不需要真实数据）:\n"
                        + userQuery
                    );
                    ctx.hydeQuery = hypotheticalDoc;
                    
                    // Multi-Query: 生成多个查询变体
                    ctx.multiQueries = llmService.generateList(
                        "将以下问题改写为3个不同角度的搜索查询:\n" + userQuery
                    );
                    
                    return ctx;
                }
                
                // ---- Step 2: 多路召回 ----
                public List<RetrievedChunk> multiPathRetrieval(QueryContext ctx) {
                    List<RetrievedChunk> allChunks = new ArrayList<>();
                    
                    // 路径1: Dense 向量检索 (语义相似)
                    float[] queryEmbedding = embeddingService.embed(ctx.originalQuery);
                    List<RetrievedChunk> denseResults = milvusSearch(
                        queryEmbedding, "embedding", 20,
                        MetricType.COSINE, "{\"ef\": 128}"
                    );
                    denseResults.forEach(c -> c.source = "dense");
                    allChunks.addAll(denseResults);
                    
                    // 路径2: HyDE 向量检索 (假设文档)
                    float[] hydeEmbedding = embeddingService.embed(ctx.hydeQuery);
                    List<RetrievedChunk> hydeResults = milvusSearch(
                        hydeEmbedding, "embedding", 10,
                        MetricType.COSINE, "{\"ef\": 128}"
                    );
                    hydeResults.forEach(c -> c.source = "hyde");
                    allChunks.addAll(hydeResults);
                    
                    // 路径3: Sparse 检索 (关键词匹配，BM25)
                    List<RetrievedChunk> sparseResults = sparseSearch(
                        ctx.originalQuery, 10
                    );
                    sparseResults.forEach(c -> c.source = "sparse");
                    allChunks.addAll(sparseResults);
                    
                    return allChunks;
                }
                
                // ---- Step 3: 融合重排 ----
                public List<RetrievedChunk> fuseAndRerank(
                        String query, List<RetrievedChunk> chunks) {
                    
                    // RRF 融合 (Reciprocal Rank Fusion)
                    Map<String, Double> rrfScores = new HashMap<>();
                    int k = 60; // RRF 常数
                    
                    // 按 source 分组排名
                    Map<String, List<RetrievedChunk>> grouped = chunks.stream()
                        .collect(Collectors.groupingBy(c -> c.source));
                    
                    for (var entry : grouped.entrySet()) {
                        List<RetrievedChunk> sorted = entry.getValue().stream()
                            .sorted(Comparator.comparing(c -> -c.score))
                            .toList();
                        for (int i = 0; i < sorted.size(); i++) {
                            String id = sorted.get(i).chunkId;
                            rrfScores.merge(id, 1.0 / (k + i + 1), Double::sum);
                        }
                    }
                    
                    // 去重 + RRF 排序
                    List<RetrievedChunk> deduped = chunks.stream()
                        .collect(Collectors.toMap(
                            c -> c.chunkId, c -> c, (a, b) -> a))
                        .values().stream()
                        .sorted(Comparator.comparing(
                            c -> -rrfScores.getOrDefault(c.chunkId, 0.0)))
                        .limit(20)
                        .toList();
                    
                    // Cross-Encoder Reranker 精排
                    return rerankerService.rerank(query, deduped, 5);
                }
                
                // ---- Step 4: LLM 生成 ----
                public RAGResponse generate(String query,
                                            List<RetrievedChunk> contexts) {
                    
                    String systemPrompt = \"\"\"
                        你是一个专业的数据分析助手。
                        规则:
                        1. 只基于提供的上下文回答，不要编造数据
                        2. 如果上下文不足以回答，明确说明
                        3. 引用来源时使用 [来源X] 格式
                        4. 数据相关的回答需包含具体数字
                        5. 如果涉及趋势分析，请说明时间范围
                    \"\"\";
                    
                    StringBuilder contextStr = new StringBuilder();
                    for (int i = 0; i < contexts.size(); i++) {
                        contextStr.append(String.format(
                            "[来源%d] (来自: %s, 更新: %s, 置信度: %.2f)\n%s\n\n",
                            i + 1,
                            contexts.get(i).metadata.get("source"),
                            contexts.get(i).metadata.get("updated_at"),
                            contexts.get(i).score,
                            contexts.get(i).content
                        ));
                    }
                    
                    String userPrompt = String.format(
                        "上下文信息:\n%s\n\n用户问题: %s\n\n请基于上下文回答:",
                        contextStr, query
                    );
                    
                    String answer = llmService.chat(systemPrompt, userPrompt);
                    
                    return new RAGResponse(
                        answer,
                        contexts,                      // 引用来源
                        detectHallucination(answer, contexts)  // 幻觉检测
                    );
                }
            }
            """;
        System.out.println(ragCode);

        // 3.3 RAG 评估体系
        System.out.println("\n📊 3.3 RAG 评估指标 (RAGAS):");
        String ragEvaluation = """
            ┌──────────────────────────────────────────────────────────────┐
            │                    RAG 评估指标 (RAGAS)                       │
            ├─────────────────┬────────────────────────────────────────────┤
            │ 指标             │ 说明                                      │
            ├─────────────────┼────────────────────────────────────────────┤
            │ Faithfulness    │ 回答是否忠实于检索上下文 (无幻觉)             │
            │                 │ 目标: > 0.90                               │
            │                 │                                            │
            │ Answer Relevancy│ 回答是否与问题相关                           │
            │                 │ 目标: > 0.85                               │
            │                 │                                            │
            │ Context Recall  │ 检索是否召回了所有需要的信息                   │
            │                 │ 目标: > 0.80                               │
            │                 │                                            │
            │ Context Precision│ 检索到的文档中有多少真正相关                 │
            │                 │ 目标: > 0.75                               │
            │                 │                                            │
            │ Answer Correctness│ 回答的事实正确性                          │
            │                 │ 目标: > 0.85                               │
            ├─────────────────┼────────────────────────────────────────────┤
            │ 系统级指标        │                                            │
            ├─────────────────┼────────────────────────────────────────────┤
            │ Latency P95     │ 端到端延迟 < 3s                            │
            │ Token Cost      │ 平均每次查询消耗 < 2000 tokens              │
            │ Throughput       │ > 50 QPS (并发)                            │
            └─────────────────┴────────────────────────────────────────────┘
            
            # 评估 Pipeline (Python):
            # from ragas import evaluate
            # from ragas.metrics import faithfulness, answer_relevancy, ...
            #
            # result = evaluate(
            #     dataset=eval_dataset,
            #     metrics=[faithfulness, answer_relevancy,
            #              context_recall, context_precision],
            #     llm=ChatOpenAI(model="gpt-4"),
            #     embeddings=OpenAIEmbeddings()
            # )
            """;
        System.out.println(ragEvaluation);
    }

    // ============================================================
    //  第4部分: 混合搜索策略
    // ============================================================
    private static void demonstrateHybridSearch() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🔍 第4部分: 混合搜索策略");
        System.out.println("=".repeat(60));

        String hybridSearch = """
            ┌──────────────────────────────────────────────────────────────┐
            │                   混合搜索策略对比                             │
            ├──────────────┬───────────────────────────────────────────────┤
            │ 策略          │ 说明                                         │
            ├──────────────┼───────────────────────────────────────────────┤
            │ Dense Only   │ 纯语义搜索，擅长同义词/改述                     │
            │              │ 弱点: 精确关键词匹配差                          │
            │              │                                               │
            │ Sparse Only  │ 纯关键词搜索 (BM25)，精确匹配强                │
            │              │ 弱点: 语义理解差                               │
            │              │                                               │
            │ Dense+Sparse │ 混合搜索 = 语义 + 关键词                       │
            │ (推荐)       │ RRF 融合，互补优势，召回率最高                   │
            │              │                                               │
            │ +Reranker    │ 二阶段: 粗排(100) → 精排(10)                  │
            │ (最佳)       │ Cross-Encoder 高精度重排                       │
            │              │                                               │
            │ +Metadata    │ 标量过滤 + 向量搜索                            │
            │              │ 如: category='手机' AND price<5000            │
            └──────────────┴───────────────────────────────────────────────┘
            
            // Milvus 混合搜索示例 (Java):
            
            // 1. Dense + Metadata Filter
            SearchParam hybridSearch = SearchParam.newBuilder()
                .withCollectionName("product_vectors")
                .withVectorFieldName("embedding")
                .withVectors(Arrays.asList(queryVector))
                .withTopK(100)
                .withMetricType(MetricType.COSINE)
                .withExpr("category IN ['手机', '平板'] AND price BETWEEN 1000 AND 8000")
                .withOutFields(Arrays.asList("product_name", "category", "price"))
                .withParams("{\"ef\": 256}")
                .build();
            
            // 2. Multi-Vector 搜索 (Milvus 2.4+)
            // 同时搜索 Dense + Sparse 向量
            AnnSearchParam denseSearch = AnnSearchParam.newBuilder()
                .withFieldName("embedding")
                .withVectors(Arrays.asList(denseVector))
                .withMetricType(MetricType.COSINE)
                .withParams("{\"ef\": 128}")
                .withTopK(50)
                .build();
            
            AnnSearchParam sparseSearch = AnnSearchParam.newBuilder()
                .withFieldName("sparse_embedding")
                .withVectors(Arrays.asList(sparseVector))
                .withMetricType(MetricType.IP)
                .withTopK(50)
                .build();
            
            HybridSearchParam hybridParam = HybridSearchParam.newBuilder()
                .withCollectionName("product_vectors")
                .addSearchRequest(denseSearch)
                .addSearchRequest(sparseSearch)
                .withRanker(new RRFRanker(60))       // RRF 融合
                .withTopK(20)
                .withOutFields(Arrays.asList("product_name", "price"))
                .build();
            
            SearchResults results = milvusClient.hybridSearch(hybridParam);
            """;
        System.out.println(hybridSearch);
    }

    // ============================================================
    //  第5部分: 向量索引深度
    // ============================================================
    private static void demonstrateVectorIndex() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("📐 第5部分: 向量索引策略");
        System.out.println("=".repeat(60));

        String vectorIndex = """
            ┌──────────────────────────────────────────────────────────────────────┐
            │                    向量索引策略选型指南                                 │
            ├───────────┬────────┬────────┬──────────┬──────────┬────────────────┤
            │ 索引类型    │ 构建速度│ 查询速度│ 内存占用   │ 召回率   │ 适用场景        │
            ├───────────┼────────┼────────┼──────────┼──────────┼────────────────┤
            │ FLAT      │ -      │ 最慢   │ 100%原始 │ 100%    │ <10万 / 精确   │
            │ IVF_FLAT  │ 中等   │ 快     │ 100%原始 │ 95%+    │ 10万~500万     │
            │ IVF_SQ8   │ 中等   │ 快     │ ~25%    │ 90%+    │ 内存受限        │
            │ IVF_PQ    │ 慢     │ 最快   │ ~10%    │ 85%+    │ 超大规模低内存  │
            │ HNSW      │ 慢     │ 最快   │ 120%    │ 99%+    │ 生产推荐 (全内存)│
            │ DiskANN   │ 最慢   │ 快     │ ~5%     │ 95%+    │ 10亿+磁盘存储  │
            │ SCANN     │ 中等   │ 很快   │ ~50%    │ 97%+    │ Google 方案    │
            └───────────┴────────┴────────┴──────────┴──────────┴────────────────┘
            
            推荐组合:
            ┌──────────────────────────────────────────────────────────────┐
            │ 数据规模         │ 推荐索引          │ 配置                   │
            ├─────────────────┼─────────────────┼────────────────────────┤
            │ < 100万         │ HNSW            │ M=16, ef=256           │
            │ 100万 ~ 1000万  │ IVF_FLAT + HNSW │ nlist=4096, ef=128    │
            │ 1000万 ~ 1亿    │ IVF_PQ          │ nlist=16384, m=16     │
            │ > 1亿           │ DiskANN         │ search_list=128       │
            │ > 10亿          │ DiskANN + 分片   │ 多 Collection 分片    │
            └─────────────────┴─────────────────┴────────────────────────┘
            
            // HNSW 参数调优:
            // M (连接数): 16~64, 越大召回越高但内存占用也越大
            // efConstruction (构建精度): 128~512, 影响构建质量
            // ef (搜索精度): 64~512, 越大越准但越慢
            
            // 生产配置示例:
            CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName("product_vectors")
                .withFieldName("embedding")
                .withIndexType(IndexType.HNSW)
                .withMetricType(MetricType.COSINE)
                .withExtraParam(new JsonObject()
                    .addProperty("M", 32)                  // 每层连接数
                    .addProperty("efConstruction", 360)     // 构建精度
                    .toString())
                .build();
            """;
        System.out.println(vectorIndex);
    }

    // ============================================================
    //  第6部分: 大数据 + AI 融合架构
    // ============================================================
    private static void demonstrateBigDataAIFusion() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🔗 第6部分: 大数据 + AI 融合架构");
        System.out.println("=".repeat(60));

        String fusionArch = """
            ┌─────────────────────────────────────────────────────────────────┐
            │               大数据 + AI 融合架构 (Lakehouse + RAG)             │
            ├─────────────────────────────────────────────────────────────────┤
            │                                                                 │
            │  数据源层                                                        │
            │  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐                 │
            │  │ MySQL  │ │ Kafka  │ │ Log    │ │ API    │                 │
            │  │ (CDC)  │ │(Events)│ │(Files) │ │(REST)  │                 │
            │  └───┬────┘ └───┬────┘ └───┬────┘ └───┬────┘                 │
            │      └──────────┴──────────┴──────────┘                        │
            │                      │                                          │
            │  ┌───────────────────┴───────────────────────────────────┐     │
            │  │              数据湖 / 数据仓库层                        │     │
            │  │  ┌──────────────────────────────────────────────┐     │     │
            │  │  │ Delta Lake / Hudi / Iceberg (数据湖)         │     │     │
            │  │  │ Doris / StarRocks (OLAP)                     │     │     │
            │  │  │ Hive (数仓)                                  │     │     │
            │  │  └──────────────────────────────────────────────┘     │     │
            │  └───────────────────┬───────────────────────────────────┘     │
            │                      │                                          │
            │      ┌───────────────┼───────────────┐                         │
            │      ▼               ▼               ▼                         │
            │  ┌─────────┐  ┌─────────────┐  ┌─────────────┐                │
            │  │ 向量化   │  │ 特征工程    │  │ 知识图谱     │                │
            │  │ Pipeline │  │ (Feast)     │  │ (Neo4j)     │                │
            │  │          │  │             │  │             │                │
            │  │ Spark    │  │ 离线特征    │  │ 实体关系    │                │
            │  │ Embedding│  │ 在线特征    │  │ 推理查询    │                │
            │  │ → Milvus │  │ → Redis     │  │ → GraphQL   │                │
            │  └────┬─────┘  └──────┬──────┘  └──────┬──────┘                │
            │       │               │                │                        │
            │       └───────────────┼────────────────┘                        │
            │                       ▼                                         │
            │  ┌──────────────────────────────────────────────────────┐      │
            │  │              AI 应用层                                │      │
            │  │  ┌──────────┐ ┌──────────┐ ┌──────────┐            │      │
            │  │  │ RAG      │ │ 推荐系统  │ │ 风控系统  │            │      │
            │  │  │ (智能问答)│ │ (个性化) │ │ (实时)   │            │      │
            │  │  └──────────┘ └──────────┘ └──────────┘            │      │
            │  │  ┌──────────┐ ┌──────────┐ ┌──────────┐            │      │
            │  │  │ Agent    │ │ 数据分析  │ │ 异常检测  │            │      │
            │  │  │ (AI代理)  │ │ (BI Chat)│ │ (ML)    │            │      │
            │  │  └──────────┘ └──────────┘ └──────────┘            │      │
            │  └──────────────────────────────────────────────────────┘      │
            │                                                                 │
            │  ┌──────────────────────────────────────────────────────┐      │
            │  │              可观测性层                                │      │
            │  │  OpenLineage │ Marquez │ Prometheus │ Grafana        │      │
            │  └──────────────────────────────────────────────────────┘      │
            └─────────────────────────────────────────────────────────────────┘
            
            典型数据流:
            ① 实时推荐: 用户行为 → Kafka → Flink → Feature Store → 推荐模型
            ② 智能客服: 用户提问 → Embedding → Milvus检索 → LLM生成回答
            ③ 数据分析: "上月GMV?" → NL2SQL → Doris查询 → 可视化展示
            ④ 异常检测: 订单数据 → Spark MLlib → 异常评分 → 告警通知
            """;
        System.out.println(fusionArch);
    }
}
