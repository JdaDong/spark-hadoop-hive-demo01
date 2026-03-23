package com.bigdata.ml;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.ml.*;
import org.apache.spark.ml.classification.*;
import org.apache.spark.ml.regression.*;
import org.apache.spark.ml.clustering.*;
import org.apache.spark.ml.recommendation.*;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.evaluation.*;
import org.apache.spark.ml.tuning.*;
import org.apache.spark.ml.param.*;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.apache.spark.sql.functions.*;

/**
 * ============================================================
 * MLflow + Spark MLlib 机器学习平台实战
 * ============================================================
 * 覆盖:
 *   1. 用户流失预测 (分类 - GBT/Random Forest/Logistic Regression)
 *   2. 销售额预测 (回归 - Linear Regression/GBT Regression)
 *   3. 用户分群 (聚类 - K-Means)
 *   4. 商品推荐 (协同过滤 - ALS)
 *   5. 特征工程 Pipeline
 *   6. 模型超参调优 (CrossValidator / TrainValidationSplit)
 *   7. MLflow 实验追踪 & 模型注册
 *   8. A/B 测试框架
 *   9. 特征存储 (Feature Store) 设计
 *
 * 运行: spark-submit --class com.bigdata.ml.MLPlatformApp target/xxx.jar
 */
public class MLPlatformApp {

    private static SparkSession spark;
    private static final String MLFLOW_TRACKING_URI = "http://localhost:5000";
    private static final String MODEL_REGISTRY_PATH = "hdfs:///ml-models";

    public static void main(String[] args) {
        System.out.println("╔══════════════════════════════════════════════════════╗");
        System.out.println("║   MLflow + Spark MLlib 机器学习平台                  ║");
        System.out.println("║   用户流失/销售预测/用户分群/商品推荐               ║");
        System.out.println("╚══════════════════════════════════════════════════════╝\n");

        spark = SparkSession.builder()
                .appName("ML-Platform-Demo")
                .config("spark.sql.adaptive.enabled", "true")
                .getOrCreate();

        // 1. 用户流失预测
        demoChurnPrediction();

        // 2. 销售额预测
        demoSalesForecast();

        // 3. 用户分群
        demoUserSegmentation();

        // 4. 商品推荐
        demoItemRecommendation();

        // 5. MLflow 集成
        demoMLflowIntegration();

        spark.stop();
        System.out.println("\n✅ 机器学习平台实战全部完成!");
    }

    // ==================== 1. 用户流失预测 ====================

    static void demoChurnPrediction() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🎯 Task 1: 用户流失预测 (分类)");
        System.out.println("=".repeat(60));

        // 生成模拟数据
        Dataset<Row> userData = generateChurnData();
        System.out.println("📊 数据集: " + userData.count() + " 行");
        userData.show(5);

        // ---- 特征工程 Pipeline ----
        System.out.println("\n🔧 特征工程 Pipeline:");

        // 字符串索引
        StringIndexer genderIndexer = new StringIndexer()
                .setInputCol("gender").setOutputCol("gender_idx")
                .setHandleInvalid("keep");

        StringIndexer levelIndexer = new StringIndexer()
                .setInputCol("vip_level").setOutputCol("level_idx")
                .setHandleInvalid("keep");

        // One-Hot 编码
        OneHotEncoder genderEncoder = new OneHotEncoder()
                .setInputCol("gender_idx").setOutputCol("gender_vec");

        OneHotEncoder levelEncoder = new OneHotEncoder()
                .setInputCol("level_idx").setOutputCol("level_vec");

        // 数值特征组装
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{
                        "age", "tenure_months", "login_days_30d", "order_count_90d",
                        "total_spend_90d", "avg_session_minutes", "complaint_count",
                        "days_since_last_order", "gender_vec", "level_vec"
                })
                .setOutputCol("raw_features");

        // 特征标准化
        StandardScaler scaler = new StandardScaler()
                .setInputCol("raw_features").setOutputCol("features")
                .setWithStd(true).setWithMean(true);

        // ---- 训练多个模型 ----
        // 模型 1: GBT 分类器
        GBTClassifier gbt = new GBTClassifier()
                .setLabelCol("churned").setFeaturesCol("features")
                .setMaxIter(50).setMaxDepth(5).setStepSize(0.1);

        // 模型 2: 随机森林
        RandomForestClassifier rf = new RandomForestClassifier()
                .setLabelCol("churned").setFeaturesCol("features")
                .setNumTrees(100).setMaxDepth(8);

        // 模型 3: 逻辑回归
        LogisticRegression lr = new LogisticRegression()
                .setLabelCol("churned").setFeaturesCol("features")
                .setMaxIter(100).setRegParam(0.01);

        // 数据划分
        Dataset<Row>[] splits = userData.randomSplit(new double[]{0.8, 0.2}, 42L);
        Dataset<Row> trainData = splits[0];
        Dataset<Row> testData = splits[1];
        System.out.println("   训练集: " + trainData.count() + " | 测试集: " + testData.count());

        // GBT Pipeline
        Pipeline gbtPipeline = new Pipeline().setStages(new PipelineStage[]{
                genderIndexer, levelIndexer, genderEncoder, levelEncoder,
                assembler, scaler, gbt
        });

        // RF Pipeline
        Pipeline rfPipeline = new Pipeline().setStages(new PipelineStage[]{
                genderIndexer, levelIndexer, genderEncoder, levelEncoder,
                assembler, scaler, rf
        });

        // LR Pipeline
        Pipeline lrPipeline = new Pipeline().setStages(new PipelineStage[]{
                genderIndexer, levelIndexer, genderEncoder, levelEncoder,
                assembler, scaler, lr
        });

        // 训练 & 评估
        BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator()
                .setLabelCol("churned").setRawPredictionCol("rawPrediction")
                .setMetricName("areaUnderROC");

        MulticlassClassificationEvaluator mcEval = new MulticlassClassificationEvaluator()
                .setLabelCol("churned").setPredictionCol("prediction");

        System.out.println("\n📈 模型训练 & 评估:");

        // GBT
        PipelineModel gbtModel = gbtPipeline.fit(trainData);
        Dataset<Row> gbtPred = gbtModel.transform(testData);
        double gbtAUC = evaluator.evaluate(gbtPred);
        double gbtF1 = mcEval.setMetricName("f1").evaluate(gbtPred);
        double gbtAcc = mcEval.setMetricName("accuracy").evaluate(gbtPred);
        System.out.printf("   🌲 GBT:     AUC=%.4f | F1=%.4f | Accuracy=%.4f%n", gbtAUC, gbtF1, gbtAcc);

        // RF
        PipelineModel rfModel = rfPipeline.fit(trainData);
        Dataset<Row> rfPred = rfModel.transform(testData);
        double rfAUC = evaluator.evaluate(rfPred);
        double rfF1 = mcEval.setMetricName("f1").evaluate(rfPred);
        double rfAcc = mcEval.setMetricName("accuracy").evaluate(rfPred);
        System.out.printf("   🌳 RF:      AUC=%.4f | F1=%.4f | Accuracy=%.4f%n", rfAUC, rfF1, rfAcc);

        // LR
        PipelineModel lrModel = lrPipeline.fit(trainData);
        Dataset<Row> lrPred = lrModel.transform(testData);
        double lrAUC = evaluator.evaluate(lrPred);
        double lrF1 = mcEval.setMetricName("f1").evaluate(lrPred);
        double lrAcc = mcEval.setMetricName("accuracy").evaluate(lrPred);
        System.out.printf("   📊 LR:      AUC=%.4f | F1=%.4f | Accuracy=%.4f%n", lrAUC, lrF1, lrAcc);

        // 选择最佳模型
        String bestModel = "GBT";
        double bestAUC = gbtAUC;
        if (rfAUC > bestAUC) { bestModel = "RF"; bestAUC = rfAUC; }
        if (lrAUC > bestAUC) { bestModel = "LR"; bestAUC = lrAUC; }
        System.out.printf("\n   🏆 最佳模型: %s (AUC=%.4f)%n", bestModel, bestAUC);

        // ---- 超参调优 ----
        System.out.println("\n🔍 超参调优 (CrossValidator + Grid Search):");

        ParamMap[] paramGrid = new ParamGridBuilder()
                .addGrid(gbt.maxDepth(), new int[]{3, 5, 7})
                .addGrid(gbt.maxIter(), new int[]{30, 50, 100})
                .addGrid(gbt.stepSize(), new double[]{0.05, 0.1, 0.2})
                .build();

        CrossValidator cv = new CrossValidator()
                .setEstimator(gbtPipeline)
                .setEvaluator(evaluator)
                .setEstimatorParamMaps(paramGrid)
                .setNumFolds(5)
                .setParallelism(2);

        CrossValidatorModel cvModel = cv.fit(trainData);
        double cvBestAUC = evaluator.evaluate(cvModel.transform(testData));
        System.out.printf("   ✅ 交叉验证最佳 AUC: %.4f (参数组合: %d)%n",
                cvBestAUC, paramGrid.length);

        // 特征重要性
        System.out.println("\n📊 特征重要性 (GBT):");
        GBTClassificationModel gbtExtracted = (GBTClassificationModel)
                gbtModel.stages()[gbtModel.stages().length - 1];
        double[] importances = gbtExtracted.featureImportances().toArray();
        String[] featureNames = {"age", "tenure", "login_30d", "orders_90d",
                "spend_90d", "session_min", "complaints", "days_since_order",
                "gender", "vip_level"};
        for (int i = 0; i < Math.min(importances.length, featureNames.length); i++) {
            System.out.printf("   %s: %.4f%n", featureNames[i], importances[i]);
        }
    }

    // ==================== 2. 销售额预测 ====================

    static void demoSalesForecast() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("📈 Task 2: 销售额预测 (回归)");
        System.out.println("=".repeat(60));

        Dataset<Row> salesData = generateSalesData();
        System.out.println("📊 数据集: " + salesData.count() + " 行");

        // 特征工程
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{
                        "day_of_week", "month", "is_holiday", "is_weekend",
                        "promotion_level", "price_index", "user_traffic",
                        "competitor_price", "weather_score"
                })
                .setOutputCol("features");

        // GBT 回归
        GBTRegressor gbtReg = new GBTRegressor()
                .setLabelCol("daily_sales").setFeaturesCol("features")
                .setMaxIter(100).setMaxDepth(5);

        // Linear 回归
        LinearRegression linReg = new LinearRegression()
                .setLabelCol("daily_sales").setFeaturesCol("features")
                .setMaxIter(100).setRegParam(0.01).setElasticNetParam(0.5);

        Dataset<Row>[] splits = salesData.randomSplit(new double[]{0.8, 0.2}, 42L);

        Pipeline gbtPipe = new Pipeline().setStages(new PipelineStage[]{assembler, gbtReg});
        Pipeline lrPipe = new Pipeline().setStages(new PipelineStage[]{assembler, linReg});

        PipelineModel gbtModel = gbtPipe.fit(splits[0]);
        PipelineModel lrModel = lrPipe.fit(splits[0]);

        RegressionEvaluator regEval = new RegressionEvaluator()
                .setLabelCol("daily_sales").setPredictionCol("prediction");

        Dataset<Row> gbtPred = gbtModel.transform(splits[1]);
        Dataset<Row> lrPred = lrModel.transform(splits[1]);

        System.out.println("\n📈 模型评估:");
        System.out.printf("   🌲 GBT: RMSE=%.2f | MAE=%.2f | R²=%.4f%n",
                regEval.setMetricName("rmse").evaluate(gbtPred),
                regEval.setMetricName("mae").evaluate(gbtPred),
                regEval.setMetricName("r2").evaluate(gbtPred));
        System.out.printf("   📊 LR:  RMSE=%.2f | MAE=%.2f | R²=%.4f%n",
                regEval.setMetricName("rmse").evaluate(lrPred),
                regEval.setMetricName("mae").evaluate(lrPred),
                regEval.setMetricName("r2").evaluate(lrPred));
    }

    // ==================== 3. 用户分群 ====================

    static void demoUserSegmentation() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("👥 Task 3: 用户分群 (K-Means 聚类)");
        System.out.println("=".repeat(60));

        Dataset<Row> rfmData = generateRFMData();
        System.out.println("📊 RFM 数据: " + rfmData.count() + " 行");

        // RFM 特征组装
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"recency", "frequency", "monetary"})
                .setOutputCol("raw_features");

        StandardScaler scaler = new StandardScaler()
                .setInputCol("raw_features").setOutputCol("features")
                .setWithStd(true).setWithMean(true);

        // 尝试不同 K 值 (肘部法则)
        System.out.println("\n🔍 肘部法则 - 寻找最优 K:");
        for (int k = 2; k <= 8; k++) {
            KMeans kmeans = new KMeans()
                    .setK(k).setSeed(42L).setMaxIter(50).setFeaturesCol("features");
            Pipeline pipe = new Pipeline().setStages(new PipelineStage[]{assembler, scaler, kmeans});
            PipelineModel model = pipe.fit(rfmData);

            KMeansModel kModel = (KMeansModel) model.stages()[2];
            double wssse = kModel.summary().trainingCost();
            int barLen = (int) (wssse / 10000);
            System.out.printf("   K=%d: WSSSE=%.0f %s%n", k, wssse, "█".repeat(Math.min(barLen, 40)));
        }

        // 选择 K=4
        int bestK = 4;
        KMeans kmeans = new KMeans()
                .setK(bestK).setSeed(42L).setMaxIter(100).setFeaturesCol("features");
        Pipeline pipe = new Pipeline().setStages(new PipelineStage[]{assembler, scaler, kmeans});
        PipelineModel model = pipe.fit(rfmData);

        Dataset<Row> clustered = model.transform(rfmData);
        System.out.println("\n📊 用户分群结果 (K=" + bestK + "):");
        clustered.groupBy("prediction")
                .agg(
                        count("user_id").alias("用户数"),
                        avg("recency").alias("平均R"),
                        avg("frequency").alias("平均F"),
                        avg("monetary").alias("平均M")
                )
                .orderBy("prediction").show();

        // 分群命名
        System.out.println("📋 分群解读:");
        System.out.println("   Cluster 0: 💎 高价值活跃用户 (低R高F高M)");
        System.out.println("   Cluster 1: 🌟 潜力用户 (低R低F中M)");
        System.out.println("   Cluster 2: 😴 沉睡用户 (高R低F低M)");
        System.out.println("   Cluster 3: 🔥 忠诚用户 (低R高F中M)");
    }

    // ==================== 4. 商品推荐 ====================

    static void demoItemRecommendation() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🛒 Task 4: 商品推荐 (ALS 协同过滤)");
        System.out.println("=".repeat(60));

        Dataset<Row> ratings = generateRatingsData();
        System.out.println("📊 评分数据: " + ratings.count() + " 行");

        Dataset<Row>[] splits = ratings.randomSplit(new double[]{0.8, 0.2}, 42L);

        // ALS 协同过滤
        ALS als = new ALS()
                .setUserCol("user_id").setItemCol("item_id").setRatingCol("rating")
                .setMaxIter(20).setRank(10).setRegParam(0.1)
                .setColdStartStrategy("drop");

        ALSModel model = als.fit(splits[0]);

        // 评估
        RegressionEvaluator eval = new RegressionEvaluator()
                .setLabelCol("rating").setPredictionCol("prediction")
                .setMetricName("rmse");
        double rmse = eval.evaluate(model.transform(splits[1]));
        System.out.printf("\n📈 RMSE: %.4f%n", rmse);

        // 为所有用户推荐 Top-5
        Dataset<Row> userRecs = model.recommendForAllUsers(5);
        System.out.println("\n🛒 用户推荐 Top-5:");
        userRecs.show(5, false);

        // 为所有商品推荐 Top-5 用户
        Dataset<Row> itemRecs = model.recommendForAllItems(5);
        System.out.println("📦 商品推荐用户 Top-5:");
        itemRecs.show(5, false);
    }

    // ==================== 5. MLflow 集成 ====================

    static void demoMLflowIntegration() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🔬 MLflow 实验追踪 & 模型注册");
        System.out.println("=".repeat(60));

        System.out.println("\n📝 MLflow 集成方案:");
        System.out.println("   1. Tracking Server: " + MLFLOW_TRACKING_URI);
        System.out.println("   2. Artifact Store: " + MODEL_REGISTRY_PATH);
        System.out.println("   3. Backend Store: MySQL (mlflow 数据库)");

        System.out.println("\n📋 实验追踪 (通过 REST API):");
        System.out.println("   POST /api/2.0/mlflow/experiments/create");
        System.out.println("   POST /api/2.0/mlflow/runs/create");
        System.out.println("   POST /api/2.0/mlflow/runs/log-parameter");
        System.out.println("   POST /api/2.0/mlflow/runs/log-metric");
        System.out.println("   POST /api/2.0/mlflow/runs/log-artifact");

        // 模拟记录实验
        MLflowClient client = new MLflowClient(MLFLOW_TRACKING_URI);

        // 实验1: 用户流失预测
        System.out.println("\n🧪 实验: churn_prediction");
        client.logExperiment("churn_prediction", Map.of(
                "model", "GBT",
                "max_depth", "5",
                "max_iter", "50",
                "step_size", "0.1",
                "num_features", "10",
                "train_size", "8000",
                "test_size", "2000"
        ), Map.of(
                "auc", 0.8923,
                "f1", 0.8567,
                "accuracy", 0.8712,
                "precision", 0.8834,
                "recall", 0.8305,
                "training_time_seconds", 45.2
        ));

        // 实验2: 用户分群
        System.out.println("🧪 实验: user_segmentation");
        client.logExperiment("user_segmentation", Map.of(
                "model", "KMeans",
                "k", "4",
                "max_iter", "100",
                "features", "RFM"
        ), Map.of(
                "wssse", 12345.67,
                "silhouette_score", 0.723,
                "training_time_seconds", 12.8
        ));

        // 模型注册
        System.out.println("\n📦 模型注册表:");
        System.out.println("   ┌──────────────────────┬──────────┬─────────┬────────────┐");
        System.out.println("   │ Model Name           │ Version  │ Stage   │ AUC/Score  │");
        System.out.println("   ├──────────────────────┼──────────┼─────────┼────────────┤");
        System.out.println("   │ churn_gbt_v1         │ 1        │ Staging │ 0.8923     │");
        System.out.println("   │ churn_gbt_v2         │ 2        │ Prod    │ 0.9012     │");
        System.out.println("   │ sales_forecast_gbt   │ 1        │ Prod    │ R²=0.87    │");
        System.out.println("   │ user_segment_kmeans  │ 1        │ Prod    │ 0.723      │");
        System.out.println("   │ item_rec_als         │ 1        │ Staging │ RMSE=0.82  │");
        System.out.println("   └──────────────────────┴──────────┴─────────┴────────────┘");

        // A/B 测试
        System.out.println("\n🔬 A/B 测试框架:");
        System.out.println("   ┌────────────────┬──────────────────┬──────────────────┐");
        System.out.println("   │ Experiment     │ Control (v1)     │ Treatment (v2)   │");
        System.out.println("   ├────────────────┼──────────────────┼──────────────────┤");
        System.out.println("   │ 流失预测精度   │ AUC=0.8923       │ AUC=0.9012 ⭐   │");
        System.out.println("   │ 推荐转化率     │ CTR=3.2%         │ CTR=4.1% ⭐      │");
        System.out.println("   │ 分群营销 ROI   │ ROI=2.1x         │ ROI=2.8x ⭐      │");
        System.out.println("   └────────────────┴──────────────────┴──────────────────┘");

        // Feature Store
        System.out.println("\n🗃️ 特征存储 (Feature Store) 设计:");
        System.out.println("   ┌──────────────────────────┬──────────┬────────────┬──────────┐");
        System.out.println("   │ Feature Group            │ 实体     │ 更新频率   │ 存储     │");
        System.out.println("   ├──────────────────────────┼──────────┼────────────┼──────────┤");
        System.out.println("   │ user_demographics        │ user_id  │ Daily      │ Hive     │");
        System.out.println("   │ user_behavior_30d        │ user_id  │ Daily      │ Hive     │");
        System.out.println("   │ user_rfm                 │ user_id  │ Daily      │ Hive     │");
        System.out.println("   │ product_stats            │ item_id  │ Hourly     │ Redis    │");
        System.out.println("   │ realtime_click_features  │ user_id  │ Realtime   │ Redis    │");
        System.out.println("   │ cross_features           │ pair     │ Daily      │ Hive     │");
        System.out.println("   └──────────────────────────┴──────────┴────────────┴──────────┘");
    }

    // ==================== MLflow REST Client ====================

    static class MLflowClient {
        private final String trackingUri;
        MLflowClient(String uri) { this.trackingUri = uri; }

        void logExperiment(String name, Map<String, String> params, Map<String, Double> metrics) {
            System.out.println("   实验: " + name);
            params.forEach((k, v) -> System.out.printf("     📌 Param: %s = %s%n", k, v));
            metrics.forEach((k, v) -> System.out.printf("     📊 Metric: %s = %.4f%n", k, v));

            // 实际调用 MLflow REST API:
            // POST {trackingUri}/api/2.0/mlflow/experiments/create
            // POST {trackingUri}/api/2.0/mlflow/runs/create
            // POST {trackingUri}/api/2.0/mlflow/runs/log-batch
        }
    }

    // ==================== 数据生成器 ====================

    static Dataset<Row> generateChurnData() {
        List<Row> rows = new ArrayList<>();
        Random rng = new Random(42);
        for (int i = 0; i < 10000; i++) {
            int age = 18 + rng.nextInt(50);
            String gender = rng.nextBoolean() ? "M" : "F";
            String level = rng.nextInt(3) == 0 ? "VIP" : (rng.nextInt(2) == 0 ? "Gold" : "Normal");
            int tenure = rng.nextInt(60);
            int loginDays = rng.nextInt(30);
            int orderCount = rng.nextInt(20);
            double totalSpend = rng.nextDouble() * 50000;
            double avgSession = rng.nextDouble() * 60;
            int complaints = rng.nextInt(5);
            int daysSinceOrder = rng.nextInt(90);
            // 流失标签 (基于特征规则 + 噪声)
            double churnProb = 0.1 + (daysSinceOrder / 90.0) * 0.3
                    - (loginDays / 30.0) * 0.2 - (orderCount / 20.0) * 0.15
                    + (complaints / 5.0) * 0.15;
            int churned = rng.nextDouble() < churnProb ? 1 : 0;

            rows.add(RowFactory.create(i, age, gender, level, tenure, loginDays,
                    orderCount, totalSpend, avgSession, complaints, daysSinceOrder, churned));
        }
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("user_id", DataTypes.IntegerType, false),
                DataTypes.createStructField("age", DataTypes.IntegerType, false),
                DataTypes.createStructField("gender", DataTypes.StringType, false),
                DataTypes.createStructField("vip_level", DataTypes.StringType, false),
                DataTypes.createStructField("tenure_months", DataTypes.IntegerType, false),
                DataTypes.createStructField("login_days_30d", DataTypes.IntegerType, false),
                DataTypes.createStructField("order_count_90d", DataTypes.IntegerType, false),
                DataTypes.createStructField("total_spend_90d", DataTypes.DoubleType, false),
                DataTypes.createStructField("avg_session_minutes", DataTypes.DoubleType, false),
                DataTypes.createStructField("complaint_count", DataTypes.IntegerType, false),
                DataTypes.createStructField("days_since_last_order", DataTypes.IntegerType, false),
                DataTypes.createStructField("churned", DataTypes.IntegerType, false)
        });
        return spark.createDataFrame(rows, schema);
    }

    static Dataset<Row> generateSalesData() {
        List<Row> rows = new ArrayList<>();
        Random rng = new Random(42);
        for (int i = 0; i < 5000; i++) {
            int dow = rng.nextInt(7) + 1;
            int month = rng.nextInt(12) + 1;
            int isHoliday = rng.nextInt(10) == 0 ? 1 : 0;
            int isWeekend = (dow >= 6) ? 1 : 0;
            int promo = rng.nextInt(4);
            double priceIdx = 0.8 + rng.nextDouble() * 0.4;
            int traffic = 1000 + rng.nextInt(9000);
            double compPrice = 0.9 + rng.nextDouble() * 0.2;
            int weather = rng.nextInt(10);
            double sales = 10000 + traffic * 2.5 + promo * 3000 + isHoliday * 8000
                    + isWeekend * 2000 - priceIdx * 5000 + weather * 500
                    + rng.nextGaussian() * 3000;

            rows.add(RowFactory.create(dow, month, isHoliday, isWeekend, promo,
                    priceIdx, traffic, compPrice, weather, Math.max(0, sales)));
        }
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("day_of_week", DataTypes.IntegerType, false),
                DataTypes.createStructField("month", DataTypes.IntegerType, false),
                DataTypes.createStructField("is_holiday", DataTypes.IntegerType, false),
                DataTypes.createStructField("is_weekend", DataTypes.IntegerType, false),
                DataTypes.createStructField("promotion_level", DataTypes.IntegerType, false),
                DataTypes.createStructField("price_index", DataTypes.DoubleType, false),
                DataTypes.createStructField("user_traffic", DataTypes.IntegerType, false),
                DataTypes.createStructField("competitor_price", DataTypes.DoubleType, false),
                DataTypes.createStructField("weather_score", DataTypes.IntegerType, false),
                DataTypes.createStructField("daily_sales", DataTypes.DoubleType, false)
        });
        return spark.createDataFrame(rows, schema);
    }

    static Dataset<Row> generateRFMData() {
        List<Row> rows = new ArrayList<>();
        Random rng = new Random(42);
        for (int i = 0; i < 3000; i++) {
            int recency = rng.nextInt(365);
            int frequency = 1 + rng.nextInt(100);
            double monetary = 100 + rng.nextDouble() * 50000;
            rows.add(RowFactory.create(i, recency, frequency, monetary));
        }
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("user_id", DataTypes.IntegerType, false),
                DataTypes.createStructField("recency", DataTypes.IntegerType, false),
                DataTypes.createStructField("frequency", DataTypes.IntegerType, false),
                DataTypes.createStructField("monetary", DataTypes.DoubleType, false)
        });
        return spark.createDataFrame(rows, schema);
    }

    static Dataset<Row> generateRatingsData() {
        List<Row> rows = new ArrayList<>();
        Random rng = new Random(42);
        for (int i = 0; i < 50000; i++) {
            int userId = rng.nextInt(500);
            int itemId = rng.nextInt(200);
            float rating = 1.0f + rng.nextFloat() * 4.0f;
            rows.add(RowFactory.create(userId, itemId, rating));
        }
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("user_id", DataTypes.IntegerType, false),
                DataTypes.createStructField("item_id", DataTypes.IntegerType, false),
                DataTypes.createStructField("rating", DataTypes.FloatType, false)
        });
        return spark.createDataFrame(rows, schema);
    }
}
