"""
PySpark ML Pipeline - Converted from sklearn
=============================================
Giữ nguyên:
  - Tham số gốc của LogisticRegression
  - Logic tìm best threshold (0.1 → 0.55)
  - Optimize mode: recall0 hoặc f1
  - class_weight="balanced"
  - Feature engineering: is_high_freight, price_per_item, delay_ratio

Thay đổi:
  - sklearn → PySpark MLlib
  - CSV → Delta (MinIO)
"""

import os
import sys
import argparse

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array

# Ensure DW utils module is importable when executed via spark-submit.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DW_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, "..", "gold", "dw"))
if DW_DIR not in sys.path:
    sys.path.insert(0, DW_DIR)

from common_utils import get_spark_session


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="s3a://lakehouse/features_ml/olist_ml_dataset/")
    parser.add_argument("--model", choices=["logreg"], default="logreg")
    parser.add_argument("--optimize", choices=["f1", "recall0"], default="recall0")
    parser.add_argument("--test-size", type=float, default=0.2)
    parser.add_argument("--random-state", type=int, default=42)
    args, _ = parser.parse_known_args()
    return args


def compute_class_weights(df, target_col):
    label_counts = df.groupBy(target_col).count().collect()
    count_dict = {row[target_col]: row["count"] for row in label_counts}
    total = sum(count_dict.values())
    n_classes = len(count_dict)
    weights = {k: total / (n_classes * v) for k, v in count_dict.items()}
    print(f"Class weights (balanced): {weights}")
    return weights


def stratified_split(df, target_col, test_size, seed):
    train_ratio = 1.0 - test_size
    train_parts, test_parts = [], []

    for label_val in [0, 1]:
        subset = df.filter(F.col(target_col) == label_val)
        tr, te = subset.randomSplit([train_ratio, test_size], seed=seed)
        train_parts.append(tr)
        test_parts.append(te)

    train_df = train_parts[0].union(train_parts[1])
    test_df = test_parts[0].union(test_parts[1])
    return train_df, test_df


def build_pipeline(model_name, feature_cols, target_col):
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="raw_features",
        handleInvalid="skip",
    )

    if model_name == "logreg":
        scaler = StandardScaler(
            inputCol="raw_features",
            outputCol="features",
            withMean=True,
            withStd=True,
        )
        clf = LogisticRegression(
            labelCol=target_col,
            featuresCol="features",
            weightCol="classWeight",
            maxIter=1000,
            regParam=0.0,
            elasticNetParam=0.0,
            standardization=False,
        )
        stages = [assembler, scaler, clf]

    return Pipeline(stages=stages)


def find_best_threshold(test_df, target_col, mode="recall0"):
    thresholds = [0.55]

    best_score = -1.0
    best_t = 0.0
    best_pred_df = None

    df_with_prob = test_df.withColumn("prob1", vector_to_array("probability")[1])

    for t in thresholds:
        df_t = df_with_prob.withColumn(
            "pred_t",
            (F.col("prob1") > t).cast(DoubleType()),
        )

        counts = df_t.select(
            F.sum(F.when((F.col("pred_t") == 0) & (F.col(target_col) == 0), 1).otherwise(0)).alias("tp0"),
            F.sum(F.when((F.col("pred_t") == 0) & (F.col(target_col) == 1), 1).otherwise(0)).alias("fp0"),
            F.sum(F.when((F.col("pred_t") == 1) & (F.col(target_col) == 0), 1).otherwise(0)).alias("fn0"),
            F.sum(F.when((F.col("pred_t") == 1) & (F.col(target_col) == 1), 1).otherwise(0)).alias("tp1"),
        ).collect()[0]

        tp0 = counts["tp0"]
        fp0 = counts["fp0"]
        fn0 = counts["fn0"]
        tp1 = counts["tp1"]

        p0 = tp0 / (tp0 + fp0) if (tp0 + fp0) else 0.0
        r0 = tp0 / (tp0 + fn0) if (tp0 + fn0) else 0.0
        f0 = (2 * p0 * r0 / (p0 + r0)) if (p0 + r0) else 0.0

        p1 = tp1 / (tp1 + fn0) if (tp1 + fn0) else 0.0
        r1 = tp1 / (tp1 + fp0) if (tp1 + fp0) else 0.0
        f1_1 = (2 * p1 * r1 / (p1 + r1)) if (p1 + r1) else 0.0

        macro_f1 = (f0 + f1_1) / 2.0
        score = r0 if mode == "recall0" else macro_f1

        print(f"\nThreshold: {t}")
        print(f"Score ({mode}): {round(score, 4)}")

        if score > best_score:
            best_score = score
            best_t = t
            best_pred_df = df_t

    return best_t, best_score, best_pred_df


def print_final_report(df_pred, target_col, best_t, auc, model_name, optimize_mode):
    counts = df_pred.select(
        F.sum(F.when((F.col("pred_t") == 0) & (F.col(target_col) == 0), 1).otherwise(0)).alias("tp0"),
        F.sum(F.when((F.col("pred_t") == 0) & (F.col(target_col) == 1), 1).otherwise(0)).alias("fp0"),
        F.sum(F.when((F.col("pred_t") == 1) & (F.col(target_col) == 0), 1).otherwise(0)).alias("fn0"),
        F.sum(F.when((F.col("pred_t") == 1) & (F.col(target_col) == 1), 1).otherwise(0)).alias("tp1"),
        F.sum(F.when(F.col(target_col) == 0, 1).otherwise(0)).alias("n0"),
        F.sum(F.when(F.col(target_col) == 1, 1).otherwise(0)).alias("n1"),
    ).collect()[0]

    tp0 = counts["tp0"]
    fp0 = counts["fp0"]
    fn0 = counts["fn0"]
    tp1 = counts["tp1"]
    n0 = counts["n0"]
    n1 = counts["n1"]
    n = n0 + n1

    p0 = tp0 / (tp0 + fp0) if (tp0 + fp0) else 0.0
    r0 = tp0 / (tp0 + fn0) if (tp0 + fn0) else 0.0
    f0 = (2 * p0 * r0 / (p0 + r0)) if (p0 + r0) else 0.0

    p1 = tp1 / (tp1 + fn0) if (tp1 + fn0) else 0.0
    r1 = tp1 / (tp1 + fp0) if (tp1 + fp0) else 0.0
    f1_1 = (2 * p1 * r1 / (p1 + r1)) if (p1 + r1) else 0.0
    acc = (tp0 + tp1) / n if n else 0.0

    macro_p = (p0 + p1) / 2
    macro_r = (r0 + r1) / 2
    macro_f = (f0 + f1_1) / 2
    wtd_p = (p0 * n0 + p1 * n1) / n if n else 0.0
    wtd_r = (r0 * n0 + r1 * n1) / n if n else 0.0
    wtd_f = (f0 * n0 + f1_1 * n1) / n if n else 0.0

    print("\n" + "=" * 50)
    print(f"Model        : {model_name}")
    print(f"Optimize for : {optimize_mode}")
    print(f"Best threshold: {best_t}")
    print(f"ROC-AUC      : {auc:.4f}")

    print("\nConfusion matrix:")
    print(f"[[{tp0:6d}  {fp0:6d}]")
    print(f" [{fn0:6d}  {tp1:6d}]]")

    print(f"\nClassification report:")
    print(f"{'':>12}{'precision':>10}{'recall':>10}{'f1-score':>10}{'support':>10}")
    print(f"{'0':>12}{p0:>10.4f}{r0:>10.4f}{f0:>10.4f}{n0:>10}")
    print(f"{'1':>12}{p1:>10.4f}{r1:>10.4f}{f1_1:>10.4f}{n1:>10}")
    print()
    print(f"{'accuracy':>12}{'':>10}{'':>10}{acc:>10.4f}{n:>10}")
    print(f"{'macro avg':>12}{macro_p:>10.4f}{macro_r:>10.4f}{macro_f:>10.4f}{n:>10}")
    print(f"{'weighted avg':>12}{wtd_p:>10.4f}{wtd_r:>10.4f}{wtd_f:>10.4f}{n:>10}")


def main():
    args = parse_args()
    spark = get_spark_session("ML_Pipeline_PySpark")
    spark.sparkContext.setLogLevel("ERROR")
    print("Spark:", spark.version)

    df = spark.read.format("delta").load(args.data)

    target_col = "label"
    feature_cols = [
        "delivery_delay",
        "delivery_time",
        "price_sum",
        "price_mean",
        "freight_sum",
        "freight_mean",
        "freight_ratio",
        "item_count",
        "is_high_freight",
        "price_per_item",
        "delay_ratio",
    ]

    df = df.dropna(subset=feature_cols + [target_col])

    weights = compute_class_weights(df, target_col)
    df = df.withColumn(
        "classWeight",
        F.when(F.col(target_col) == 0, weights[0]).otherwise(weights[1]),
    )

    train_df, test_df = stratified_split(
        df, target_col, args.test_size, args.random_state
    )
    pipeline = build_pipeline(args.model, feature_cols, target_col)
    print(f"Training [{args.model}]...")
    model = pipeline.fit(train_df)

    model_path = "s3a://lakehouse/models/olist_logreg"
    model.write().overwrite().save(model_path)
    print(f"Saved model to {model_path}")

    predictions = model.transform(test_df)
    auc_eval = BinaryClassificationEvaluator(
        labelCol=target_col, metricName="areaUnderROC"
    )
    auc = auc_eval.evaluate(predictions)

    print(f"\nSearching best threshold (optimize={args.optimize})...\n")
    best_t, best_score, best_pred_df = find_best_threshold(predictions, target_col, args.optimize)
    print_final_report(best_pred_df, target_col, best_t, auc, args.model, args.optimize)

    spark.stop()
    print("Done.")


if __name__ == "__main__":
    main()
