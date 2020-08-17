from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer


if __name__ == "__main__":
    ss = SparkSession.builder.master("local").appName("MLWordCount").getOrCreate()
    # 训练数据集
    trainningDF = ss.createDataFrame(
        [
            (0, "a b c d e spark", 1.0),
            (1, "b d", 0.0),
            (2, "spark f g h", 1.0),
            (3, "hadoop mapreduce", 0.0)
        ],
        [
            "id", "text", "label"
        ]
    )
    # 测试数据集
    testDF = ss.createDataFrame(
        [
            (4, "spark i j k"),
            (5, "i m n"),
            (6, "spark hadoop spark"),
            (7, "apache hadoop")
        ],
        [
            "id", "text"
        ]
    )

    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    hashingTF =  HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")
    lr = LogisticRegression(maxIter=10, regParam=0.001)

    pipeLine = Pipeline(stages=[tokenizer, hashingTF, lr])
    model = pipeLine.fit(trainningDF)

    prediction = model.transform(testDF)
    selected = prediction.select("id", "text", "probability", "prediction")

    for row in selected.collect():
        rid, text, prob, prediction = row
        print("(%d, %s) --> %s, prediction=%f"%(rid, text, str(prob), prediction))
