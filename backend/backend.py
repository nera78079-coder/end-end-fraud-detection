import pymongo
from fastapi import FastAPI
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.feature import VectorAssembler, StandardScaler,StringIndexer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from asgiref.sync import sync_to_async

from kafka_consumer_frontend import consumer_frontend

app = FastAPI()

spark = (
    SparkSession.builder
    .appName("fraud")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")
    .getOrCreate()
)

model = LogisticRegressionModel.load("logistic_regression_model")

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["fraud-detection"]
collection = db["check-transaction-credential"]


@app.get("/check/transaction")
async def check_transaction():

    data = await sync_to_async(consumer_frontend)()

    data = {
        **data,
        "step": float(data["step"]),
        "amount": float(data["amount"]),
        "oldbalanceOrg": float(data["oldbalanceOrg"]),
        "newbalanceOrig": float(data["newbalanceOrig"]),
        "oldbalanceDest": float(data["oldbalanceDest"]),
        "newbalanceDest": float(data["newbalanceDest"]),
        "isFlaggedFraud": float(data["isFlaggedFraud"]),
    }

    doc = collection.insert_one({**data, "result": None})
    _id = doc.inserted_id

    df = spark.createDataFrame([data])

    df = df.withColumn(
        "nameOrigClass",
        when(col("nameOrig").startswith("C"), "Customer")
        .otherwise("Merchant")
    )

    df = df.withColumn(
        "nameDestClass",
        when(col("nameDest").startswith("C"), "Customer")
        .otherwise("Merchant")
    )
    
    indexer = StringIndexer(
        inputCol="nameDestClass",
        outputCol="nameDestClassEncoded"
)     
    df = indexer.fit(df).transform(df)
    
    indexer = StringIndexer(
        inputCol="nameOrigClass",
        outputCol="nameOrigClassEncoded"
) 
    df = indexer.fit(df).transform(df)

    indexer = StringIndexer(
        inputCol="type",
        outputCol="typeEncoded"
) 

    df = indexer.fit(df).transform(df)

    df = df.drop("nameOrigClass", "nameDestClass","nameOrig","nameDest","type")
    
    col_name=['step', 'amount', 'oldbalanceOrg', 'newbalanceOrig', 'oldbalanceDest',
          'newbalanceDest', 'isFlaggedFraud', 'nameDestClassEncoded',"nameOrigClassEncoded","typeEncoded"]
    
    assembler = VectorAssembler(
        inputCols=col_name,
        outputCol="features"
    )

    df = assembler.transform(df)

    scaler = StandardScaler(
        inputCol="features",
        outputCol="scaledFeatures",
        withMean=True,
        withStd=True,
    )

    df = scaler.fit(df).transform(df)

    predictions = model.transform(df)
    result = predictions.select("prediction").first()[0]

    collection.update_one(
        {"_id": _id},
        {"$set": {"result": float(result)}},
    )

    return {
        "result": "fraud" if result == 1.0 else "not fraud",
    }
