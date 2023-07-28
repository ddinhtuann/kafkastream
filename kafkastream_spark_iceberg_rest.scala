
package com.sparkbyexamples.spark.streaming.kafka.json
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,LongType};
import org.apache.spark.sql.functions.{col, from_json,lit,rand}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf


object KafkafStream {
    def main(args: Array[String]) {
        
        val conf = new SparkConf().setAppName("kafkastream_iceberg")
                                .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                                .set("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
                                .set("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
                                .set("spark.sql.catalog.iceberg.uri", "https://iceberg.dtcsolution.vn")
                                .set("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse/wh")
                                .set("spark.sql.catalog.iceberg.s3.endpoint", "https://s3.dtcsolution.vn")
                                .set("spark.sql.defaultCatalog", "iceberg")
                                .set("spark.sql.catalog.iceberg.s3.path-style-access", "true")
                                .set("spark.sql.catalogImplementation", "in-memory")
                                .set("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")


        val spark = SparkSession.builder().config(conf).getOrCreate()

        val kafkaschema = StructType(Seq(
                                StructField("schema", StringType, nullable = true),
                                StructField("payload", StructType(Seq(
                                    StructField("before", StringType, nullable = true),
                                    StructField("after", StructType(Seq(
                                    StructField("log_id", IntegerType, nullable = false),
                                    StructField("camera_key", IntegerType, nullable = false),
                                    StructField("timestamp", LongType, nullable = false),
                                    StructField("path", StringType, nullable = false)

                                    ))),
                                    StructField("source", StringType, nullable = true),
                                    StructField("op", StringType, nullable = true),
                                    StructField("ts_ms", LongType, nullable = true),
                                    StructField("transaction", StringType, nullable = true)
                                )))
                                ))



        val kafkastreamdf = spark.readStream.format("kafka")
                    .option("kafka.bootstrap.servers", "1.52.48.26:9092")
                    .option("subscribe", "dtc_its.image_stream.log_camera")
                    .option("startingOffsets", "latest").load()
        
        val df = kafkastreamdf.select(from_json(col("value").cast("string"), kafkaschema)
                            .alias("data"))
                            .select("data.payload.after.log_id", "data.payload.after.camera_key", "data.payload.after.timestamp", "data.payload.after.path")
                            .withColumn("density", (rand()*50).cast("int")) 

        val metadata =  df.writeStream.format("iceberg")
                                .outputMode("append")
                                .option("path", "iceberg.db.traffic_table1")
                                .option("checkpointLocation", "/tmp/checkpoints")
                                .start().awaitTermination() 

        
    }
}

