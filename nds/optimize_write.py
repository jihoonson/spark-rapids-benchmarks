#!/usr/bin/env python3

import argparse
import pyspark
import timeit


def run(args):
    spark = pyspark.sql.SparkSession.builder.appName(f"Optimize write test - {args.type}").getOrCreate()

    result = timeit.timeit(
        lambda: spark.read.format("parquet").load("/home/jihoons/Local/scripts/spark-warehouse/bad_store_sales")\
            .write.format("delta").partitionBy("ss_sold_date_sk")\
            .option("optimizeWrite", "True").save(f"/home/jihoons/data/tpcds/sf=10/delta-{args.type}/store_sales"),
        number=1)
    
    with open(args.report_file, "w") as report:
        report.write(f"Time taken for optimize write on {args.type} table: {result:.2f} seconds\n")
        report.write("Spark Configuration:\n")

        for conf in spark.sparkContext.getConf().getAll():
            report.write(str(conf) + "\n")

    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--type',
        type=str,
        choices=['gpu', 'cpu'],
        required=True,
        help='Type of the table to create: gpu or cpu.'
    )
    parser.add_argument(
        'report_file',
        help='location to store a performance report(local)')
    args = parser.parse_args()
    run(args)
