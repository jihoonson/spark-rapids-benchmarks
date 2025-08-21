#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# SPDX-FileCopyrightText: Copyright (c) 2022-2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# -----
#
# Certain portions of the contents of this file are derived from TPC-DS version 3.2.0
# (retrieved from www.tpc.org/tpc_documents_current_versions/current_specifications5.asp).
# Such portions are subject to copyrights held by Transaction Processing Performance Council (“TPC”)
# and licensed under the TPC EULA (a copy of which accompanies this file as “TPC EULA” and is also
# available at http://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp) (the “TPC EULA”).
#
# You may not use this file except in compliance with the TPC EULA.
# DISCLAIMER: Portions of this file is derived from the TPC-DS Benchmark and as such any results
# obtained using this file are not comparable to published TPC-DS Benchmark results, as the results
# obtained from using this file do not comply with the TPC-DS Benchmark.
#

import argparse
import csv
import os
import sys
import time 
from pyspark.sql import SparkSession
from PysparkBenchReport import PysparkBenchReport

from check import check_json_summary_folder, check_version
from power_run_common import load_properties, get_query_subset
from power_run_common import Profiler, NDSPowerRunner

check_version()


def run_query_stream(input_prefix,
                     property_file,
                     query_stream_file,
                     time_log_output_path,
                     extra_time_log_output_path,
                     sub_queries,
                     warmup_iterations,
                     iterations,
                     plan_types,
                     input_format="parquet",
                     use_decimal=True,
                     output_path=None,
                     output_format="parquet",
                     json_summary_folder=None,
                     delta_unmanaged=False,
                     keep_sc=False,
                     hive_external=False,
                     allow_failure=False,
                     profiling_hook=None,
                     save_plan_path=None,
                     skip_execution=False):
    """
    run SQL in Spark and record execution time log. The execution time log is saved as a CSV file
    for easy accessibility. Table/TempView Creation time is also recorded.
    """
    queries_reports = []
    execution_time_list = []
    total_time_start = time.time()
    # check if it's running specific query or Power Run
    if len(query_dict) == 1:
        app_name = "NDS - " + list(query_dict.keys())[0]
    else:
        app_name = "NDS - Power Run"
    # Execute Power Run or Specific query in Spark
    # build Spark Session
    session_builder = SparkSession.builder
    if property_file:
        spark_properties = load_properties(property_file)
        for k,v in spark_properties.items():
            session_builder = session_builder.config(k,v)
    if input_format == 'iceberg':
        session_builder.config("spark.sql.catalog.spark_catalog.warehouse", input_prefix)
    if input_format == 'delta' and not delta_unmanaged:
        session_builder.config("spark.sql.warehouse.dir", input_prefix)
        session_builder.enableHiveSupport()
    if hive_external:
        session_builder.enableHiveSupport()

    spark_session = session_builder.appName(
        app_name).getOrCreate()
    if hive_external:
        spark_session.catalog.setCurrentDatabase(input_prefix)

    spark_app_id = spark_session.sparkContext.applicationId
    # create runner early so we can use its setup_tables and delta registration methods
    runner = NDSPowerRunner(PysparkBenchReport)
    if input_format != 'iceberg' and input_format != 'delta' and not hive_external:
        execution_time_list = runner.setup_tables(spark_session, input_prefix, input_format,
                                                  execution_time_list, use_decimal)
    elif input_format == 'delta' and delta_unmanaged:
        # Register tables for Delta Lake. This is only needed for unmanaged tables.
        execution_time_list = runner.register_delta_tables(spark_session, input_prefix, execution_time_list)

    # parse queries from stream using the runner's parser
    query_dict = runner.gen_sql_from_stream(query_stream_file)

    check_json_summary_folder(json_summary_folder)
    if sub_queries:
        query_dict = get_query_subset(query_dict, sub_queries)
    
    # Setup profiler
    profiler = Profiler(profiling_hook=profiling_hook, output_root=json_summary_folder)

    # Run query
    power_start = int(time.time())
    for query_name, q_content in query_dict.items():
        # show query name in Spark web UI
        spark_session.sparkContext.setJobGroup(query_name, query_name)
        print("====== Run {} ======".format(query_name))
        q_report = PysparkBenchReport(spark_session, query_name)
        # use shared runner.run_one_query to execute each query; leave profiler and behavior unchanged
        summary = q_report.report_on(runner.run_one_query, warmup_iterations,
                                                   iterations,
                                                   spark_session,
                                                   q_content,
                                                   query_name,
                                                   output_path,
                                                   output_format,
                                                   save_plan_path,
                                                   plan_types,
                                                   skip_execution,
                                                   profiler)
        print(f"Time taken: {summary['queryTimes']} millis for {query_name}")
        query_times = summary['queryTimes']
        for query_time in query_times:
            execution_time_list.append((spark_app_id, query_name, query_time))
        queries_reports.append(q_report)
        if json_summary_folder:
            # property_file e.g.: "property/aqe-on.properties" or just "aqe-off.properties"
            if property_file:
                summary_prefix = os.path.join(
                    json_summary_folder, os.path.basename(property_file).split('.')[0])
            else:
                summary_prefix =  os.path.join(json_summary_folder, '')
            q_report.write_summary(prefix=summary_prefix)
    power_end = int(time.time())
    power_elapse = int((power_end - power_start)*1000)
    if not keep_sc:
        spark_session.sparkContext.stop()
    total_time_end = time.time()
    total_elapse = int((total_time_end - total_time_start)*1000)
    print("====== Power Test Time: {} milliseconds ======".format(power_elapse))
    print("====== Total Time: {} milliseconds ======".format(total_elapse))
    execution_time_list.append(
        (spark_app_id, "Power Start Time", power_start))
    execution_time_list.append(
        (spark_app_id, "Power End Time", power_end))
    execution_time_list.append(
        (spark_app_id, "Power Test Time", power_elapse))
    execution_time_list.append(
        (spark_app_id, "Total Time", total_elapse))

    header = ["application_id", "query", "time/milliseconds"]
    # print to driver stdout for quick view
    print(header)
    for row in execution_time_list:
        print(row)
    # write to local file at driver node
    with open(time_log_output_path, 'w', encoding='UTF8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(execution_time_list)
    # write to csv in cloud environment
    if extra_time_log_output_path:
        spark_session = SparkSession.builder.getOrCreate()
        time_df = spark_session.createDataFrame(data=execution_time_list, schema = header)
        time_df.coalesce(1).write.csv(extra_time_log_output_path)

    # check queries_reports, if there's any task or query failed, exit a non-zero to represent the script failure
    exit_code = 0
    for q in queries_reports:
        if not q.is_success():
            if exit_code == 0:
                print("====== Queries with failure ======")
            print("{} status: {}".format(q.summary['query'], q.summary['queryStatus']))
            exit_code = 1
    if exit_code:
        print("Above queries failed or completed with failed tasks. Please check the logs for the detailed reason.")

    if not allow_failure and exit_code:
        sys.exit(exit_code)

if __name__ == "__main__":
    parser = parser = argparse.ArgumentParser()
    parser.add_argument('input_prefix',
                        help='text to prepend to every input file path (e.g., "hdfs:///ds-generated-data"). ' +
                        'If --hive or if input_format is "iceberg", this argument will be regarded as the value of property ' +
                        '"spark.sql.catalog.spark_catalog.warehouse". Only default Spark catalog ' +
                        'session name "spark_catalog" is supported now, customized catalog is not ' +
                        'yet supported. Note if this points to a Delta Lake table, the path must be ' +
                        'absolute. Issue: https://github.com/delta-io/delta/issues/555')
    parser.add_argument('query_stream_file',
                        help='query stream file that contains NDS queries in specific order')
    parser.add_argument('time_log',
                        help='path to execution time log, only support local path.',
                        default="")
    parser.add_argument('--input_format',
                        help='type for input data source, e.g. parquet, orc, json, csv or iceberg, delta. ' +
                        'Certain types are not fully supported by GPU reading, please refer to ' +
                        'https://github.com/NVIDIA/spark-rapids/blob/branch-22.08/docs/compatibility.md ' +
                        'for more details.',
                        choices=['parquet', 'orc', 'avro', 'csv', 'json', 'iceberg', 'delta'],
                        default='parquet')
    parser.add_argument('--output_prefix',
                        help='text to prepend to every output file (e.g., "hdfs:///ds-parquet")')
    parser.add_argument('--output_format',
                        help='type of query output',
                        default='parquet')
    parser.add_argument('--property_file',
                        help='property file for Spark configuration.')
    parser.add_argument('--floats',
                        action='store_true',
                        help='When loading Text files like json and csv, schemas are required to ' +
                        'determine if certain parts of the data are read as decimal type or not. '+
                        'If specified, float data will be used.')
    parser.add_argument('--json_summary_folder',
                        help='Empty folder/path (will create if not exist) to save JSON summary file for each query.')
    parser.add_argument('--delta_unmanaged',
                        action='store_true',
                        help='Use unmanaged tables for DeltaLake. This is useful for testing DeltaLake without ' +
        '               leveraging a Metastore service.')
    parser.add_argument('--keep_sc',
                        action='store_true',
                        help='Keep SparkContext alive after running all queries. This is a ' +
                        'limitation on Databricks runtime environment. User should always attach ' +
                        'this flag when running on Databricks.')
    parser.add_argument('--hive',
                        action='store_true',
                        help='use table meta information in Hive metastore directly without ' +
                        'registering temp views.')
    parser.add_argument('--extra_time_log',
                        help='extra path to save time log when running in cloud environment where '+
                        'driver node/pod cannot be accessed easily. User needs to add essential extra ' +
                        'jars and configurations to access different cloud storage systems. ' +
                        'e.g. s3, gs etc.')
    parser.add_argument('--sub_queries',
                        type=lambda s: [x.strip() for x in s.split(',')],
                        help='comma separated list of queries to run. If not specified, all queries ' +
                        'in the stream file will be run. e.g. "query1,query2,query3". Note, use ' +
                        '"_part1" and "_part2" suffix for the following query names: ' +
                        'query14, query23, query24, query39. e.g. query14_part1, query39_part2')
    parser.add_argument('--allow_failure',
                        action='store_true',
                        help='Do not exit with non zero when any query failed or any task failed')
    parser.add_argument('--profiling_hook',
                        help='Executable that is called just before/after a query executes.' +
                        'The executable is called like this ' +
                        './hook {start|stop} output_root query_name.')
    parser.add_argument('--warmup_iterations',
                        type=int,
                        help='Number of warmup iterations for each query.',
                        default=0)
    parser.add_argument('--iterations',
                        type=int,
                        help='Number of iterations for each query.',
                        default=1)
    parser.add_argument('--save_plan_path',
                        help='Save the execution plan of each query to the specified file. If --skip_execution is ' +
                        'specified, the execution plan will be saved without executing the query.')
    parser.add_argument('--plan_types',
                        type=lambda s: [x.strip() for x in s.split(',')],
                        help='Comma separated list of plan types to save. ' +
                        'e.g. "physical, logical". Default is "logical".',
                        default='logical')
    parser.add_argument('--skip_execution',
                        action='store_true',
                        help='Skip the execution of the queries. This can be used in conjunction with ' +
                        '--save_plan_path to only save the execution plans without running the queries.' +
                        'Note that "spark.sql.adaptive.enabled" should be set to false to get GPU physical plans.')
    args = parser.parse_args()
    run_query_stream(args.input_prefix,
                     args.property_file,
                     args.query_stream_file,
                     args.time_log,
                     args.extra_time_log,
                     args.sub_queries,
                     args.warmup_iterations,
                     args.iterations,
                     args.plan_types,
                     args.input_format,
                     not args.floats,
                     args.output_prefix,
                     args.output_format,
                     args.json_summary_folder,
                     args.delta_unmanaged,
                     args.keep_sc,
                     args.hive,
                     args.allow_failure,
                     args.profiling_hook,
                     args.save_plan_path,
                     args.skip_execution)
