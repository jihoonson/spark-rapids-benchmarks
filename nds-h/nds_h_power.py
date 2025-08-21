#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# SPDX-FileCopyrightText: Copyright (c) 2024-2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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
# Certain portions of the contents of this file are derived from TPC-H version 3.0.1
# (retrieved from www.tpc.org/tpc_documents_current_versions/current_specifications5.asp).
# Such portions are subject to copyrights held by Transaction Processing Performance Council (“TPC”)
# and licensed under the TPC EULA (a copy of which accompanies this file as “TPC EULA” and is also
# available at http://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp) (the “TPC EULA”).
#
# You may not use this file except in compliance with the TPC EULA.
# DISCLAIMER: Portions of this file is derived from the TPC-H Benchmark and as such any results
# obtained using this file are not comparable to published TPC-H Benchmark results, as the results
# obtained from using this file do not comply with the TPC-H Benchmark.
#

import argparse
import csv
import time
from pyspark.sql import SparkSession
import os
import sys

parent_dir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))

# Construct the path to the utils directory
utils_dir = os.path.join(parent_dir, 'utils')
# Add the utils directory to sys.path
sys.path.insert(0, utils_dir)

from python_benchmark_reporter.PysparkBenchReport import PysparkBenchReport

from check import check_version, check_json_summary_folder
from power_run_common import load_properties, get_query_subset, NDSHPowerRunner, Profiler

check_version()


def run_query_stream(input_prefix,
                     property_file,
                     query_stream_file,
                     time_log_output_path,
                     sub_queries,
                     warmup_iterations,
                     iterations,
                     plan_types,
                     input_format,
                     output_path=None,
                     keep_sc=False,
                     output_format="parquet",
                     json_summary_folder=None,
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
    app_name = "NDS-H - Power Run"
    # Execute Power Run or Specific query in Spark
    # build Spark Session
    session_builder = SparkSession.builder
    if property_file:
        spark_properties = load_properties(property_file)
        for k, v in spark_properties.items():
            session_builder = session_builder.config(k, v)
    spark_session = session_builder.appName(
        app_name).getOrCreate()
    spark_app_id = spark_session.sparkContext.applicationId

    # NDS-H does not support the profiling yet. We make a no-op profiler.
    profiler = Profiler(profiling_hook=None, output_root=None)

    # create runner early so we can use its setup_tables method
    runner = NDSHPowerRunner(PysparkBenchReport)
    
    if input_format != 'iceberg' and input_format != 'delta':
        execution_time_list = runner.setup_tables(spark_session, input_prefix, input_format,
                                                  execution_time_list, use_decimal=False)
    # parse queries from stream using the runner's parser
    query_dict = runner.gen_sql_from_stream(query_stream_file)
    check_json_summary_folder(json_summary_folder)
    if sub_queries:
        query_dict = get_query_subset(query_dict, sub_queries)
    power_start = int(time.time())
    for query_name, q_content in query_dict.items():
        # show query name in Spark web UI
        spark_session.sparkContext.setJobGroup(query_name, query_name)
        print("====== Run {} ======".format(query_name))
        q_report = PysparkBenchReport(spark_session, query_name)
        # TPC-H has empty-output queries for query15 parts
        empty_output = {'query15_part1', 'query15_part3'}
        summary = q_report.report_on(lambda *args, **kwargs: runner.run_one_query(*args, **kwargs),
                                     warmup_iterations,
                                     iterations,
                                     spark_session,
                                     q_content,
                                     query_name,
                                     output_path,
                                     output_format,
                                     save_plan_path,
                                     plan_types,
                                     skip_execution,
                                     empty_output_queries=empty_output)
        print(f"Time taken: {summary['queryTimes']} millis for {query_name}")
        query_times = summary['queryTimes']
        execution_time_list.append((spark_app_id, query_name, query_times[0]))
        queries_reports.append(q_report)
        if json_summary_folder:
            if property_file:
                summary_prefix = os.path.join(
                    json_summary_folder, os.path.basename(property_file)
                )
            else:
                summary_prefix = os.path.join(json_summary_folder, '')
            q_report.write_summary(prefix=summary_prefix)
    power_end = int(time.time())
    power_elapse = int((power_end - power_start)*1000)
    if not keep_sc:
        spark_session.sparkContext.stop()
    total_time_end = time.time()
    total_elapse = int((total_time_end - total_time_start) * 1000)
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

    sys.exit(exit_code)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('input_prefix',
                        help='text to prepend to every input file path (e.g., "hdfs:///ds-generated-data"). ' +
                             'If --hive or if input_format is "iceberg", this argument will be regarded as the value of property ' +
                             '"spark.sql.catalog.spark_catalog.warehouse". Only default Spark catalog ' +
                             'session name "spark_catalog" is supported now, customized catalog is not ' +
                             'yet supported. Note if this points to a Delta Lake table, the path must be ' +
                             'absolute. Issue: https://github.com/delta-io/delta/issues/555')
    parser.add_argument('query_stream_file',
                        help='query stream file that contains NDS queries in specific order')
    parser.add_argument('--keep_sc',
                        action='store_true',
                        help='Keep SparkContext alive after running all queries. This is a ' +
                             'limitation on Databricks runtime environment. User should always '
                             'attach this flag when running on Databricks.')
    parser.add_argument('time_log',
                        help='path to execution time log, only support local path.',
                        default="")
    parser.add_argument('--input_format',
                        help='type for input data source, e.g. parquet, orc, json, csv or iceberg, delta. ' +
                             'Certain types are not fully supported by GPU reading, please refer to ' +
                             'https://github.com/NVIDIA/spark-rapids/blob/branch-24.08/docs/compatibility.md ' +
                             'for more details.',
                        choices=['parquet', 'orc', 'avro', 'csv', 'json', 'iceberg', 'delta'],
                        default='parquet')
    parser.add_argument('--output_prefix',
                        help='text to prepend to every output file (e.g., "hdfs:///ds-parquet")')
    parser.add_argument('--json_summary_folder',
                        help='Empty folder/path (will create if not exist) to save JSON summary file for each query.')
    parser.add_argument('--sub_queries',
                        type=lambda s: [x.strip() for x in s.split(',')],
                        help='comma separated list of queries to run. If not specified, all queries ' +
                             'in the stream file will be run. e.g. "query1,query2,query3". Note, use ' +
                             '"_part1" and "_part2" suffix for the following query names: ' +
                             'query14, query23, query24, query39. e.g. query14_part1, query39_part2')
    parser.add_argument('--output_format',
                        help='type of query output',
                        default='parquet')
    parser.add_argument('--property_file',
                        help='property file for Spark configuration.')
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
                     args.sub_queries,
                     args.warmup_iterations,
                     args.iterations,
                     args.plan_types,
                     args.input_format,
                     args.output_prefix,
                     args.keep_sc,
                     args.output_format,
                     args.json_summary_folder,
                     args.save_plan_path,
                     args.skip_execution)
