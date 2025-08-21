#!/usr/bin/env python3
"""Shared helpers for NDS and NDS-H power-run wrappers.

This module provides helper functions and a minimal PowerRunner skeleton.
It intentionally keeps `setup_tables` signature compatible with `nds/nds_power.py`.
"""
from typing import Callable, Dict, Iterable, List, Optional, Set
import abc
import os
import time
import csv
import subprocess
import shlex
from pyspark.sql import DataFrame


def ensure_valid_column_names(df: DataFrame) -> DataFrame:
    def is_column_start(char):
        return char.isalpha() or char == '_'

    def is_column_part(char):
        return char.isalpha() or char.isdigit() or char == '_'

    def is_valid(column_name):
        return len(column_name) > 0 and is_column_start(column_name[0]) and all(
            [is_column_part(char) for char in column_name[1:]])

    def make_valid(column_name):
        valid_name = ''
        if is_column_start(column_name[0]):
            valid_name += column_name[0]
        else:
            valid_name += '_'
        for char in column_name[1:]:
            if not is_column_part(char):
                valid_name += '_'
            else:
                valid_name += char
        return valid_name

    def deduplicate(column_names):
        dedup_col_names = []
        for i, v in enumerate(column_names):
            count = column_names.count(v)
            index = column_names[:i].count(v)
            dedup_col_names.append(v + str(index) if count > 1 else v)
        return dedup_col_names

    valid_col_names = [c if is_valid(c) else make_valid(c) for c in df.columns]
    dedup_col_names = deduplicate(valid_col_names)
    return df.toDF(*dedup_col_names)


def parse_explain_str(explain_str: str) -> Dict[str, str]:
    plan_strs = explain_str.split('\n\n')
    plan_dict = {}
    for plan_str in plan_strs:
        if plan_str.startswith('== Optimized Logical Plan =='):
            plan_dict['logical'] = plan_str
        elif plan_str.startswith('== Physical Plan =='):
            plan_dict['physical'] = plan_str
    return plan_dict


def load_properties(filename: str) -> Dict[str, str]:
    myvars = {}
    with open(filename) as myfile:
        for line in myfile:
            name, var = line.partition("=")[::2]
            myvars[name.strip()] = var.strip()
    return myvars


def get_query_subset(query_dict, subset):
    """Get a subset of queries from query_dict.
    The subset is specified by a list of query names. This mirrors the wrappers' previous helper.
    """
    # import locally to avoid top-level dependency issues in test/import time
    from check import check_query_subset_exists
    check_query_subset_exists(query_dict, subset)
    return dict((k, query_dict[k]) for k in subset)



class Profiler:
    def __init__(self, profiling_hook, output_root):
        self.profiling_hook = profiling_hook
        self.output_root = output_root
        self.is_profiling_enabled = output_root is not None and profiling_hook is not None

        self.query_name = None

    def __call__(self, query_name):
        self.query_name = query_name
        return self

    def __enter__(self,):
        self.start_profiling()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop_profiling()
        self.query_name = None

    def execute_script(self, action):
        script_path = self.profiling_hook
        command = f"{script_path} {action} {shlex.quote(self.output_root)} {shlex.quote(self.query_name)}"
        try:
            subprocess.run(command, shell=True, check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error: Script exited with status {e.returncode}")
            raise

    def start_profiling(self):
        if self.is_profiling_enabled:
            print(f"Profiling started with profiling script: {self.profiling_hook} writing to {self.output_root} for query {self.query_name}.")
            self.execute_script('start')

    def stop_profiling(self):
        if self.is_profiling_enabled:
            self.execute_script('stop')
            print("Profiling stopped")


class PowerRunner(abc.ABC):
    """Abstract runner. Subclasses must implement `get_schemas(use_decimal)`.
    This centralizes shared behavior while allowing benchmark-specific schema loaders.
    """
    def __init__(self, reporter_cls):
        self.reporter_cls = reporter_cls

    @abc.abstractmethod
    def get_schemas(self, use_decimal: bool) -> Dict[str, object]:
        """Return a dict of table_name -> schema; subclasses implement this."""

    @abc.abstractmethod
    def gen_sql_from_stream(self, query_stream_file_path: str) -> Dict[str, str]:
        """Parse a query stream file and return an ordered mapping of query_name -> SQL string.
        Subclasses must implement this because query stream formats differ between benchmarks.
        """

    def setup_tables(self, 
                     spark_session, 
                     input_prefix, 
                     input_format, 
                     execution_time_list, 
                     use_decimal):
        """set up data tables in Spark before running the Power Run queries.
    
        Args:
            spark_session (SparkSession): a SparkSession instance to run queries.
            input_prefix (str): path of input data.
            input_format (str): type of input data source, e.g. parquet, orc, csv, json.
            execution_time_list ([(str, str, int)]): a list to record query and its execution time.
            use_decimal (bool): use decimal type for certain columns when loading data of text type.
    
        Returns:
            execution_time_list: a list recording query execution time.
        """
        spark_app_id = spark_session.sparkContext.applicationId
        # Create TempView for tables
        for table_name in self.get_schemas(False).keys():
            start = int(time.time() * 1000)
            table_path = input_prefix + '/' + table_name
            reader = spark_session.read.format(input_format)
            if input_format in ['csv', 'json']:
                reader = reader.schema(self.get_schemas(use_decimal)[table_name])
            reader.load(table_path).createOrReplaceTempView(table_name)
            end = int(time.time() * 1000)
            print("====== Creating TempView for table {} ======".format(table_name))
            print("Time taken: {} millis for table {}".format(end - start, table_name))
            execution_time_list.append(
                (spark_app_id, "CreateTempView {}".format(table_name), end - start))
        return execution_time_list

    def register_delta_tables(self, spark_session, input_prefix, execution_time_list):
        spark_app_id = spark_session.sparkContext.applicationId
        # Register tables for Delta Lake
        for table_name in self.get_schemas(False).keys():
            start = int(time.time() * 1000)
            # input_prefix must be an absolute path: https://github.com/delta-io/delta/issues/555
            register_sql = f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{input_prefix}/{table_name}'"
            print(register_sql)
            spark_session.sql(register_sql)
            end = int(time.time() * 1000)
            print("====== Registering for table {} ======".format(table_name))
            print("Time taken: {} millis for table {}".format(end - start, table_name))
            execution_time_list.append(
                (spark_app_id, "Register {}".format(table_name), end - start))
        return execution_time_list

    def run_one_query(self, 
                      spark_session, 
                      profiler, 
                      query, 
                      query_name, 
                      output_path, 
                      output_format, 
                      save_plan_path, 
                      plan_types, 
                      skip_execution, 
                      empty_output_queries=[]):
        with profiler(query_name=query_name):
            print(f"Running query {query_name}")
            df = spark_session.sql(query)
            if not skip_execution:
                if query_name in empty_output_queries or not output_path:
                    df.collect()
                else:
                    ensure_valid_column_names(df).write.format(output_format).mode('overwrite').save(
                            output_path + '/' + query_name)
            if save_plan_path:
                os.makedirs(save_plan_path, exist_ok=True)
                explain_str = spark_session._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), 'extended')
                plans = parse_explain_str(explain_str)
                for plan_type in plan_types:
                    with open(save_plan_path + '/' + query_name + "." + plan_type, 'w') as f:
                        f.write(plans[plan_type])


class NDSPowerRunner(PowerRunner):
    def __init__(self, reporter_cls):
        super().__init__(reporter_cls)

    def get_schemas(self, use_decimal: bool) -> Dict[str, object]:
        # defer import to here to avoid heavy imports at module import time
        from nds.nds_schema import get_schemas as _get_schemas
        return _get_schemas(use_decimal)

    def gen_sql_from_stream(self, query_stream_file_path: str):
        """Read Spark compatible query stream and split them one by one
    
        Args:
            query_stream_file_path (str): path of query stream generated by TPC-DS tool
    
        Returns:
            ordered dict: an ordered dict of {query_name: query content} query pairs
        """

        from collections import OrderedDict
        from nds_gen_query_stream import split_special_query

        with open(query_stream_file_path, 'r') as f:
            stream = f.read()
        all_queries = stream.split('-- start')[1:]
        # split query in query14, query23, query24, query39
        extended_queries = OrderedDict()
        for q in all_queries:
            # e.g. "-- start query 32 in stream 0 using template query98.tpl"
            query_name = q[q.find('template')+9: q.find('.tpl')]
            if 'select' in q.split(';')[1]:
                part_1, part_2 = split_special_query(q)
                extended_queries[query_name + '_part1'] = part_1
                extended_queries[query_name + '_part2'] = part_2
            else:
                extended_queries[query_name] = q
    
        # add "-- start" string back to each query
        for q_name, q_content in extended_queries.items():
            extended_queries[q_name] = '-- start' + q_content
        return extended_queries

class NDSHPowerRunner(PowerRunner):
    def __init__(self, reporter_cls):
        super().__init__(reporter_cls)

    def get_schemas(self, use_decimal: bool) -> Dict[str, object]:
        if use_decimal:
            raise NotImplementedError("Decimal type is not supported for NDS-H")
        from nds_h.nds_h_schema import get_schemas as _get_schemas
        # NDS-H always calls with use_decimal=False per repo conventions
        return _get_schemas()

    def gen_sql_from_stream(self, query_stream_file_path: str):
        """Read Spark compatible query stream and split them one by one

        Args:
            query_stream_file_path (str): path of query stream generated by NDS-H tool

        Returns:
            ordered dict: an ordered dict of {query_name: query content} query pairs
        """
        
        import re
        from collections import OrderedDict

        extended_queries = OrderedDict()
        with open(query_stream_file_path, 'r') as f:
            stream = f.read()
        pattern = re.compile(r'-- Template file: (\d+)\n\n(.*?)(?=(?:-- Template file: \d+)|\Z)', re.DOTALL)

        # Find all matches in the content
        matches = pattern.findall(stream)

        # Populate the dictionary with template file numbers as keys and queries as values
        for match in matches:
            template_number = match[0]
            if int(template_number) == 15:
                new_queries = match[1].split(";")
                extended_queries[f'query{template_number}_part1'] = new_queries[0].strip()
                extended_queries[f'query{template_number}_part2'] = new_queries[1].strip()
                extended_queries[f'query{template_number}_part3'] = new_queries[2].strip()
            else:
                sql_query = match[1].strip()
                extended_queries[f'query{template_number}'] = sql_query

        return extended_queries
