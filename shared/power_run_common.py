#!/usr/bin/env python3
"""Shared helpers for NDS and NDS-H power-run wrappers.

This module provides helper functions and a minimal PowerRunner skeleton.
It intentionally keeps `setup_tables` signature compatible with `nds/nds_power.py`.
"""
from typing import Callable, Dict, Iterable, List, Optional, Set
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


def register_delta_tables(spark_session, input_prefix: str, execution_time_list: List):
    spark_app_id = spark_session.sparkContext.applicationId
    for table_name in spark_session.sql("SHOW TABLES").collect():
        # placeholder if runner wants to use it; the wrappers may provide their own schema lists
        pass
    # keep interface parity with nds implementation: return execution_time_list unchanged
    return execution_time_list


def setup_tables(spark_session, input_prefix: str, input_format: str, use_decimal: bool, execution_time_list: List, get_schemas_fn: Callable):
    """Top-level setup_tables kept compatible with `nds/nds_power.py` signature plus a required
    `get_schemas_fn` that the caller must provide. This centralizes table creation logic.
    """
    spark_app_id = spark_session.sparkContext.applicationId
    for table_name in get_schemas_fn(False).keys():
        start = int(time.time() * 1000)
        table_path = input_prefix + '/' + table_name
        reader = spark_session.read.format(input_format)
        if input_format in ['csv', 'json']:
            reader = reader.schema(get_schemas_fn(use_decimal)[table_name])
        reader.load(table_path).createOrReplaceTempView(table_name)
        end = int(time.time() * 1000)
        print("====== Creating TempView for table {} ======".format(table_name))
        print("Time taken: {} millis for table {}".format(end - start, table_name))
        execution_time_list.append(
            (spark_app_id, "CreateTempView {}".format(table_name), end - start))
    return execution_time_list


class Profiler:
    def __init__(self, profiling_hook: Optional[str], output_root: Optional[str]):
        self.profiling_hook = profiling_hook
        self.output_root = output_root
        self.is_profiling_enabled = output_root is not None and profiling_hook is not None
        self.query_name = None

    def __call__(self, query_name: str):
        self.query_name = query_name
        return self

    def __enter__(self):
        self.start_profiling()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop_profiling()
        self.query_name = None

    def execute_script(self, action: str):
        script_path = self.profiling_hook
        command = f"{script_path} {action} {shlex.quote(self.output_root)} {shlex.quote(self.query_name)}"
        subprocess.run(command, shell=True, check=True)

    def start_profiling(self):
        if self.is_profiling_enabled:
            print(f"Profiling started with profiling script: {self.profiling_hook} writing to {self.output_root} for query {self.query_name}.")
            self.execute_script('start')

    def stop_profiling(self):
        if self.is_profiling_enabled:
            self.execute_script('stop')
            print("Profiling stopped")


class PowerRunner:
    """Minimal runner skeleton. Per-wrapper behavior should be preserved by passing wrapper-specific
    collaborators (get_schemas, reporter, etc.)
    """
    def __init__(self, reporter_cls, get_schemas_fn: Callable, app_name: str = "Power Run"):
        self.reporter_cls = reporter_cls
        self.get_schemas_fn = get_schemas_fn
        self.app_name = app_name

    def setup_tables(self, spark_session, input_prefix: str, input_format: str, execution_time_list: List, use_decimal: bool = True):
        """Keep signature compatible with `nds/nds_power.py`.

        Note: if a caller (e.g. nds-h) needs `use_decimal=False`, it must pass that explicitly.
        """
        spark_app_id = spark_session.sparkContext.applicationId
        for table_name in self.get_schemas_fn(False).keys():
            start = int(time.time() * 1000)
            table_path = input_prefix + '/' + table_name
            reader = spark_session.read.format(input_format)
            if input_format in ['csv', 'json']:
                reader = reader.schema(self.get_schemas_fn(use_decimal)[table_name])
            reader.load(table_path).createOrReplaceTempView(table_name)
            end = int(time.time() * 1000)
            print("====== Creating TempView for table {} ======".format(table_name))
            print("Time taken: {} millis for table {}".format(end - start, table_name))
            execution_time_list.append(
                (spark_app_id, "CreateTempView {}".format(table_name), end - start))
        return execution_time_list

    def run_one_query(self, spark_session, profiler, query: str, query_name: str, output_path: Optional[str], output_format: str, save_plan_path: Optional[str], plan_types: Iterable[str], skip_execution: bool, empty_output_queries: Optional[Set[str]] = None):
        df = spark_session.sql(query)
        if empty_output_queries is None:
            empty_output_queries = set()
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
