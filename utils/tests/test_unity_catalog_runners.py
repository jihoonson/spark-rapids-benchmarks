import contextlib
import csv
import importlib.util
import inspect
import io
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[2]


def load_runner(directory, filename):
    source = ROOT / directory / filename
    sys.path.insert(0, str(source.parent))
    sys.path.insert(0, str(ROOT / 'utils'))
    spec = importlib.util.spec_from_file_location(directory.replace('-', '_') + '_uc_test', source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Reporter:
    def __init__(self, spark, name):
        self.summary = {'queryTimes': [0], 'query': name, 'queryStatus': []}

    def report_on(self, callback, warmups, iterations, *arguments):
        callback(*arguments)
        return self.summary

    def is_success(self):
        return True


class UnityCatalogRunnersTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.runners = {
            'nds': load_runner('nds', 'nds_power.py'),
            'nds-h': load_runner('nds-h', 'nds_h_power.py'),
        }

    def run_runner(self, benchmark, **options):
        module = self.runners[benchmark]
        spark = Mock()
        builder = Mock()
        builder.appName.return_value = builder
        builder.config.return_value = builder
        builder.getOrCreate.return_value = spark
        builder.enableHiveSupport.return_value = builder
        defaults = {
            'input_prefix': 'file:///datasets', 'property_file': None,
            'query_dict': {}, 'time_log_output_path': '',
            'extra_time_log_output_path': None, 'sub_queries': None,
            'sub_query_patterns': None, 'warmup_iterations': 0, 'iterations': 1,
            'plan_types': None, 'input_format': 'delta', 'keep_sc': True,
        }
        defaults.update(options)
        arguments = {name: defaults[name] for name in inspect.signature(module.run_query_stream).parameters
                     if name in defaults}
        with tempfile.TemporaryDirectory() as directory:
            arguments['time_log_output_path'] = str(Path(directory) / 'result.csv')
            with (patch.object(module, 'SparkSession', SimpleNamespace(builder=builder)),
                  patch.object(module, '_get_app_id', return_value='app-test'),
                  patch.object(module, 'check_json_summary_folder'),
                  patch.object(module, 'PysparkBenchReport', Reporter),
                  contextlib.redirect_stdout(io.StringIO())):
                try:
                    module.run_query_stream(**arguments)
                except SystemExit as stopped:
                    if stopped.code != 0:
                        raise
            with open(arguments['time_log_output_path']) as handle:
                rows = list(csv.reader(handle))
        return spark, builder, rows

    def test_both_runners_register_complete_datasets_before_sql(self):
        queries = {'read': 'SELECT 1', 'create': 'CREATE TABLE scratch USING DELTA AS SELECT 1',
                   'update': 'UPDATE scratch SET id = 2', 'delete': 'DELETE FROM scratch',
                   'maintenance': 'OPTIMIZE scratch', 'cleanup': 'DROP TABLE scratch'}
        for benchmark, module in self.runners.items():
            with self.subTest(benchmark=benchmark):
                spark, builder, rows = self.run_runner(
                    benchmark, unity_catalog=True, query_dict=queries)
                statements = [call.args[0] for call in spark.sql.call_args_list]
                self.assertEqual('CREATE SCHEMA IF NOT EXISTS `ab`.`' + benchmark + '`', statements[0])
                self.assertEqual('USE `ab`.`' + benchmark + '`', statements[1])
                tables = module.get_schemas(False) if benchmark == 'nds' else module.get_schemas()
                registrations = statements[2:2 + len(tables)]
                self.assertEqual(len(tables), len(registrations))
                for table, statement in zip(tables, registrations):
                    self.assertIn('`' + table + '` USING DELTA', statement)
                    self.assertIn("LOCATION 'file:///datasets/" + table + "'", statement)
                self.assertEqual(list(queries.values()), statements[2 + len(tables):])
                self.assertEqual(len(tables), sum(row[1].startswith('Register ') for row in rows[1:]))
                builder.enableHiveSupport.assert_not_called()

    def test_uc_overrides_legacy_hive_setup_and_accepts_custom_namespace(self):
        spark, builder, _ = self.run_runner('nds', unity_catalog=True,
                                          delta_unmanaged=True, hive_external=True,
                                          uc_catalog='custom', uc_schema='custom-schema')
        builder.enableHiveSupport.assert_not_called()
        builder.config.assert_not_called()
        spark.catalog.setCurrentDatabase.assert_not_called()
        self.assertEqual('USE `custom`.`custom-schema`', spark.sql.call_args_list[1].args[0])

    def test_non_uc_nds_managed_delta_still_uses_hive_and_warehouse(self):
        spark, builder, _ = self.run_runner('nds')
        builder.enableHiveSupport.assert_called_once()
        builder.config.assert_called_once_with('spark.sql.warehouse.dir', 'file:///datasets')
        spark.sql.assert_not_called()

    def test_non_uc_delta_registration_is_unchanged(self):
        for benchmark in self.runners:
            with self.subTest(benchmark=benchmark):
                options = {'delta_unmanaged': True} if benchmark == 'nds' else {}
                spark, _, _ = self.run_runner(benchmark, **options)
                statements = [call.args[0] for call in spark.sql.call_args_list]
                self.assertTrue(statements)
                self.assertTrue(all(statement.startswith('CREATE TABLE IF NOT EXISTS ') for statement in statements))
                self.assertTrue(all('`ab`.' not in statement and 'USE ' not in statement for statement in statements))

    def test_non_uc_parquet_still_uses_existing_setup(self):
        for benchmark, module in self.runners.items():
            with self.subTest(benchmark=benchmark), patch.object(module, 'setup_tables', return_value=[]) as setup:
                spark, _, _ = self.run_runner(benchmark, input_format='parquet')
                setup.assert_called_once()
                spark.sql.assert_not_called()


if __name__ == '__main__':
    unittest.main()
