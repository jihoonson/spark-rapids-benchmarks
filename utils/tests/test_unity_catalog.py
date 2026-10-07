import contextlib
import io
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from unity_catalog import setup_unity_catalog


class UnityCatalogStartupTest(unittest.TestCase):
    def test_namespace_and_every_source_table_are_registered_idempotently(self):
        spark = Mock()
        with contextlib.redirect_stdout(io.StringIO()):
            rows = setup_unity_catalog(spark, 's3://datasets/nds-h/',
                                      ['lineitem', 'orders'], 'app-test', 'ab', 'nds-h')
            setup_unity_catalog(spark, 's3://datasets/nds-h/',
                                ['lineitem', 'orders'], 'app-test', 'ab', 'nds-h')
        statements = [call.args[0] for call in spark.sql.call_args_list]
        self.assertEqual(statements[:4], statements[4:])
        self.assertEqual('CREATE SCHEMA IF NOT EXISTS `ab`.`nds-h`', statements[0])
        self.assertEqual('USE `ab`.`nds-h`', statements[1])
        self.assertEqual("CREATE TABLE IF NOT EXISTS `ab`.`nds-h`.`lineitem` USING DELTA "
                         "LOCATION 's3://datasets/nds-h/lineitem'", statements[2])
        self.assertEqual(['Register lineitem', 'Register orders'], [row[1] for row in rows])
        self.assertTrue(all(row[0] == 'app-test' and row[2] >= 0 for row in rows))

    def test_custom_names_and_paths_are_escaped(self):
        spark = Mock()
        with contextlib.redirect_stdout(io.StringIO()):
            setup_unity_catalog(spark, "file:///data/o'brien", ["table`name"],
                                'app-test', 'catalog`name', 'custom-schema')
        statement = spark.sql.call_args_list[-1].args[0]
        self.assertIn('`catalog``name`.`custom-schema`.`table``name`', statement)
        self.assertIn("o\\'brien/table`name", statement)

    def test_native_registration_error_is_propagated_without_cleanup(self):
        error = RuntimeError('upstream UC storage rejection')
        spark = Mock()
        spark.sql.side_effect = [None, None, error]
        with contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(RuntimeError) as raised:
                setup_unity_catalog(spark, 'hdfs://datasets/nds', ['store_sales'], 'app-test')
        self.assertIs(error, raised.exception)
        self.assertEqual(3, spark.sql.call_count)

    def test_missing_catalog_error_stops_before_registration(self):
        error = RuntimeError('upstream catalog not found')
        spark = Mock()
        spark.sql.side_effect = error
        with self.assertRaises(RuntimeError) as raised:
            setup_unity_catalog(spark, 'file:///datasets', ['store_sales'], 'app-test')
        self.assertIs(error, raised.exception)
        self.assertEqual(1, spark.sql.call_count)


if __name__ == '__main__':
    unittest.main()
