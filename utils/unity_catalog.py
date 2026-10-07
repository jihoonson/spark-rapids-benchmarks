#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

"""Initialize a benchmark namespace and external Delta tables in Unity Catalog."""

import time


def _quote_identifier(value):
    return '`' + value.replace('`', '``') + '`'


def _quote_string(value):
    # Spark SQL string literals use backslash escapes, including for apostrophes.
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def setup_unity_catalog(spark_session, input_prefix, table_names, app_id,
                        catalog='ab', schema='nds'):
    """Create/select a schema and register source directories without replacing tables.

    The catalog, connection, credentials and managed storage are prepared externally.
    Spark/Delta/UC own validation and errors; this function does not catch them.
    Return registration timings in the runners' existing CSV format.
    """
    namespace = _quote_identifier(catalog) + '.' + _quote_identifier(schema)
    spark_session.sql('CREATE SCHEMA IF NOT EXISTS ' + namespace)
    spark_session.sql('USE ' + namespace)
    timings = []
    for table_name in table_names:
        start = int(time.time() * 1000)
        identifier = namespace + '.' + _quote_identifier(table_name)
        location = input_prefix.rstrip('/') + '/' + table_name
        statement = ('CREATE TABLE IF NOT EXISTS ' + identifier +
                     ' USING DELTA LOCATION ' + _quote_string(location))
        print(statement)
        spark_session.sql(statement)
        elapsed = int(time.time() * 1000) - start
        print('====== Registering for table {} ======'.format(table_name))
        print('Time taken: {} millis for table {}'.format(elapsed, table_name))
        timings.append((app_id, 'Register ' + table_name, elapsed))
    return timings
