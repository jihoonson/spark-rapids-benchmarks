# OSS Unity Catalog benchmarks

Both NDS and NDS-H power runners accept `--unity_catalog`, `--uc_catalog` (default
`ab`) and `--uc_schema` (default `nds` or `nds-h`). UC mode treats the input dataset
as Delta table directories. It connects through the Spark catalog configuration;
it does not start a UC server or prepare a catalog or managed storage.

Configure the UC connector, Delta extension, packages and authentication in the
Spark submission template. The Spark catalog binding must use the selected UC
catalog name: `spark.sql.catalog.ab=io.unitycatalog.spark.UCSingleCatalog`, with
connection settings under `spark.sql.catalog.ab.*`. For a custom catalog, change
those keys to the same name supplied by `--uc_catalog`. AB exports
`AB_UC_CATALOG` before sourcing the template so one template can support overrides.
When launching a runner directly through `shared/spark-submit-template`, export
that variable yourself if the template uses it.

Startup creates the selected schema with `IF NOT EXISTS`, selects that namespace,
then registers every standard table in `<input_prefix>/<table>` as an external
Delta table using `CREATE TABLE IF NOT EXISTS ... USING DELTA LOCATION ...`.
Existing entries are preserved, including their locations. Use separate schemas
when registering different datasets. Identifiers are quoted, including `nds-h`.

Queries run unchanged in that namespace. Named CREATE/CTAS without LOCATION can
create UC-managed tables according to upstream configuration and capabilities.
Storage access, managed locations, permissions, compatibility and unsupported
operations are handled by Spark, Delta and UC; their errors propagate through
the existing runner error handling. No new version or storage checks are added.
Registration persists across runs; only explicit workload SQL removes tables.

Without `--unity_catalog`, table setup and benchmark execution are unchanged.
Setup/cleanup SQL, iterations, profiling, output, and timings retain their existing
behavior. Registration timings use the existing `Register <table>` CSV rows.

Tests: `python3 -B -m unittest discover -s utils/tests -p 'test_unity_catalog*.py'`.
