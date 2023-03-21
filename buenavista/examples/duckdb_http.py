import os

import duckdb
from fastapi import FastAPI

from .. import bv_dialects, rewrite
from ..backends.duckdb import DuckDBConnection
from ..http import main

#### Rewriter setup/config
rewriter = rewrite.Rewriter(bv_dialects.BVTrino(), bv_dialects.BVDuckDB())


@rewriter.relation("system.jdbc.tables")
def jdbc_tables():
    return """
        SELECT table_catalog as table_cat
        , table_schema as table_schem
        , table_name
        , table_type
        , CAST(NULL AS VARCHAR) as remarks
        , user_defined_type_catalog as type_cat
        , user_defined_type_schema as type_schem
        , user_defined_type_name as type_name
        , self_referencing_column_name as self_referencing_col_name
        , reference_generation as ref_generation
        FROM information_schema.tables
    """


@rewriter.relation("system.jdbc.schemas")
def jdbc_schemas():
    return """
        SELECT catalog_name as table_catalog
        , schema_name as table_schem
        FROM information_schema.schemata
    """


@rewriter.relation("system.jdbc.catalogs")
def jdbc_catalogs():
    return (
        """SELECT DISTINCT catalog_name as table_cat FROM information_schema.schemata"""
    )


@rewriter.relation("system.jdbc.table_types")
def jdbc_table_types():
    return "SELECT * FROM VALUES ('TABLE'), ('VIEW') AS t(table_type)"


@rewriter.relation("system.jdbc.columns")
def jdbc_columns():
    return """
        SELECT table_catalog as table_cat
        , table_schema as table_schem
        , table_name
        , column_name
        , CAST(0 AS BIGINT) as data_type
        , data_type as type_name
        , CAST(0 AS BIGINT) as column_size
        , CAST(0 AS BIGINT) as buffer_length
        , CAST(NULL AS BIGINT) as decimal_digits
        , CAST(NULL AS BIGINT) as num_prec_radix
        , IF(is_nullable = 'YES', 1, 0) as nullable
        , CAST(NULL AS VARCHAR) as remarks
        , CAST(NULL AS VARCHAR) as column_def
        , CAST(0 AS BIGINT) as sql_data_type
        , CAST(NULL AS BIGINT) as sql_datetime_sub
        , character_octet_length as char_octet_length
        , ordinal_position
        , is_nullable
        , scope_catalog
        , scope_schema
        , scope_name as scope_table
        , CAST(0 AS BIGINT) as source_data_type
        , CAST(NULL AS VARCHAR) as is_autoincrement
        , CAST(NULL AS VARCHAR) as is_generatedcolumn
        FROM information_schema.columns
    """


@rewriter.relation("system.jdbc.procedures")
def jdbc_procedures():
    return """
        SELECT CAST(NULL AS VARCHAR) as procedure_cat
        , CAST(NULL AS VARCHAR) as procedure_schem
        , CAST(NULL AS VARCHAR) as procedure_name
        , CAST(NULL AS VARCHAR) as remarks
        , CAST(NULL AS BIGINT) as procedure_type
        , CAST(NULL AS VARCHAR) as specific_name
        WHERE false
    """


if __name__ == "__main__":
    import uvicorn

    # Setup DuckDB file and FastAPI app with Presto API
    if os.getenv("DUCKDB_FILE"):
        print("Loading DuckDB db: " + os.getenv("DUCKDB_FILE"))
        db = duckdb.connect(os.getenv("DUCKDB_FILE"))
    else:
        print("Using in-memory DuckDB")
        db = duckdb.connect()

    bv_host = "127.0.0.1"
    bv_port = 8080
    if "BUENAVISTA_HOST" in os.environ:
        bv_host = os.environ["BUENAVISTA_HOST"]
    if "BUENAVISTA_PORT" in os.environ:
        bv_port = int(os.environ["BUENAVISTA_PORT"])

    app = FastAPI()
    main.quacko(app, DuckDBConnection(db), rewriter)
    uvicorn.run(app, host=bv_host, port=bv_port, log_level="info")
