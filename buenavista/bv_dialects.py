from sqlglot import exp
from sqlglot.dialects import DuckDB, Postgres, Trino
from sqlglot.tokens import TokenType

# Additional expressions I need
class ToISO8601(exp.Func):
    pass


class BVPostgres(Postgres):
    pass


# Trino-specific modifications
class BVTrino(Trino):
    class Tokenizer(Trino.Tokenizer):
        KEYWORDS = {
            **Trino.Tokenizer.KEYWORDS,
            "DEALLOCATE": TokenType.COMMAND,
        }

    class Parser(Trino.Parser):
        FUNCTIONS = {
            **Trino.Parser.FUNCTIONS,
            "TO_ISO8601": ToISO8601.from_arg_list,
        }


# DuckDB-specific modifications
def _duckdb_command_handler(self, expression):
    cmd = expression.this.upper()
    if cmd == "PREPARE":
        literal = expression.expression.this
        tokens = literal.split()
        stmt = tokens[0]
        if tokens[1].upper() not in ("FROM", "AS"):
            raise Exception("Badness badness")
        res = " ".join(tokens[2:])
        rest = BVDuckDB().generate(BVTrino().parse(res)[0])
        return f"PREPARE {stmt} AS " + rest
    elif cmd == "SHOW":
        literal = expression.expression.this
        tokens = literal.split()
        entity = tokens[0].upper()
        if entity == "CATALOGS":
            return "SELECT DISTINCT catalog_name as Catalog FROM information_schema.schemata"
            # TODO: LIKE
        elif entity == "SCHEMAS":
            return (
                "SELECT DISTINCT schema_name as Schema FROM information_schema.schemata"
            )
            # TODO: LIKE
        elif entity == "TABLES":
            return "SELECT DISTINCT table_name as Table from information_schema.tables"
            # TODO: LIKE
        elif entity == "COLUMNS" and tokens[1].upper() == "FROM":
            return f"DESCRIBE {tokens[2]}"
        elif entity == "TRANSACTION":
            return "SELECT 'read committed' as transaction_isolation"
        elif entity == "STANDARD_CONFORMING_STRINGS":
            return "SELECT 'on' as standard_conforming_strings"
        else:
            raise Exception("Unhandled SHOW command: " + literal)

    return expression.sql()


class BVDuckDB(DuckDB):
    class Generator(DuckDB.Generator):
        TRANSFORMS = {
            **DuckDB.Generator.TRANSFORMS,
            exp.CurrentTimestamp: lambda *_: "CURRENT_TIMESTAMP",
            ToISO8601: lambda self, e: f"STRFTIME({self.sql(e, 'this')}, '%Y-%m-%dT%H:%M:%S.%f%z')",
            exp.Command: _duckdb_command_handler,
        }
