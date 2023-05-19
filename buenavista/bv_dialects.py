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
            q = "SELECT DISTINCT catalog_name as Catalog FROM information_schema.schemata"
            if len(tokens) >= 3:
                if len(tokens) == 5:
                    like = tokens[2].replace(tokens[4], "")
                else:
                    like = tokens[2]
                q += " WHERE catalog_name LIKE " + like
            else:
                return q
        elif entity == "SCHEMAS":
            q = "SELECT DISTINCT schema_name as Schema FROM information_schema.schemata"
            if len(tokens) > 1 and tokens[1].upper() == "FROM":
                q += f" WHERE catalog_name = '{tokens[2]}'"
                if len(tokens) >= 5:
                    if len(tokens) == 7:
                        like = tokens[4].replace(tokens[6], "")
                    else:
                        like = tokens[4]
                    q += " AND schema_name LIKE " + like
            else:
                q += " WHERE catalog_name IN (SELECT current_database())"
                if len(tokens) >= 3:
                    if len(tokens) == 5:
                        like = tokens[2].replace(tokens[4], "")
                    else:
                        like = tokens[2]
                    q += " AND schema_name LIKE " + like
            return q
        elif entity == "TABLES":
            q = "SELECT DISTINCT table_name as Table from information_schema.tables"
            if len(tokens) > 1 and tokens[1].upper() == "FROM":
                q += f" WHERE table_schema = '{tokens[2]}'"
                if len(tokens) >= 5:
                    if len(tokens) == 7:
                        like = tokens[4].replace(tokens[6], "")
                    else:
                        like = tokens[4]
                    q += " AND table_name LIKE " + like
            else:
                q += " WHERE table_schema IN (SELECT current_schema())"
                if len(tokens) >= 3:
                    if len(tokens) == 5:
                        like = tokens[2].replace(tokens[4], "")
                    else:
                        like = tokens[2]
                    q += " AND table_name LIKE " + like
            return q
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
