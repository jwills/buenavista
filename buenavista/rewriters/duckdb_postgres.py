from ..rewrite import Rewriter
from . import bv_dialects

class DuckDBPostgresRewriter(Rewriter):
    def rewrite(self, sql: str) -> str:
        if sql == "select pg_catalog.version()":
            return "SELECT 'PostgreSQL 9.3' as version"
        return super().rewrite(sql)
    
rewriter = DuckDBPostgresRewriter(bv_dialects.BVPostgres(), bv_dialects.BVDuckDB())