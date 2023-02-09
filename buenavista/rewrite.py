from typing import Callable, DecoratedCallable, Dict

import sqlglot
import sqlglot.expressions as exp

from .core import Connection, Session, QueryResult


class Rewriter:
    def __init__(self, prefix: str):
        self.prefix = prefix
        self.relations = {}

    def relation(self, name: str) -> Callable[[DecoratedCallable], DecoratedCallable]:
        def decorator(func: DecoratedCallable) -> DecoratedCallable:
            self.relations[name] = func
            return func

        return decorator

    def rewrite(self, sql: str) -> str:
        stmts = sqlglot.parse(sql)
        ret = []
        for stmt in stmts:
            ret.append(self.rewrite_one(stmt))
        return ";\n".join(s.sql() for s in ret)

    def rewrite_one(self, expression: exp.Expression) -> exp.Expression:
        def _expand(node: exp.Expression):
            if isinstance(node, exp.Table):
                name = exp.table_name(node)
                if name.startswith(self.prefix):
                    source = self.relations.get(name[len(self.prefix) :])
                    if source:
                        subquery = exp.paren(exp.maybe_parse(source()))
                        if node.alias:
                            subquery = exp.alias_(subquery, node.alias)
                        subquery.comments = [f"source: {name}"]
                        return subquery
            return node

        return expression.transform(_expand, copy=True)


class RewriteConnection(Connection):
    def __init__(self, rewriter: Rewriter, delegate: Connection):
        super().__init__()
        self.rewriter = rewriter
        self.delegate = delegate

    def new_session(self) -> Session:
        return RewriteSession(self.rewriter, self.delegate.new_session())

    def parameters(self) -> Dict[str, str]:
        return self.delegate.parameters()


class RewriteSession(Session):
    def __init__(self, rewriter: Rewriter, delegate: Session):
        self.rewriter = rewriter
        self.delegate = delegate

    def cursor(self):
        # TODO: rewrite cursor?
        return self.delegate.cursor()

    def close(self):
        self.delegate.close()

    def execute_sql(self, sql: str, params=None) -> QueryResult:
        sql = self.rewriter.rewrite(sql)
        return self.delegate.execute_sql(sql, params)

    def in_transaction(self) -> bool:
        return self.delegate.in_transaction()

    def load_df_function(self, table: str):
        return self.delegate.load_df_function(table)
