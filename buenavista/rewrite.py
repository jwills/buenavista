from typing import Any, Callable, Dict, TypeVar

import sqlglot
import sqlglot.expressions as exp

from .core import Connection, Session, QueryResult

DecoratedCallable = TypeVar("DecoratedCallable", bound=Callable[..., Any])


class Rewriter:
    def __init__(self, read, write):
        self._relations = {}
        self._read = read
        self._dialect = sqlglot.Dialect.get_or_raise(write)()

    def relation(self, name: str) -> Callable[[DecoratedCallable], DecoratedCallable]:
        def decorator(func: DecoratedCallable) -> DecoratedCallable:
            self._relations[name] = func
            return func

        return decorator

    def rewrite(self, sql: str) -> str:
        stmts = sqlglot.parse(sql, self._read)
        ret = []
        for stmt in stmts:
            ret.append(self.rewrite_one(stmt))
        return ";\n".join(self._dialect.generate(s) for s in ret)

    def rewrite_one(self, expression: exp.Expression) -> exp.Expression:
        def _expand(node: exp.Expression):
            if isinstance(node, exp.Table):
                name = exp.table_name(node)
                if name in self._relations:
                    source = self._relations[name]
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
        super().__init__()
        self.rewriter = rewriter
        self.delegate = delegate

    def cursor(self):
        # TODO: rewrite cursor?
        return self.delegate.cursor()

    def close(self):
        self.delegate.close()

    def execute_sql(self, sql: str, params=None) -> QueryResult:
        print("Rewrite input: " + sql)
        try:
            sql = self.rewriter.rewrite(sql)
        except sqlglot.errors.ParseError as e:
            print("sqlglot parse error: " + str(e))
            pass
        print("Rewritten to: " + sql)
        return self.delegate.execute_sql(sql, params)

    def in_transaction(self) -> bool:
        return self.delegate.in_transaction()

    def load_df_function(self, table: str):
        return self.delegate.load_df_function(table)


if __name__ == "__main__":
    rewriter = Rewriter()

    @rewriter.relation("schema.test")
    def test():
        return "SELECT 1 as a, 'foo' as b"

    print(rewriter.rewrite("SELECT * FROM schema.test"))
