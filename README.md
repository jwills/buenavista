# Buena Vista: A Programmable Postgres Proxy Server

Buena Vista is a Python library that provides a [socketserver](https://docs.python.org/3/library/socketserver.html)-based implementation
of the [Postgres wire protocol (PDF)](https://beta.pgcon.org/2014/schedule/attachments/330_postgres-for-the-wire.pdf).

I started working on this project in order to address a common issue that people had when they were using another
one of my Python projects, [dbt-duckdb](https://github.com/jwills/dbt-duckdb): when a long-running Python process
is operating on a [DuckDB](http://duckdb.org) database, you cannot connect to the DuckDB file using the CLI or
with a database query tool like [DBeaver](https://dbeaver.io/) to examine the state of the database, because each DuckDB file
may only be open by a single process at a time. The Buena Vista library makes it possible to work with a DuckDB database
from multiple different processes over the Postgres wire protocol, and the library makes it simple enough to run an example
that illustrates the idea locally:

```sh
pip3 install buenavista
python3 -m buenavista.backends.duckdb <optional_duckdb_file>
```

in order to start a Postgres server on `localhost:5433` backed by the DuckDB database file that you passed in at the command line
(or by an in-memory DuckDB database if you do not specify an argument.) You should be able to query the database via `psql` in
another window by running `psql -h localhost -p 5433` (no database/username/password arguments required) or by using the DBeaver
Postgres client connection.
