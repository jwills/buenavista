# syntax=docker/dockerfile:1.4
FROM docker.io/python:3.11.1

ENV PYTHONUNBUFFERED=1
ENV TZ=UTC
ENV DEBIAN_FRONTEND=noninteractive
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

ENV TINI_VERSION="v0.19.0"
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

RUN apt-get update -y && apt-get install -y --no-install-recommends \
	git

WORKDIR /src

COPY . .
#COPY ./docker/duckdb_server.py examples

RUN python setup.py build
RUN python setup.py install

EXPOSE 5433

ENTRYPOINT ["/tini", "--"]
#CMD PYTHONPATH=. ./docker-entrypoint.sh python3 examples/duckdb_server.py
CMD PYTHONPATH=. python3 examples/duckdb_server.py
