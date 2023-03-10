# syntax=docker/dockerfile:1.4
FROM docker.io/python:3.11.1

ENV PYTHONUNBUFFERED=1
ENV TZ=UTC
ENV DEBIAN_FRONTEND=noninteractive
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

ENV TINI_VERSION="v0.19.0"
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

WORKDIR /src
COPY dev-requirements.txt .
RUN pip install -r dev-requirements.txt

COPY . .

RUN python setup.py build
RUN python setup.py install

EXPOSE 5433

ENTRYPOINT ["/tini", "--"]
CMD python -m buenavista.examples.duckdb_postgres
