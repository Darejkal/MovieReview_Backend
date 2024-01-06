FROM python:3.9.18-slim-bookworm AS base
FROM base AS producer
RUN pip install kafka-python flask gunicorn 

FROM producer AS consumer
RUN pip install pandas sentence-transformers

FROM base AS tester-producer
RUN pip install requests

FROM base AS spark-package
RUN  python3 -m venv /venv
RUN /venv/bin/pip install pandas sentence-transformers venv-pack
RUN /venv/bin/pip install pandas sentence-transformers venv-pack
RUN . /venv/bin/activate && venv-pack -o /pyspark_venv.tar.gz
RUN chmod 755 /pyspark_venv.tar.gz

FROM spark:3.5.0-scala2.12-java17-python3-ubuntu AS spark-sbert2
USER root
RUN pip install pyspark py4j pandas pyarrow numpy grpcio grpcio-status googleapis-common-protos sentence-transformers
COPY --from=spark-package /pyspark_venv.tar.gz /opt/spark/work-dir/pyspark_venv.tar.gz
RUN curl https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar\
    --output  /opt/spark/jars/spark-sql-kafka.jar &&\
    curl https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar\
    --output  /opt/spark/jars/kafka-clients.jar &&\
    curl https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar\
    --output  /opt/spark/jars/commons-pool.jar &&\
    curl https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10-assembly_2.12/3.5.0/spark-streaming-kafka-0-10-assembly_2.13-3.5.0.jar\
    --output  /opt/spark/jars/spark-streaming-kafka.jar &&\
    curl https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar\
    --output  /opt/spark/jars/spark-token-provider.jar
RUN chown spark -R /opt/spark
User spark