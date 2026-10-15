# Spark runtime for the lake jobs: Spark 4.1 + Delta Lake + Kafka connector + S3A.
#
# Versions (all must agree):
#   Spark 4.1.3 (Scala 2.13, bundles Hadoop 3.4.2)
#   Delta 4.4.1, artifact delta-spark_4.1_2.13 (Delta's build for Spark 4.1)
#   spark-sql-kafka-0-10_2.13 4.1.3 (same version as Spark)
#   hadoop-aws 3.4.2 (must equal Spark's bundled Hadoop; pulls the matching AWS SDK v2 bundle)
FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu AS runtime

ARG SPARK_VERSION=4.1.3
ARG DELTA_VERSION=4.4.1
ARG HADOOP_VERSION=3.4.2

USER root

# Resolve connector jars at build time so containers never depend on Maven at runtime.
# spark-submit --packages resolves the full dependency tree; install_jars.sh keeps the
# jars Spark doesn't already ship and registers them in spark-defaults.conf.
COPY docker/resolve_jars.py docker/install_jars.sh /opt/lake/
RUN /opt/spark/bin/spark-submit \
        --conf spark.jars.ivy=/tmp/ivy \
        --packages "io.delta:delta-spark_4.1_2.13:${DELTA_VERSION},org.apache.spark:spark-sql-kafka-0-10_2.13:${SPARK_VERSION},org.apache.hadoop:hadoop-aws:${HADOOP_VERSION}" \
        /opt/lake/resolve_jars.py \
    && bash /opt/lake/install_jars.sh /tmp/ivy/jars \
    && rm -rf /tmp/ivy

# Make the bundled PySpark importable from the system Python (no second copy from pip)
RUN SITE=$(python3 -c "import site; print(site.getsitepackages()[0])") \
    && echo /opt/spark/python > "$SITE/spark.pth" \
    && ls /opt/spark/python/lib/py4j-*-src.zip >> "$SITE/spark.pth"

# delta-spark's Python API only; --no-deps keeps pip from installing a second PySpark
COPY requirements.txt /opt/lake/requirements.txt
RUN pip install --no-cache-dir --no-deps -r /opt/lake/requirements.txt

WORKDIR /opt/lake/app
COPY src ./src
ENV PYTHONPATH=/opt/lake/app/src \
    PYTHONUNBUFFERED=1

USER spark
ENTRYPOINT ["python3", "-m"]
CMD ["lake.jobs.bronze"]

# Test image: runtime + test dependencies + tests
FROM runtime AS test
USER root
COPY requirements-test.txt /opt/lake/requirements-test.txt
RUN pip install --no-cache-dir -r /opt/lake/requirements-test.txt
COPY tests ./tests
COPY pytest.ini ./pytest.ini
USER spark
ENTRYPOINT []
CMD ["python3", "-m", "pytest", "tests", "-q"]
