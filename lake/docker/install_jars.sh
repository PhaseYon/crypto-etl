#!/usr/bin/env bash
# Copy resolved connector jars into /opt/lake/jars, skipping libraries Spark already ships
# (e.g. Jackson, Parquet, Hadoop client, log4j) so only one version of each is on the
# classpath. Then register them in spark-defaults.conf.
set -euo pipefail

src="$1"
dest=/opt/lake/jars
mkdir -p "$dest" /opt/spark/conf

for jar in "$src"/*.jar; do
  file=$(basename "$jar" .jar)
  artifact=${file#*_}                          # drop the "group.id_" prefix Ivy adds
  name=$(echo "$artifact" | sed -E 's/-[0-9].*$//')   # drop the version suffix
  if compgen -G "/opt/spark/jars/${name}-[0-9]*.jar" > /dev/null; then
    echo "skip   $file (Spark ships $(basename "$(compgen -G "/opt/spark/jars/${name}-[0-9]*.jar" | head -1)"))"
  else
    echo "add    $file"
    cp "$jar" "$dest/"
  fi
done

echo "spark.jars $(ls "$dest"/*.jar | paste -sd, -)" >> /opt/spark/conf/spark-defaults.conf
