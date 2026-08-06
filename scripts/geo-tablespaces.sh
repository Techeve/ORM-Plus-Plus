#!/usr/bin/env bash
# Legt je Region einen Tablespace mit replica_placement an — die
# Vorbedingung für TestGeoResidenzPhysisch.
#
# Bewusst ein Skript und kein ORM++-Code: Replikatzahl und
# Placement-Blöcke sind eine Betriebsentscheidung. ORM++ prüft nur, dass
# der deklarierte Tablespace existiert, und bindet die Partition beim
# CREATE TABLE daran.
set -euo pipefail

CONTAINER=${CONTAINER:-ormpp-geo-eu-central}
CLOUD=${CLOUD:-cloud1}

regionen=(eu-central eu-southwest na)

for r in "${regionen[@]}"; do
  ts="ts_${r//-/_}"
  echo "==> $ts  ->  $CLOUD.$r"
  docker exec -i "$CONTAINER" bin/ysqlsh -h "$CONTAINER" -v ON_ERROR_STOP=1 <<SQL
DROP TABLESPACE IF EXISTS $ts;
CREATE TABLESPACE $ts WITH (replica_placement='{
  "num_replicas": 1,
  "placement_blocks": [
    {"cloud":"$CLOUD","region":"$r","zone":"$r-1a","min_num_replicas":1}
  ]}');
SQL
done

echo
echo "Fertig. Vorhandene Tablespaces:"
docker exec -i "$CONTAINER" bin/ysqlsh -h "$CONTAINER" \
  -c "SELECT spcname FROM pg_tablespace WHERE spcname LIKE 'ts_%' ORDER BY 1"
