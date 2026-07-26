set -euo pipefail

DATABASE_NAME="orderflow_test"
DATABASE_USER="orderflow"

docker compose up -d postgres

until docker compose exec -T postgres \
  pg_isready \
  -U "${DATABASE_USER}" \
  -d postgres \
  >/dev/null 2>&1
do
  sleep 1
done

DATABASE_EXISTS="$(
  docker compose exec -T postgres \
    psql \
    -U "${DATABASE_USER}" \
    -d postgres \
    -tAc "
      SELECT 1
      FROM pg_database
      WHERE datname = '${DATABASE_NAME}'
    "
)"

if [[ "${DATABASE_EXISTS}" == "1" ]]; then
  echo "Database ${DATABASE_NAME} already exists"
else
  docker compose exec -T postgres \
    psql \
    -U "${DATABASE_USER}" \
    -d postgres \
    -v ON_ERROR_STOP=1 \
    -c "
      CREATE DATABASE ${DATABASE_NAME}
      OWNER ${DATABASE_USER}
    "

  echo "Database ${DATABASE_NAME} created"
fi
