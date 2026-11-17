#!/usr/bin/env bash
#
# Runs the test suite in throw-away containers.
#
# PHP runs in TYPO3's core-testing image. For functional tests, a database container
# is started on tmpfs with every durability feature switched off (no fsync, no
# doublewrite, no binlog): fast, and gone when the run ends. Never point this at a
# database you care about.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

SUITE="functional"
DBMS="postgres"
PHP_VERSION="8.1"

usage() {
    cat <<EOF
Usage: $(basename "$0") [-s <suite>] [-d <dbms>] [-p <php-version>] [-- <arguments>]

  -s <suite>   functional (default)  Run the functional tests. Arguments go to phpunit,
                                     e.g. -- --filter renameFile
               composer              Run composer in the PHP container. Arguments go to
                                     composer (default: install).
               lint                  Syntax-check all PHP files.
  -d <dbms>    postgres (default, as in production) or mariadb. Functional tests only.
  -p <version> PHP version of the test image, e.g. 8.1 (default), 8.2, 8.3.
  -h           Show this help.

Examples:
  $(basename "$0") -s composer -- update
  $(basename "$0")
  $(basename "$0") -d mariadb -- --filter DatabaseDriverTest
EOF
}

while getopts ":s:d:p:h" opt; do
    case "${opt}" in
        s) SUITE="${OPTARG}" ;;
        d) DBMS="${OPTARG}" ;;
        p) PHP_VERSION="${OPTARG}" ;;
        h) usage; exit 0 ;;
        *) usage >&2; exit 2 ;;
    esac
done
shift $((OPTIND - 1))

PHP_IMAGE="ghcr.io/typo3/core-testing-php${PHP_VERSION//./}:latest"
POSTGRES_IMAGE="postgres:16-alpine"
MARIADB_IMAGE="mariadb:10.11"

RUN_ID="fal-database-tests-$$"
DB_CONTAINER="${RUN_ID}-db"
COMPOSER_CACHE="${HOME}/.cache/composer"

TTY_ARGS=()
if [ -t 0 ] && [ -t 1 ]; then
    TTY_ARGS=(-it)
fi

run_php() {
    mkdir -p "${COMPOSER_CACHE}"
    docker run --rm "${TTY_ARGS[@]}" \
        --user "$(id -u):$(id -g)" \
        -e HOME=/tmp \
        -e COMPOSER_CACHE_DIR=/tmp/composer-cache \
        -v "${COMPOSER_CACHE}:/tmp/composer-cache" \
        -v "${ROOT_DIR}:${ROOT_DIR}" \
        -w "${ROOT_DIR}" \
        "$@"
}

cleanup() {
    docker rm -f "${DB_CONTAINER}" >/dev/null 2>&1 || true
    docker network rm "${RUN_ID}" >/dev/null 2>&1 || true
}

# wait_for <description> <command...>: runs the command inside the DB container until it succeeds.
wait_for() {
    local description="$1"
    shift
    for _ in $(seq 1 60); do
        if docker exec "${DB_CONTAINER}" "$@" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    echo "${description} did not become ready within 60 seconds. Container log:" >&2
    docker logs "${DB_CONTAINER}" >&2 || true
    return 1
}

start_database() {
    docker network create "${RUN_ID}" >/dev/null

    case "${DBMS}" in
        postgres)
            docker run -d --rm --name "${DB_CONTAINER}" --network "${RUN_ID}" \
                --tmpfs /var/lib/postgresql/data:rw,size=1g \
                -e POSTGRES_USER=funcu \
                -e POSTGRES_PASSWORD=funcp \
                -e POSTGRES_INITDB_ARGS=--no-sync \
                "${POSTGRES_IMAGE}" \
                -c fsync=off \
                -c synchronous_commit=off \
                -c full_page_writes=off \
                >/dev/null
            # Probing via TCP skips the image's socket-only init server.
            wait_for "PostgreSQL" pg_isready -h 127.0.0.1 -U funcu
            DB_ENV=(
                -e typo3DatabaseDriver=pdo_pgsql
                -e typo3DatabaseHost="${DB_CONTAINER}"
                -e typo3DatabasePort=5432
                -e typo3DatabaseName=func_test
                -e typo3DatabaseUsername=funcu
                -e typo3DatabasePassword=funcp
            )
            ;;
        mariadb)
            docker run -d --rm --name "${DB_CONTAINER}" --network "${RUN_ID}" \
                --tmpfs /var/lib/mysql:rw,size=1g \
                -e MARIADB_ROOT_PASSWORD=funcp \
                "${MARIADB_IMAGE}" \
                --innodb-flush-log-at-trx-commit=0 \
                --innodb-doublewrite=0 \
                --innodb-flush-method=nosync \
                --innodb-use-native-aio=0 \
                --sync-binlog=0 \
                --skip-log-bin \
                --max-allowed-packet=64M \
                >/dev/null
            # Probing via TCP skips the image's socket-only init server.
            wait_for "MariaDB" mariadb-admin ping -h 127.0.0.1 -uroot -pfuncp --silent
            DB_ENV=(
                -e typo3DatabaseDriver=mysqli
                -e typo3DatabaseHost="${DB_CONTAINER}"
                -e typo3DatabasePort=3306
                -e typo3DatabaseName=func_test
                -e typo3DatabaseUsername=root
                -e typo3DatabasePassword=funcp
            )
            ;;
        *)
            echo "Unknown DBMS \"${DBMS}\"." >&2
            usage >&2
            exit 2
            ;;
    esac
}

case "${SUITE}" in
    functional)
        if [ ! -x "${ROOT_DIR}/vendor/bin/phpunit" ]; then
            echo "vendor/bin/phpunit is missing. Run: $0 -s composer -- install" >&2
            exit 1
        fi
        trap cleanup EXIT
        start_database
        run_php --network "${RUN_ID}" "${DB_ENV[@]}" "${PHP_IMAGE}" \
            php -d memory_limit=1G vendor/bin/phpunit -c Build/phpunit/FunctionalTests.xml "$@"
        ;;
    composer)
        if [ $# -eq 0 ]; then
            set -- install
        fi
        run_php "${PHP_IMAGE}" composer "$@"
        ;;
    lint)
        run_php "${PHP_IMAGE}" sh -c \
            'find Classes Configuration Tests -name "*.php" -print0 | xargs -0 -n1 php -l >/dev/null'
        ;;
    *)
        echo "Unknown suite \"${SUITE}\"." >&2
        usage >&2
        exit 2
        ;;
esac
