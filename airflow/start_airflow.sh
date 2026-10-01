#!/usr/bin/env bash
# Local Airflow for one environment:  ./start_airflow.sh          -> dev  on :8080
#                                     RETAIL_ENV=prod ./start_airflow.sh -> prod on :8081
# Each environment gets its own AIRFLOW_HOME (metadata DB, logs, login), the
# same way dev and prod would be separate Airflow deployments.
set -euo pipefail
cd "$(dirname "$0")"

export RETAIL_ENV="${RETAIL_ENV:-dev}"
export AIRFLOW_HOME="$(pwd)/airflow_home/${RETAIL_ENV}"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__API__PORT="$([ "$RETAIL_ENV" = prod ] && echo 8081 || echo 8080)"
# macOS: forked task processes crash in Objective-C init / system proxy lookup
# when they use boto3 or requests, unless these are set.
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
export NO_PROXY="*"

export PATH="$(cd .. && pwd)/.venv/bin:$PATH"   # standalone spawns `airflow ...` subprocesses from PATH
AIRFLOW=airflow
$AIRFLOW db migrate > /dev/null
$AIRFLOW pools set redshift 1 "Serialize Redshift writers (serializable isolation)" > /dev/null
echo "Airflow ($RETAIL_ENV) -> http://localhost:${AIRFLOW__API__PORT}  (login: $AIRFLOW_HOME/simple_auth_manager_passwords.json.generated)"
exec $AIRFLOW standalone
