#!/usr/bin/env bash

set -euo pipefail

PROJECT_ID="${PROJECT_ID:-stock-scanner-490821}"
LOCATION="${LOCATION:-europe-west1}"
RUN_REGION="${RUN_REGION:-europe-west1}"
ZONE="${ZONE:-europe-west1-b}"
INSTANCE_NAME="${INSTANCE_NAME:-ibkr-bridge-vm}"
RUN_SERVICE_NAME="${RUN_SERVICE_NAME:-stock-scanner}"
RUN_BASE_URL="${RUN_BASE_URL:-}"
TIMEZONE="${TIMEZONE:-America/New_York}"
START_SCHEDULE="${START_SCHEDULE:-15 9 * * 1-5}"
STOP_SCHEDULE="${STOP_SCHEDULE:-0 17 * * 1-5}"
LOGIN_ALERT_SCHEDULE="${LOGIN_ALERT_SCHEDULE:-*/10 9-16 * * 1-5}"
SERVICE_ACCOUNT_NAME="${SERVICE_ACCOUNT_NAME:-ibkr-vm-scheduler}"
SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_EMAIL:-${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com}"

START_JOB_ID="${START_JOB_ID:-ibkr-vm-start}"
STOP_JOB_ID="${STOP_JOB_ID:-ibkr-vm-stop}"
LOGIN_ALERT_JOB_ID="${LOGIN_ALERT_JOB_ID:-ibkr-login-alert}"

if [[ -z "${RUN_BASE_URL}" ]]; then
  RUN_BASE_URL="$(gcloud run services describe "${RUN_SERVICE_NAME}" \
    --project="${PROJECT_ID}" \
    --region="${RUN_REGION}" \
    --format='value(status.url)')"
fi

START_URI="${RUN_BASE_URL}/scheduler/ibkr-vm-control"
STOP_URI="${RUN_BASE_URL}/scheduler/ibkr-vm-control"
LOGIN_ALERT_URI="${RUN_BASE_URL}/scheduler/ibkr-login-alert"

echo "Using project: ${PROJECT_ID}"
echo "Using instance: ${INSTANCE_NAME}"
echo "Using Cloud Run base URL: ${RUN_BASE_URL}"
echo "Using scheduler service account: ${SERVICE_ACCOUNT_EMAIL}"

if ! gcloud iam service-accounts describe "${SERVICE_ACCOUNT_EMAIL}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  echo "Creating service account ${SERVICE_ACCOUNT_NAME}..."
  gcloud iam service-accounts create "${SERVICE_ACCOUNT_NAME}" \
    --project="${PROJECT_ID}" \
    --display-name="IBKR VM Scheduler"
fi

echo "Granting Compute Instance Admin role..."
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
  --role="roles/compute.instanceAdmin.v1" \
  --condition=None \
  >/dev/null

echo "Granting Cloud Run Invoker role..."
gcloud run services add-iam-policy-binding "${RUN_SERVICE_NAME}" \
  --project="${PROJECT_ID}" \
  --region="${RUN_REGION}" \
  --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
  --role="roles/run.invoker" \
  >/dev/null

echo "Creating or updating start job..."
if gcloud scheduler jobs describe "${START_JOB_ID}" --project="${PROJECT_ID}" --location="${LOCATION}" >/dev/null 2>&1; then
  gcloud scheduler jobs update http "${START_JOB_ID}" \
    --project="${PROJECT_ID}" \
    --location="${LOCATION}" \
    --schedule="${START_SCHEDULE}" \
    --time-zone="${TIMEZONE}" \
    --uri="${START_URI}" \
    --http-method=POST \
    --update-headers="Content-Type=application/json" \
    --message-body='{"action":"start"}' \
    --oidc-service-account-email="${SERVICE_ACCOUNT_EMAIL}" \
    --oidc-token-audience="${RUN_BASE_URL}"
else
  gcloud scheduler jobs create http "${START_JOB_ID}" \
    --project="${PROJECT_ID}" \
    --location="${LOCATION}" \
    --schedule="${START_SCHEDULE}" \
    --time-zone="${TIMEZONE}" \
    --uri="${START_URI}" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --message-body='{"action":"start"}' \
    --oidc-service-account-email="${SERVICE_ACCOUNT_EMAIL}" \
    --oidc-token-audience="${RUN_BASE_URL}"
fi

echo "Creating or updating stop job..."
if gcloud scheduler jobs describe "${STOP_JOB_ID}" --project="${PROJECT_ID}" --location="${LOCATION}" >/dev/null 2>&1; then
  gcloud scheduler jobs update http "${STOP_JOB_ID}" \
    --project="${PROJECT_ID}" \
    --location="${LOCATION}" \
    --schedule="${STOP_SCHEDULE}" \
    --time-zone="${TIMEZONE}" \
    --uri="${STOP_URI}" \
    --http-method=POST \
    --update-headers="Content-Type=application/json" \
    --message-body='{"action":"stop"}' \
    --oidc-service-account-email="${SERVICE_ACCOUNT_EMAIL}" \
    --oidc-token-audience="${RUN_BASE_URL}"
else
  gcloud scheduler jobs create http "${STOP_JOB_ID}" \
    --project="${PROJECT_ID}" \
    --location="${LOCATION}" \
    --schedule="${STOP_SCHEDULE}" \
    --time-zone="${TIMEZONE}" \
    --uri="${STOP_URI}" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --message-body='{"action":"stop"}' \
    --oidc-service-account-email="${SERVICE_ACCOUNT_EMAIL}" \
    --oidc-token-audience="${RUN_BASE_URL}"
fi

echo "Creating or updating login alert job..."
if gcloud scheduler jobs describe "${LOGIN_ALERT_JOB_ID}" --project="${PROJECT_ID}" --location="${LOCATION}" >/dev/null 2>&1; then
  gcloud scheduler jobs update http "${LOGIN_ALERT_JOB_ID}" \
    --project="${PROJECT_ID}" \
    --location="${LOCATION}" \
    --schedule="${LOGIN_ALERT_SCHEDULE}" \
    --time-zone="${TIMEZONE}" \
    --uri="${LOGIN_ALERT_URI}" \
    --http-method=POST \
    --update-headers="Content-Type=application/json" \
    --message-body='{}' \
    --oidc-service-account-email="${SERVICE_ACCOUNT_EMAIL}" \
    --oidc-token-audience="${RUN_BASE_URL}"
else
  gcloud scheduler jobs create http "${LOGIN_ALERT_JOB_ID}" \
    --project="${PROJECT_ID}" \
    --location="${LOCATION}" \
    --schedule="${LOGIN_ALERT_SCHEDULE}" \
    --time-zone="${TIMEZONE}" \
    --uri="${LOGIN_ALERT_URI}" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --message-body='{}' \
    --oidc-service-account-email="${SERVICE_ACCOUNT_EMAIL}" \
    --oidc-token-audience="${RUN_BASE_URL}"
fi

echo
echo "IBKR VM scheduler jobs are configured:"
echo "- ${START_JOB_ID}: ${START_SCHEDULE} (${TIMEZONE})"
echo "- ${STOP_JOB_ID}: ${STOP_SCHEDULE} (${TIMEZONE})"
echo "- ${LOGIN_ALERT_JOB_ID}: ${LOGIN_ALERT_SCHEDULE} (${TIMEZONE})"
