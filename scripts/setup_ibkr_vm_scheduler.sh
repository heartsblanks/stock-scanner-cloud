#!/usr/bin/env bash

set -euo pipefail

PROJECT_ID="${PROJECT_ID:-stock-scanner-490821}"
LOCATION="${LOCATION:-europe-west1}"
ZONE="${ZONE:-europe-west1-b}"
INSTANCE_NAME="${INSTANCE_NAME:-ibkr-bridge-vm}"
TIMEZONE="${TIMEZONE:-America/New_York}"
START_SCHEDULE="${START_SCHEDULE:-15 9 * * 1-5}"
STOP_SCHEDULE="${STOP_SCHEDULE:-0 17 * * 1-5}"
SERVICE_ACCOUNT_NAME="${SERVICE_ACCOUNT_NAME:-ibkr-vm-scheduler}"
SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_EMAIL:-${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com}"

START_JOB_ID="${START_JOB_ID:-ibkr-vm-start}"
STOP_JOB_ID="${STOP_JOB_ID:-ibkr-vm-stop}"

START_URI="https://compute.googleapis.com/compute/v1/projects/${PROJECT_ID}/zones/${ZONE}/instances/${INSTANCE_NAME}/start"
STOP_URI="https://compute.googleapis.com/compute/v1/projects/${PROJECT_ID}/zones/${ZONE}/instances/${INSTANCE_NAME}/stop"

echo "Using project: ${PROJECT_ID}"
echo "Using instance: ${INSTANCE_NAME}"
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
    --oauth-service-account-email="${SERVICE_ACCOUNT_EMAIL}" \
    --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform"
else
  gcloud scheduler jobs create http "${START_JOB_ID}" \
    --project="${PROJECT_ID}" \
    --location="${LOCATION}" \
    --schedule="${START_SCHEDULE}" \
    --time-zone="${TIMEZONE}" \
    --uri="${START_URI}" \
    --http-method=POST \
    --oauth-service-account-email="${SERVICE_ACCOUNT_EMAIL}" \
    --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform"
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
    --oauth-service-account-email="${SERVICE_ACCOUNT_EMAIL}" \
    --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform"
else
  gcloud scheduler jobs create http "${STOP_JOB_ID}" \
    --project="${PROJECT_ID}" \
    --location="${LOCATION}" \
    --schedule="${STOP_SCHEDULE}" \
    --time-zone="${TIMEZONE}" \
    --uri="${STOP_URI}" \
    --http-method=POST \
    --oauth-service-account-email="${SERVICE_ACCOUNT_EMAIL}" \
    --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform"
fi

echo
echo "IBKR VM scheduler jobs are configured:"
echo "- ${START_JOB_ID}: ${START_SCHEDULE} (${TIMEZONE})"
echo "- ${STOP_JOB_ID}: ${STOP_SCHEDULE} (${TIMEZONE})"
