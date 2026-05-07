#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

echo "=== News Intelligence Docker Setup ==="

check_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: '$1' is required but not installed." >&2
    exit 1
  fi
}

check_cmd docker

COMPOSE_CMD=""
if command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD="docker-compose"
elif docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD="docker compose"
else
  echo "ERROR: Docker Compose is required (docker-compose or 'docker compose')." >&2
  exit 1
fi

if [ ! -f Dockerfile ] || [ ! -f docker-compose.yml ]; then
  echo "ERROR: This script must be run from the project root containing Dockerfile and docker-compose.yml." >&2
  exit 1
fi

printf "Checking required files...\n"
for f in Dockerfile docker-compose.yml .github/workflows/ci-cd.yml; do
  if [ ! -f "$f" ]; then
    echo "ERROR: Missing file: $f" >&2
    exit 1
  fi
  printf "  - %s OK\n" "$f"
done

printf "\nValidating Docker Compose configuration...\n"
$COMPOSE_CMD config

printf "\nBuilding Docker images...\n"
$COMPOSE_CMD build

printf "\nStarting application containers...\n"
$COMPOSE_CMD up -d

printf "\nDocker containers started. Current status:\n"
$COMPOSE_CMD ps

printf "\nTo view the dashboard, run:\n"
printf "  docker compose --profile dashboard up --build -d\n"
printf "Then open: http://localhost:8080\n"
printf "\nIf you want to follow a local preview without Compose, run:\n"
printf "  python -m http.server 8000\n"
