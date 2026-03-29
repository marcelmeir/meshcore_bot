#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${SCRIPT_DIR}"
VENV_DIR="${PROJECT_DIR}/.venv"
CONFIG_EXAMPLE="${PROJECT_DIR}/config.example.yaml"
CONFIG_FILE="${PROJECT_DIR}/config.yaml"
SERVICE_NAME="meshcore-bot"
SYSTEMD_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

INSTALL_SERVICE=0
SKIP_APT=0
RUN_AS_USER="${SUDO_USER:-$USER}"

usage() {
  cat <<EOF
MeshCore Bot Installationsskript

Nutzung:
  ./install.sh [optionen]

Optionen:
  --with-service     Richtet zusaetzlich einen systemd-Dienst ein
  --skip-apt         Ueberspringt apt update/apt install
  --user NAME        Benutzer fuer den systemd-Dienst
  --help             Diese Hilfe anzeigen

Beispiele:
  ./install.sh
  ./install.sh --with-service
  sudo ./install.sh --with-service --user meshcorebot
EOF
}

log() {
  printf '[INFO] %s\n' "$*"
}

warn() {
  printf '[WARN] %s\n' "$*" >&2
}

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf '[ERROR] Benoetigtes Kommando fehlt: %s\n' "$1" >&2
    exit 1
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --with-service)
      INSTALL_SERVICE=1
      ;;
    --skip-apt)
      SKIP_APT=1
      ;;
    --user)
      shift
      if [[ $# -eq 0 ]]; then
        printf '[ERROR] --user benoetigt einen Namen\n' >&2
        exit 1
      fi
      RUN_AS_USER="$1"
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      printf '[ERROR] Unbekannte Option: %s\n' "$1" >&2
      usage >&2
      exit 1
      ;;
  esac
  shift
done

need_cmd python3

if [[ ${SKIP_APT} -eq 0 ]]; then
  if ! command -v apt >/dev/null 2>&1; then
    warn "apt wurde nicht gefunden. Paketinstallation wird uebersprungen."
  else
    log "Installiere Systempakete"
    sudo apt update
    sudo apt install -y python3 python3-venv python3-pip
  fi
fi

log "Pruefe Projektverzeichnis: ${PROJECT_DIR}"
cd "${PROJECT_DIR}"

if id -nG "${RUN_AS_USER}" | grep -qw dialout; then
  log "Benutzer ${RUN_AS_USER} ist bereits in der Gruppe dialout"
else
  log "Fuege Benutzer ${RUN_AS_USER} zur Gruppe dialout hinzu"
  sudo usermod -aG dialout "${RUN_AS_USER}"
  warn "Bitte spaeter einmal neu anmelden, damit dialout sicher aktiv ist."
fi

if [[ ! -d "${VENV_DIR}" ]]; then
  log "Erstelle virtuelle Python-Umgebung"
  python3 -m venv "${VENV_DIR}"
else
  log "Virtuelle Umgebung existiert bereits"
fi

log "Installiere Python-Abhaengigkeiten"
"${VENV_DIR}/bin/python" -m pip install --upgrade pip
"${VENV_DIR}/bin/python" -m pip install -r "${PROJECT_DIR}/requirements.txt"

if [[ ! -f "${CONFIG_FILE}" && -f "${CONFIG_EXAMPLE}" ]]; then
  log "Lege config.yaml aus config.example.yaml an"
  cp "${CONFIG_EXAMPLE}" "${CONFIG_FILE}"
else
  log "Bestehende config.yaml wird beibehalten"
fi

if [[ ${INSTALL_SERVICE} -eq 1 ]]; then
  log "Richte systemd-Dienst ein"
  sudo tee "${SYSTEMD_FILE}" >/dev/null <<EOF
[Unit]
Description=MeshCore Bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=${PROJECT_DIR}
ExecStart=${VENV_DIR}/bin/python ${PROJECT_DIR}/app.py
Restart=always
RestartSec=5
User=${RUN_AS_USER}
Environment=MESHCORE_BOT_CONFIG=${CONFIG_FILE}

[Install]
WantedBy=multi-user.target
EOF
  sudo systemctl daemon-reload
  sudo systemctl enable "${SERVICE_NAME}"
  sudo systemctl restart "${SERVICE_NAME}"
  log "systemd-Dienst ${SERVICE_NAME} wurde aktiviert und gestartet"
fi

cat <<EOF

Installation abgeschlossen.

Naechste Schritte:
1. Konfiguration pruefen: ${CONFIG_FILE}
2. Bot manuell starten:
   cd ${PROJECT_DIR}
   source .venv/bin/activate
   python app.py

Webinterface:
  http://<IP-DEINES-PI>:8080

EOF
