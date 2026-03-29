from __future__ import annotations

"""MeshCore Bot with FastAPI web interface.

The application combines four responsibilities in a single runtime:
1. Manage the MeshCore companion connection and basic device configuration.
2. Run command-driven bot profiles on one or more MeshCore channels.
3. Mirror selected traffic to and from Telegram bridges.
4. Expose a browser UI for operations, diagnostics and configuration changes.

The code is intentionally kept in one file so deployment on a Raspberry Pi stays
simple. Comments and docstrings focus on the structural parts that are useful
when extending or operating the bot later on.
"""

import asyncio
import glob
import hashlib
import json
import logging
import os
import platform
import shutil
import sqlite3
import socket
import subprocess
import time
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import httpx
import uvicorn
import yaml
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field

try:
    from serial.tools import list_ports
except Exception:  # pragma: no cover
    list_ports = None

from meshcore import EventType, MeshCore

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = Path(os.environ.get("MESHCORE_BOT_CONFIG", BASE_DIR / "config.yaml"))
DB_PATH = BASE_DIR / "meshcore_bot.db"
MESHCORE_TEXT_LIMIT = 141

# Default configuration used for first start and for deep-merge fallbacks when
# older config files do not yet contain newly added sections.
DEFAULT_CONFIG = {
    "meshcore": {
        "connection": {
            "type": "serial",
            "port": "/dev/ttyACM0",
            "baudrate": 115200,
            "auto_detect": True,
            "debug": False,
            "auto_reconnect": True,
            "max_reconnect_attempts": 0,
            "address": None,
            "pin": None,
            "host": None,
        },
        "device": {
            "name": "Pi Mesh Bot",
            "tx_power": 20,
            "coords": {"lat": None, "lon": None},
            "set_time_on_connect": True,
        },
        "radio": {
            "freq": None,
            "bw": None,
            "sf": None,
            "cr": None,
        },
        "channel_sync": {
            "source": "device",
            "write_back_config": True,
            "max_channels": 8,
        },
        "channels": [
            {"channel_idx": 0, "name": "main", "secret_hex": None},
        ],
    },
    "bot": {
        "name": "MeshcoreBot",
        "enabled": True,
        "command_prefix": "!",
        "allowed_channels": [0],
        "ignore_own_messages": True,
        "poll_seconds": 5,
        "dedupe_window_seconds": 180,
        "help_text": "🤖 !help !ping !wetter !forecast !status !info",
        "custom_commands": {},
        "profiles": [
            {
                "name": "Standard",
                "enabled": True,
                "allowed_channels": [0],
                "custom_commands": {},
            }
        ],
        "auto_responses": {},
        "telegram_bridge": {
            "enabled": False,
            "bot_token": "",
            "chat_id": "",
            "mesh_channel_idx": 0,
            "mesh_to_telegram": True,
            "telegram_to_mesh": True,
            "poll_seconds": 4,
        },
        "telegram_bridges": [],
        "weather": {
            "enabled": True,
            "latitude": 49.407,
            "longitude": 7.479,
            "label": "Altenkirchen 66903",
            "units": "metric",
        },
        "maintenance": {
            "enabled": True,
            "cleanup_interval_minutes": 30,
            "message_retention_days": 14,
            "max_message_rows": 5000,
            "max_log_entries": 500,
            "vacuum_after_cleanup": True,
        },
    },
    "web": {
        "host": "0.0.0.0",
        "port": 8080,
        "cors": ["*"],
    },
}

INDEX_HTML = """
<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>MeshCore Bot</title>
  <style>
    :root {
      --bg: #07111f;
      --panel: #0f1a2d;
      --panel-soft: #182742;
      --card: rgba(15, 25, 43, 0.9);
      --card-border: rgba(148, 163, 184, 0.14);
      --field: rgba(8, 16, 31, 0.92);
      --field-border: #314462;
      --text: #ecf4ff;
      --muted: #9bb0d0;
      --accent: #49b2ff;
      --accent-2: #18c2a5;
      --accent-3: #ffcc66;
      --danger: #f97373;
      --shadow: 0 22px 50px rgba(0, 0, 0, 0.34);
      --radius: 22px;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Segoe UI", "Noto Sans", sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at 12% 0%, rgba(73, 178, 255, 0.24), transparent 26%),
        radial-gradient(circle at 88% 4%, rgba(24, 194, 165, 0.16), transparent 24%),
        radial-gradient(circle at 50% 100%, rgba(255, 204, 102, 0.08), transparent 28%),
        linear-gradient(180deg, #06101b 0%, #091321 42%, #060d17 100%);
      min-height: 100vh;
    }
    body::before {
      content: "";
      position: fixed;
      inset: 0;
      pointer-events: none;
      background-image:
        linear-gradient(rgba(255,255,255,0.025) 1px, transparent 1px),
        linear-gradient(90deg, rgba(255,255,255,0.025) 1px, transparent 1px);
      background-size: 36px 36px;
      mask-image: radial-gradient(circle at center, black 45%, transparent 92%);
      opacity: 0.18;
    }
    .wrap { max-width: 1480px; margin: 0 auto; padding: 28px 22px 46px; position: relative; z-index: 1; }
    .hero {
      display: flex;
      align-items: flex-end;
      justify-content: space-between;
      gap: 22px;
      padding: 30px 30px;
      border-radius: 32px;
      background:
        radial-gradient(circle at top right, rgba(73, 178, 255, 0.18), transparent 24%),
        linear-gradient(135deg, rgba(13, 22, 38, 0.98), rgba(20, 33, 55, 0.92));
      border: 1px solid rgba(147, 197, 253, 0.14);
      box-shadow: var(--shadow);
      margin-bottom: 22px;
      position: relative;
      overflow: hidden;
    }
    .hero::after {
      content: "";
      position: absolute;
      inset: auto -10% -40% auto;
      width: 280px;
      height: 280px;
      border-radius: 50%;
      background: radial-gradient(circle, rgba(24, 194, 165, 0.22), transparent 70%);
      pointer-events: none;
    }
    .hero h1 { margin: 0; font-size: clamp(30px, 5vw, 44px); letter-spacing: -0.03em; }
    .hero p { margin: 10px 0 0; max-width: 760px; line-height: 1.55; }
    .hero-badges {
      display: flex;
      flex-wrap: wrap;
      justify-content: flex-end;
      gap: 12px;
    }
    .badge {
      padding: 10px 15px;
      border-radius: 999px;
      background: linear-gradient(135deg, rgba(73, 178, 255, 0.12), rgba(24, 194, 165, 0.12));
      border: 1px solid rgba(73, 178, 255, 0.18);
      color: #e2f1ff;
      font-size: 13px;
      font-weight: 700;
      letter-spacing: 0.03em;
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.05);
    }
    .section-label {
      margin: 28px 0 12px;
      font-size: 12px;
      letter-spacing: 0.22em;
      text-transform: uppercase;
      color: #88a4d1;
      display: flex;
      align-items: center;
      gap: 12px;
    }
    .section-label::after {
      content: "";
      height: 1px;
      flex: 1;
      background: linear-gradient(90deg, rgba(73, 178, 255, 0.24), transparent);
    }
    .grid { display: grid; gap: 16px; }
    .grid-top { grid-template-columns: 1.05fr 1.05fr; margin-bottom: 6px; }
    .grid-primary { grid-template-columns: 1.1fr 1.1fr 1.35fr; }
    .grid-config-top { grid-template-columns: 1.1fr 0.9fr; }
    .grid-config { grid-template-columns: 1fr 1.08fr 1.08fr; }
    .grid-data { grid-template-columns: 1.1fr 1.15fr; }
    .grid-data-serial { grid-template-columns: 1fr 1.2fr; }
    .span-2 { grid-column: span 2; }
    .span-all { grid-column: 1 / -1; }
    .view { display: none; }
    .view.active { display: block; }
    .nav-tabs {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 20px;
    }
    .nav-tab {
      padding: 16px 18px;
      border-radius: 20px;
      border: 1px solid rgba(73, 178, 255, 0.16);
      background: linear-gradient(180deg, rgba(12, 21, 37, 0.96), rgba(10, 17, 30, 0.9));
      color: var(--text);
      text-align: left;
      box-shadow: none;
      margin-top: 0;
    }
    .nav-tab.active {
      background: linear-gradient(135deg, rgba(73, 178, 255, 0.22), rgba(24, 194, 165, 0.18));
      border-color: rgba(73, 178, 255, 0.34);
      box-shadow: 0 16px 30px rgba(10, 28, 56, 0.28);
    }
    .nav-tab strong {
      display: block;
      font-size: 16px;
      margin-bottom: 4px;
    }
    .nav-tab span {
      color: var(--muted);
      font-size: 13px;
      line-height: 1.45;
    }
    .card {
      background: var(--card);
      border-radius: var(--radius);
      padding: 20px;
      border: 1px solid var(--card-border);
      box-shadow: var(--shadow);
      backdrop-filter: blur(10px);
      position: relative;
      overflow: hidden;
      transition: transform 160ms ease, border-color 160ms ease, box-shadow 160ms ease;
    }
    .card::before {
      content: "";
      position: absolute;
      inset: 0 0 auto 0;
      height: 1px;
      background: linear-gradient(90deg, rgba(73, 178, 255, 0.45), rgba(24, 194, 165, 0.2), transparent);
      pointer-events: none;
    }
    .card:hover {
      transform: translateY(-2px);
      border-color: rgba(114, 174, 255, 0.22);
      box-shadow: 0 26px 56px rgba(0, 0, 0, 0.36);
    }
    .card h2 {
      margin: 0;
      font-size: 27px;
      letter-spacing: -0.03em;
    }
    .card-top {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 14px;
    }
    .card-top p {
      margin: 6px 0 0;
      color: var(--muted);
      font-size: 14px;
    }
    .pill {
      padding: 8px 12px;
      border-radius: 999px;
      background: rgba(22, 198, 183, 0.12);
      color: #c8fff4;
      font-size: 12px;
      font-weight: 700;
      white-space: nowrap;
      border: 1px solid rgba(22, 198, 183, 0.14);
    }
    .status-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
    }
    .status-item {
      padding: 12px 14px;
      border-radius: 14px;
      background: rgba(12, 21, 39, 0.84);
      border: 1px solid rgba(99, 119, 158, 0.18);
    }
    .status-label {
      display: block;
      color: var(--muted);
      font-size: 11px;
      margin-bottom: 4px;
      text-transform: uppercase;
      letter-spacing: 0.12em;
    }
    .status-value {
      font-size: 18px;
      font-weight: 700;
      word-break: break-word;
    }
    label {
      display: block;
      margin-top: 12px;
      font-size: 13px;
      color: #a9bcda;
      font-weight: 600;
    }
    input, textarea, select, button {
      width: 100%;
      margin-top: 8px;
      padding: 12px 14px;
      border-radius: 16px;
      border: 1px solid var(--field-border);
      background: var(--field);
      color: var(--text);
      font-size: 15px;
      transition: border-color 140ms ease, box-shadow 140ms ease, transform 140ms ease;
    }
    textarea { resize: vertical; min-height: 118px; line-height: 1.5; }
    input:focus, textarea:focus, select:focus {
      outline: none;
      border-color: rgba(57, 160, 255, 0.9);
      box-shadow: 0 0 0 3px rgba(57, 160, 255, 0.16);
    }
    button {
      cursor: pointer;
      border: none;
      margin-top: 14px;
      font-weight: 700;
      background: linear-gradient(135deg, var(--accent), #2670f0);
      box-shadow: 0 14px 28px rgba(37, 99, 235, 0.24);
    }
    button:hover { transform: translateY(-1px); }
    button.secondary {
      background: linear-gradient(135deg, #2b354a, #1a2436);
      box-shadow: none;
      border: 1px solid rgba(148, 163, 184, 0.15);
    }
    button.ghost {
      background: transparent;
      box-shadow: none;
      border: 1px dashed rgba(148, 163, 184, 0.28);
      color: #cfe1ff;
    }
    .button-row {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
      margin-top: 14px;
    }
    .button-row button { margin-top: 0; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
    .log {
      background: rgba(7, 13, 26, 0.92);
      border-radius: 18px;
      padding: 14px;
      overflow: auto;
      max-height: 360px;
      white-space: pre-wrap;
      border: 1px solid rgba(99, 119, 158, 0.16);
      font-family: "SFMono-Regular", Consolas, monospace;
      font-size: 13px;
      line-height: 1.5;
    }
    .command-list {
      margin-top: 8px;
      padding: 14px;
      border-radius: 18px;
      background: rgba(7, 13, 26, 0.92);
      border: 1px solid rgba(99, 119, 158, 0.16);
      color: #d9e7ff;
      line-height: 1.8;
      font-family: "SFMono-Regular", Consolas, monospace;
      font-size: 14px;
    }
    .stack { display: grid; gap: 12px; }
    .profile-list { display: grid; gap: 12px; margin-top: 10px; }
    .profile-card {
      padding: 16px;
      border-radius: 20px;
      background:
        linear-gradient(180deg, rgba(10, 18, 32, 0.98), rgba(8, 15, 28, 0.92));
      border: 1px solid rgba(99, 119, 158, 0.18);
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.03);
    }
    .profile-card h3 {
      margin: 0 0 10px;
      font-size: 17px;
      letter-spacing: -0.02em;
    }
    .profile-actions {
      display: flex;
      justify-content: flex-end;
      margin-top: 10px;
    }
    .profile-actions button { margin-top: 0; }
    .grid-bridge-note {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 12px;
      align-items: end;
    }
    .muted { color: var(--muted); font-size: 14px; }
    .hint {
      margin-top: 10px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.52;
    }
    .guide {
      margin-top: 14px;
      padding: 14px 16px;
      border-radius: 18px;
      background: linear-gradient(180deg, rgba(15, 28, 48, 0.88), rgba(10, 18, 32, 0.84));
      border: 1px solid rgba(99, 119, 158, 0.18);
      color: #dbe8ff;
      font-size: 13px;
      line-height: 1.6;
    }
    .guide strong {
      display: block;
      margin-bottom: 6px;
      color: #f2f7ff;
      letter-spacing: 0.01em;
    }
    .segmented {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
      margin-top: 8px;
    }
    @media (max-width: 1120px) {
      .grid-primary, .grid-config, .grid-config-top, .grid-data, .grid-data-serial { grid-template-columns: 1fr; }
      .grid-top { grid-template-columns: 1fr; }
      .nav-tabs { grid-template-columns: 1fr; }
      .span-2, .span-all { grid-column: auto; }
      .hero { align-items: flex-start; flex-direction: column; }
      .hero-badges { justify-content: flex-start; }
    }
    @media (max-width: 640px) {
      .wrap { padding: 18px 14px 28px; }
      .hero { padding: 20px; border-radius: 24px; }
      .card { padding: 16px; }
      .status-grid, .row, .button-row { grid-template-columns: 1fr; }
      .grid-bridge-note { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <div>
        <h1>MeshCore Bot Control</h1>
        <p class="muted">Verbindung, Bot-Kommandos, Channels und Diagnose an einem Ort. Die wichtigsten Aktionen stehen oben, tiefergehende Konfiguration darunter.</p>
      </div>
      <div class="hero-badges">
        <div class="badge">Serial Companion</div>
        <div class="badge">WebGUI</div>
        <div class="badge">Channel Sync</div>
      </div>
    </div>

    <div class="nav-tabs">
      <button id="tab-overview" class="nav-tab active" type="button" onclick="switchView('overview')">
        <strong>Uebersicht</strong>
        <span>Pi- und Heltec-Statistiken, Live-Status und letzte Nachrichten.</span>
      </button>
      <button id="tab-config" class="nav-tab" type="button" onclick="switchView('config')">
        <strong>Konfiguration</strong>
        <span>Verbindung, Versand, Bot-Profile, Geraet, Channels und Bridges.</span>
      </button>
      <button id="tab-debug" class="nav-tab" type="button" onclick="switchView('debug')">
        <strong>Debug</strong>
        <span>Live-Logs, Serial-Trace und direkter UART-Zugriff.</span>
      </button>
    </div>

    <section id="view-overview" class="view active">
      <div class="section-label">Uebersicht</div>
      <div class="grid grid-top">
        <div class="card">
          <div class="card-top">
            <div>
              <h2>Raspberry Pi</h2>
              <p>Wichtige Systemmetriken des Hosts.</p>
            </div>
            <div class="pill">Pi</div>
          </div>
          <div id="pi-metrics" class="status-grid">Lade...</div>
          <div class="button-row">
            <button onclick="refreshStatus()">Metriken neu laden</button>
            <button class="secondary" onclick="restartPi()">Pi neu starten</button>
          </div>
        </div>

        <div class="card">
          <div class="card-top">
            <div>
              <h2>Heltec / LoRa</h2>
              <p>Geraeteinfos, Batterie und Funkstatistiken.</p>
            </div>
            <div class="pill">LoRa</div>
          </div>
          <div id="device-metrics" class="status-grid">Lade...</div>
          <div class="button-row">
            <button onclick="refreshStatus()">Werte aktualisieren</button>
            <button class="secondary" onclick="restartDevice()">Heltec neu starten</button>
          </div>
        </div>
      </div>

      <div class="grid grid-data">
        <div class="card">
          <div class="card-top">
            <div>
              <h2>Status</h2>
              <p>Live-Status der Verbindung und des Bots.</p>
            </div>
            <div class="pill">Live</div>
          </div>
          <div id="status" class="status-grid">Lade...</div>
          <div class="button-row">
            <button onclick="refreshStatus()">Aktualisieren</button>
            <button class="secondary" onclick="testConnection()">Verbindung testen</button>
          </div>
        </div>

        <div class="card">
          <div class="card-top">
            <div>
              <h2>Letzte Nachrichten</h2>
              <p>Empfangene und gesendete Meldungen aus der lokalen Historie.</p>
            </div>
            <div class="pill">History</div>
          </div>
          <div id="messages" class="log"></div>
        </div>
      </div>
    </section>

    <section id="view-config" class="view">
      <div class="section-label">Konfiguration</div>
      <div class="card" style="margin-bottom: 16px;">
        <div class="card-top">
          <div>
            <h2>Kurzanleitung</h2>
            <p>Die wichtigsten Konfigurationsmoeglichkeiten kurz erklaert.</p>
          </div>
          <div class="pill">Guide</div>
        </div>
        <div class="guide">
          <strong>So ist die Seite aufgebaut</strong>
          `Nachricht senden` und `Verbindung` sind fuer den schnellen Betrieb gedacht. Darunter folgen die dauerhaften Einstellungen fuer Bot, Geraet, Channels und Telegram-Bridges.
        </div>
        <div class="guide">
          <strong>Typischer Ablauf fuer die Ersteinrichtung</strong>
          Zuerst den richtigen seriellen Port und die Baudrate setzen, dann den Bot aktivieren, anschliessend Geraetename und Funkparameter pruefen und zum Schluss Channels sowie optionale Telegram-Bridges konfigurieren.
        </div>
        <div class="guide">
          <strong>Wichtig zu wissen</strong>
          Aenderungen in den Konfigurationskarten werden erst wirksam, wenn du sie ueber den jeweiligen Button speicherst oder ans Geraet sendest. Nicht jede Einstellung wird automatisch sofort auf den Heltec geschrieben.
        </div>
      </div>
      <div class="grid grid-config-top">
        <div class="card">
          <div class="card-top">
            <div>
              <h2>Nachricht senden</h2>
              <p>Direkter Versand an einen Channel vom Webinterface aus.</p>
            </div>
            <div class="pill">TX</div>
          </div>
          <label>Channel</label>
          <input id="send-channel" type="number" value="0" />
          <label>Text</label>
          <textarea id="send-text" rows="5" placeholder="Hallo Mesh"></textarea>
          <div class="button-row">
            <button onclick="sendMessage()">Senden</button>
            <button class="secondary" onclick="loadMessages()">Verlauf aktualisieren</button>
          </div>
          <div class="guide">
            <strong>Wofuer ist das?</strong>
            Hier sendest du manuell eine Nachricht in einen MeshCore-Channel. Das ist hilfreich zum Testen, fuer Durchsagen oder um zu pruefen, ob der Bot und die Verbindung gerade sauber arbeiten.
          </div>
        </div>

        <div class="card">
          <div class="card-top">
            <div>
              <h2>Verbindung</h2>
              <p>Port-Erkennung, Baudrate und Verbindungswahl.</p>
            </div>
            <div class="pill">Serial</div>
          </div>
          <label>Erkannte serielle Ports</label>
          <select id="serial-port"></select>
          <label>Manueller Port</label>
          <input id="serial-port-manual" placeholder="/dev/ttyACM0 oder /dev/ttyUSB0" />
          <div class="row">
            <div><label>Baudrate</label><input id="serial-baudrate" type="number" value="115200" /></div>
            <div><label>Auto-Erkennung</label><select id="serial-auto-detect"><option value="true">ja</option><option value="false">nein</option></select></div>
          </div>
          <div class="button-row">
            <button onclick="refreshConnections()">Ports neu laden</button>
            <button class="secondary" onclick="saveSerialConnection()">Verbindung speichern</button>
          </div>
          <div class="hint">Wenn keine Ports erkannt werden, kannst du den Pfad wie `/dev/ttyACM0` manuell eintragen.</div>
          <div class="guide">
            <strong>Wofuer ist das?</strong>
            Hier legst du fest, ueber welchen seriellen Port der Raspberry Pi mit dem Heltec spricht. `Auto-Erkennung` ist praktisch beim ersten Test, ein fest gesetzter Port ist im Dauerbetrieb oft stabiler und vorhersagbarer.
          </div>
        </div>
      </div>

      <div class="grid grid-config">
        <div class="card">
          <div class="card-top">
            <div>
              <h2>Bot-Konfiguration</h2>
              <p>Globale Bot-Einstellungen und gemeinsame Zusatzbefehle.</p>
            </div>
            <div class="pill">Bot</div>
          </div>
          <label>Bot aktiviert</label>
          <select id="bot-enabled"><option value="true">ja</option><option value="false">nein</option></select>
          <label>Prefix</label>
          <input id="bot-prefix" value="!" />
          <label>Abrufintervall Nachrichten (Sekunden)</label>
          <input id="bot-poll-seconds" type="number" min="0.5" step="0.5" value="5" />
          <label>Feste Systembefehle</label>
          <div id="builtin-command-list" class="command-list">Lade feste Befehle...</div>
          <div class="hint">Diese Befehle sind fest eingebaut und koennen nicht ueber das Webinterface ueberschrieben werden. Anpassbar sind nur die zugrunde liegenden Werte: `Prefix` hier in der Bot-Konfiguration und der Wetter-Standort in `config.yaml` unter `bot.weather`.</div>
          <label>Gemeinsame Zusatzbefehle</label>
          <textarea id="bot-custom-commands" rows="7" placeholder="hallo=Hallo zusammen&#10;club=Treffen heute 19 Uhr&#10;karte=https://beispiel.de"></textarea>
          <div class="hint">Diese Zusatzbefehle gelten fuer alle Profile. Eine Antwort pro Zeile im Format `befehl=antwort`.</div>
          <button onclick="saveBotConfig()">Bot speichern</button>
          <div class="guide">
            <strong>Wofuer ist das?</strong>
            Hier steuerst du das Grundverhalten des Bots. `Prefix` bestimmt, wie Befehle anfangen, `Abrufintervall` beeinflusst, wie aggressiv nach neuen Nachrichten geschaut wird, und `Zusatzbefehle` sind einfache Textantworten ohne weitere Logik. Die festen Befehle `!help`, `!ping`, `!wetter`, `!forecast`, `!status` und `!info` bleiben aktiv und koennen hier nicht ersetzt werden.
          </div>
        </div>

        <div class="card span-2">
          <div class="card-top">
            <div>
              <h2>Bot-Profile</h2>
              <p>Mehrere logische Bots auf unterschiedlichen MeshCore-Channels.</p>
            </div>
            <div class="pill">Profiles</div>
          </div>
          <div id="bot-profiles" class="profile-list"></div>
          <button class="secondary" onclick="addBotProfile()">Weiteres Profil</button>
          <div class="hint">Jedes Profil kann eigene Channels und eigene Zusatzbefehle haben. Der erste passende Bot fuer einen Channel verarbeitet die Nachricht.</div>
          <div class="guide">
            <strong>Wofuer ist das?</strong>
            Mit Profilen kannst du mehrere logische Bots in einer Instanz betreiben. So kann z. B. ein Channel andere Befehle und Antworten haben als ein zweiter, ohne dass du einen zweiten Raspberry Pi oder einen zweiten Prozess brauchst.
          </div>
        </div>

        <div class="card">
          <div class="card-top">
            <div>
              <h2>Geräte-Konfiguration</h2>
              <p>Name, TX-Power und Radio-Parameter auf das Gerät schreiben.</p>
            </div>
            <div class="pill">Device</div>
          </div>
          <label>Name</label>
          <input id="dev-name" placeholder="Pi Mesh Bot" />
          <label>TX Power</label>
          <input id="dev-tx" type="number" placeholder="20" />
          <div class="row">
            <div><label>Frequenz (MHz)</label><input id="radio-freq" placeholder="869.525" /></div>
            <div><label>Bandbreite (kHz)</label><input id="radio-bw" placeholder="125" /></div>
          </div>
          <div class="row">
            <div><label>SF</label><input id="radio-sf" placeholder="11" /></div>
            <div><label>CR</label><input id="radio-cr" placeholder="5" /></div>
          </div>
          <button onclick="applyDeviceConfig()">Ans Gerät senden</button>
          <div class="guide">
            <strong>Wofuer ist das?</strong>
            Diese Karte schreibt Basiswerte direkt auf den Heltec. Name, TX-Power und Radio-Parameter sollten nur geaendert werden, wenn du weisst, dass sie zu deinem Netz und deiner Region passen.
          </div>
        </div>

        <div class="card">
          <div class="card-top">
            <div>
              <h2>Channels am Gerät</h2>
              <p>Vorhandene Channels lesen und einzelne Slots pflegen.</p>
            </div>
            <div class="pill">PSK</div>
          </div>
          <div id="device-channels" class="log"></div>
          <label>Channel Index</label>
          <input id="channel-idx" type="number" value="0" />
          <label>Name</label>
          <input id="channel-name" placeholder="#meinchannel oder privat" />
          <label>Secret / PSK (32 Hex, optional)</label>
          <input id="channel-secret" placeholder="leer = MeshCore berechnet aus dem Namen" />
          <button onclick="loadDeviceChannels()">Channels lesen</button>
          <button onclick="saveDeviceChannel()">Channel setzen</button>
          <button class="secondary" onclick="removeDeviceChannel()">Channel entfernen</button>
          <div class="guide">
            <strong>Wofuer ist das?</strong>
            Hier verwaltest du die Channels direkt auf dem Geraet. Der `Name` bestimmt den Channel, und das optionale `Secret / PSK` wird fuer private oder gezielt abgestimmte Channel-Konfigurationen genutzt.
          </div>
        </div>

        <div class="card span-all">
          <div class="card-top">
            <div>
              <h2>Telegram Bridges</h2>
              <p>Mehrere Telegram-zu-MeshCore-Bridges fuer verschiedene Chats und Channels.</p>
            </div>
            <div class="pill">Bridge</div>
          </div>
          <div id="telegram-bridges" class="profile-list"></div>
          <div class="grid-bridge-note">
            <div class="command-list">Format ist fest:<br>Telegram -> MeshCore: Nachricht aus Telegram von NAME<br>MeshCore -> Telegram: Nachricht aus Meshcore Channel von NAME</div>
            <button class="secondary" onclick="addTelegramBridge()">Weitere Bridge</button>
          </div>
          <div class="hint">Jede Bridge hat eigenen Bot-Token, Chat und MeshCore-Channel. Einfache Spiegel-Schleifen werden verhindert.</div>
          <button onclick="saveBotConfig()">Bridges mit speichern</button>
          <div class="guide">
            <strong>Wofuer ist das?</strong>
            Telegram-Bridges verbinden einen Telegram-Chat mit einem bestimmten MeshCore-Channel. Du kannst pro Bridge getrennt festlegen, ob nur Telegram nach MeshCore, nur MeshCore nach Telegram oder beides gespiegelt werden soll.
          </div>
        </div>
      </div>
    </section>

    <section id="view-debug" class="view">
      <div class="section-label">Debug</div>
      <div class="grid grid-data">
        <div class="card">
          <div class="card-top">
            <div>
              <h2>Live-Log</h2>
              <p>Bot-, Verbindungs- und MeshCore-Logs in Echtzeit.</p>
            </div>
            <button class="ghost" onclick="downloadLogs()">Logs herunterladen</button>
          </div>
          <div id="log" class="log"></div>
        </div>

        <div class="card">
          <div class="card-top">
            <div>
              <h2>Heltec Aktionen</h2>
              <p>Manuelle Advert- und Diagnoseaktionen direkt am Geraet ausloesen.</p>
            </div>
            <div class="pill">Action</div>
          </div>
          <div class="button-row">
            <button onclick="sendAdvert(false)">Advert senden</button>
            <button class="secondary" onclick="sendAdvert(true)">Flood Advert senden</button>
          </div>
          <div class="guide">
            <strong>Wofuer ist das?</strong>
            `Advert senden` stoesst eine normale Geraete-Ankuendigung an. `Flood Advert senden` verteilt diese aggressiver im Netz und ist eher fuer Diagnose, Reichweitentests oder gezielte Sichtbarkeit gedacht.
          </div>
        </div>
      </div>

      <div class="section-label">UART / Serial</div>
      <div class="grid grid-data-serial">
        <div class="card">
          <div class="card-top">
            <div>
              <h2>Direkt an Heltec senden</h2>
              <p>Text, rohe Hex-Bytes oder ein nackter MeshCore-Payload direkt ueber UART.</p>
            </div>
            <div class="pill">UART TX</div>
          </div>
          <label>Modus</label>
          <select id="serial-send-mode">
            <option value="text">Text</option>
            <option value="hex">Hex-Bytes direkt</option>
            <option value="meshcore_hex">MeshCore Payload (Hex, mit Frame)</option>
          </select>
          <label>Payload</label>
          <textarea id="serial-send-payload" rows="6" placeholder="Textbefehl oder Hexdaten"></textarea>
          <label>Newline anhaengen</label>
          <select id="serial-send-newline">
            <option value="false">nein</option>
            <option value="true">ja</option>
          </select>
          <div class="button-row">
            <button onclick="sendSerialCommand()">An Heltec senden</button>
            <button class="secondary" onclick="loadSerialTrace()">UART-Trace aktualisieren</button>
          </div>
          <div class="hint">`Text` schreibt UTF-8 direkt auf die serielle Leitung. `Hex-Bytes direkt` sendet unveraenderte Bytes. `MeshCore Payload` verpackt den Hex-Payload automatisch in den Companion-Frame `0x3c + len + payload`.</div>
        </div>

        <div class="card">
          <div class="card-top">
            <div>
              <h2>Kompletter Serial-Trace</h2>
              <p>Zeigt RX/TX-Rohdaten zwischen Raspberry Pi und Heltec.</p>
            </div>
            <button class="ghost" onclick="loadSerialTrace()">Neu laden</button>
          </div>
          <div id="serial-trace" class="log"></div>
        </div>
      </div>
    </section>
  </div>

<script>
function switchView(viewName) {
  for (const section of document.querySelectorAll('.view')) {
    section.classList.toggle('active', section.id === `view-${viewName}`);
  }
  for (const tab of document.querySelectorAll('.nav-tab')) {
    tab.classList.toggle('active', tab.id === `tab-${viewName}`);
  }
}

function renderLog(data) {
  const line = `[${data.ts}] ${data.level} ${data.source}: ${data.message}`;
  const el = document.getElementById('log');
  el.textContent = line + "\\n" + el.textContent;
}

function formatAutoResponses(map) {
  return Object.entries(map || {}).map(([cmd, reply]) => `${cmd}=${reply}`).join('\\n');
}

function escapeHtml(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function parseAutoResponses(text) {
  const result = {};
  for (const rawLine of text.split('\\n')) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) continue;
    const idx = line.indexOf('=');
    if (idx === -1) continue;
    const key = line.slice(0, idx).trim().replace(/^!/, '').toLowerCase();
    const value = line.slice(idx + 1).trim();
    if (key && value) result[key] = value;
  }
  return result;
}

function renderBuiltinCommandList(commands) {
  const el = document.getElementById('builtin-command-list');
  el.innerHTML = (commands || []).map(item => `
    <strong>${escapeHtml(item.command)}</strong> -> ${escapeHtml(item.description)}<br>
    Beispielantwort: ${escapeHtml(item.response)}<br>
    Anpassbar ueber: ${escapeHtml(item.config_hint)}
  `).join('<br><br>');
}

function normalizeBotProfiles(bot) {
  const profiles = Array.isArray(bot?.profiles) && bot.profiles.length
    ? bot.profiles
    : [{
        name: bot?.name || 'Standard',
        enabled: bot?.enabled ?? true,
        allowed_channels: Array.isArray(bot?.allowed_channels) ? bot.allowed_channels : [0],
        custom_commands: bot?.custom_commands || bot?.auto_responses || {}
      }];
  return profiles.map((profile, index) => ({
    id: profile.id || `profile-${index + 1}`,
    name: profile.name || `Bot ${index + 1}`,
    enabled: profile.enabled !== false,
    allowed_channels: Array.isArray(profile.allowed_channels) ? profile.allowed_channels : [0],
    custom_commands: profile.custom_commands || {}
  }));
}

function normalizeTelegramBridges(bot) {
  const bridges = Array.isArray(bot?.telegram_bridges) && bot.telegram_bridges.length
    ? bot.telegram_bridges
    : (bot?.telegram_bridge ? [bot.telegram_bridge] : []);
  return bridges.map((bridge, index) => ({
    id: bridge.id || `bridge-${index + 1}`,
    name: bridge.name || `Bridge ${index + 1}`,
    enabled: bridge.enabled === true,
    bot_token: bridge.bot_token || '',
    chat_id: bridge.chat_id || '',
    mesh_channel_idx: Number(bridge.mesh_channel_idx ?? 0),
    mesh_to_telegram: bridge.mesh_to_telegram !== false,
    telegram_to_mesh: bridge.telegram_to_mesh !== false,
    poll_seconds: Number(bridge.poll_seconds ?? 4)
  }));
}

function renderBotProfiles(profiles) {
  const container = document.getElementById('bot-profiles');
  container.innerHTML = profiles.map((profile, index) => `
    <div class="profile-card bot-profile" data-index="${index}">
      <h3>${escapeHtml(profile.name || `Bot ${index + 1}`)}</h3>
      <div class="row">
        <div>
          <label>Name</label>
          <input class="profile-name" value="${escapeHtml(profile.name || '')}" placeholder="z. B. Friends Bot" />
        </div>
        <div>
          <label>Aktiv</label>
          <select class="profile-enabled">
            <option value="true" ${profile.enabled !== false ? 'selected' : ''}>ja</option>
            <option value="false" ${profile.enabled === false ? 'selected' : ''}>nein</option>
          </select>
        </div>
      </div>
      <label>Channels (CSV)</label>
      <input class="profile-channels" value="${escapeHtml((profile.allowed_channels || []).join(','))}" placeholder="4 oder 1,4,5" />
      <label>Eigene Zusatzbefehle</label>
      <textarea class="profile-commands" rows="5" placeholder="info=Friends Channel Bot&#10;lage=Alles ruhig">${escapeHtml(formatAutoResponses(profile.custom_commands || {}))}</textarea>
      <div class="profile-actions">
        <button class="ghost" type="button" onclick="removeBotProfile(${index})">Profil entfernen</button>
      </div>
    </div>`).join('');
}

function renderTelegramBridges(bridges) {
  const container = document.getElementById('telegram-bridges');
  container.innerHTML = bridges.map((bridge, index) => `
    <div class="profile-card telegram-bridge" data-index="${index}">
      <h3>${escapeHtml(bridge.name || `Bridge ${index + 1}`)}</h3>
      <div class="row">
        <div>
          <label>Name</label>
          <input class="bridge-name" value="${escapeHtml(bridge.name || '')}" placeholder="z. B. Friends TG" />
        </div>
        <div>
          <label>Aktiv</label>
          <select class="bridge-enabled">
            <option value="true" ${bridge.enabled ? 'selected' : ''}>ja</option>
            <option value="false" ${!bridge.enabled ? 'selected' : ''}>nein</option>
          </select>
        </div>
      </div>
      <label>Telegram Bot Token</label>
      <input class="bridge-bot-token" type="password" value="${escapeHtml(bridge.bot_token || '')}" placeholder="123456:ABC..." />
      <div class="row">
        <div>
          <label>Telegram Chat ID</label>
          <input class="bridge-chat-id" value="${escapeHtml(bridge.chat_id || '')}" placeholder="-1001234567890" />
        </div>
        <div>
          <label>MeshCore Channel</label>
          <input class="bridge-channel" type="number" value="${escapeHtml(bridge.mesh_channel_idx ?? 0)}" />
        </div>
      </div>
      <div class="row">
        <div>
          <label>Mesh -> Telegram</label>
          <select class="bridge-mesh-to-telegram">
            <option value="true" ${bridge.mesh_to_telegram !== false ? 'selected' : ''}>ja</option>
            <option value="false" ${bridge.mesh_to_telegram === false ? 'selected' : ''}>nein</option>
          </select>
        </div>
        <div>
          <label>Telegram -> Mesh</label>
          <select class="bridge-telegram-to-mesh">
            <option value="true" ${bridge.telegram_to_mesh !== false ? 'selected' : ''}>ja</option>
            <option value="false" ${bridge.telegram_to_mesh === false ? 'selected' : ''}>nein</option>
          </select>
        </div>
      </div>
      <label>Telegram Polling (Sekunden)</label>
      <input class="bridge-poll-seconds" type="number" min="1" step="1" value="${escapeHtml(bridge.poll_seconds ?? 4)}" />
      <div class="profile-actions">
        <button class="ghost" type="button" onclick="removeTelegramBridge(${index})">Bridge entfernen</button>
      </div>
    </div>`).join('');
}

function collectBotProfiles() {
  return Array.from(document.querySelectorAll('.bot-profile')).map((card, index) => ({
    id: `profile-${index + 1}`,
    name: card.querySelector('.profile-name')?.value.trim() || `Bot ${index + 1}`,
    enabled: card.querySelector('.profile-enabled')?.value === 'true',
    allowed_channels: (card.querySelector('.profile-channels')?.value || '')
      .split(',')
      .map(v => Number(v.trim()))
      .filter(v => !Number.isNaN(v)),
    custom_commands: parseAutoResponses(card.querySelector('.profile-commands')?.value || '')
  })).filter(profile => profile.allowed_channels.length > 0);
}

function collectTelegramBridges() {
  return Array.from(document.querySelectorAll('.telegram-bridge')).map((card, index) => ({
    id: `bridge-${index + 1}`,
    name: card.querySelector('.bridge-name')?.value.trim() || `Bridge ${index + 1}`,
    enabled: card.querySelector('.bridge-enabled')?.value === 'true',
    bot_token: card.querySelector('.bridge-bot-token')?.value.trim() || '',
    chat_id: card.querySelector('.bridge-chat-id')?.value.trim() || '',
    mesh_channel_idx: Number(card.querySelector('.bridge-channel')?.value || 0),
    mesh_to_telegram: card.querySelector('.bridge-mesh-to-telegram')?.value === 'true',
    telegram_to_mesh: card.querySelector('.bridge-telegram-to-mesh')?.value === 'true',
    poll_seconds: Number(card.querySelector('.bridge-poll-seconds')?.value || 4)
  })).filter(bridge => bridge.bot_token && bridge.chat_id);
}

function addBotProfile() {
  const current = collectBotProfiles();
  current.push({
    id: `profile-${current.length + 1}`,
    name: `Bot ${current.length + 1}`,
    enabled: true,
    allowed_channels: [0],
    custom_commands: {}
  });
  renderBotProfiles(current);
}

function removeBotProfile(index) {
  const current = collectBotProfiles();
  current.splice(index, 1);
  renderBotProfiles(current.length ? current : [{
    id: 'profile-1',
    name: 'Standard',
    enabled: true,
    allowed_channels: [0],
    custom_commands: {}
  }]);
}

function addTelegramBridge() {
  const current = collectTelegramBridges();
  current.push({
    id: `bridge-${current.length + 1}`,
    name: `Bridge ${current.length + 1}`,
    enabled: true,
    bot_token: '',
    chat_id: '',
    mesh_channel_idx: 0,
    mesh_to_telegram: true,
    telegram_to_mesh: true,
    poll_seconds: 4
  });
  renderTelegramBridges(current);
}

function removeTelegramBridge(index) {
  const current = collectTelegramBridges();
  current.splice(index, 1);
  renderTelegramBridges(current);
}

function renderMetricGrid(targetId, items) {
  const el = document.getElementById(targetId);
  el.innerHTML = items.map(item => `
    <div class="status-item">
      <span class="status-label">${item.label}</span>
      <span class="status-value">${item.value ?? '-'}</span>
    </div>`).join('');
}

async function refreshStatus() {
  const r = await fetch('/api/status');
  const data = await r.json();
  renderMetricGrid('status', [
    {label: 'Verbunden', value: data.connected ? 'Ja' : 'Nein'},
    {label: 'Aktiver Port', value: data.active_port || '-'},
    {label: 'Letztes ACK', value: data.last_ack || '-'},
    {label: 'Queue', value: data.outbound_queue_size},
    {label: 'Channel-Quelle', value: data.channel_source || '-'},
    {label: 'Synchronisierte Channels', value: data.synced_channels},
    {label: 'Uptime', value: `${data.uptime_seconds}s`},
    {label: 'Letzter Fehler', value: data.last_error || '-'}
  ]);
  renderMetricGrid('pi-metrics', [
    {label: 'Hostname', value: data.system_metrics?.hostname || '-'},
    {label: 'IP', value: data.system_metrics?.ip_address || '-'},
    {label: 'System-Uptime', value: data.system_metrics?.uptime || '-'},
    {label: 'CPU Load', value: data.system_metrics?.cpu_load || '-'},
    {label: 'RAM', value: data.system_metrics?.memory || '-'},
    {label: 'Disk', value: data.system_metrics?.disk || '-'},
    {label: 'CPU Temp', value: data.system_metrics?.temperature || '-'},
    {label: 'Architektur', value: data.system_metrics?.arch || '-'}
  ]);
  renderMetricGrid('device-metrics', [
    {label: 'Verbunden', value: data.device_metrics?.connected ? 'Ja' : 'Nein'},
    {label: 'Modell', value: data.device_metrics?.model || '-'},
    {label: 'Firmware', value: data.device_metrics?.firmware || '-'},
    {label: 'Akku', value: data.device_metrics?.battery || '-'},
    {label: 'Funk', value: data.device_metrics?.radio || '-'},
    {label: 'Pakete', value: data.device_metrics?.packets || '-'},
    {label: 'Letztes SNR', value: data.device_metrics?.last_snr || '-'},
    {label: 'Letztes RSSI', value: data.device_metrics?.last_rssi || '-'}
  ]);
}

async function loadConfig() {
  const r = await fetch('/api/config');
  const c = await r.json();
  document.getElementById('bot-enabled').value = String(c.bot.enabled);
  document.getElementById('bot-prefix').value = c.bot.command_prefix;
  document.getElementById('bot-poll-seconds').value = c.bot.poll_seconds ?? 5;
  document.getElementById('dev-name').value = c.meshcore.device.name || '';
  document.getElementById('dev-tx').value = c.meshcore.device.tx_power ?? '';
  document.getElementById('radio-freq').value = c.meshcore.radio.freq ?? '';
  document.getElementById('radio-bw').value = c.meshcore.radio.bw ?? '';
  document.getElementById('radio-sf').value = c.meshcore.radio.sf ?? '';
  document.getElementById('radio-cr').value = c.meshcore.radio.cr ?? '';
  document.getElementById('bot-custom-commands').value = formatAutoResponses(c.bot.custom_commands || c.bot.auto_responses || {});
  renderBotProfiles(normalizeBotProfiles(c.bot));
  renderTelegramBridges(normalizeTelegramBridges(c.bot));
  document.getElementById('serial-baudrate').value = c.meshcore.connection.baudrate ?? 115200;
  document.getElementById('serial-auto-detect').value = String(c.meshcore.connection.auto_detect ?? true);
  document.getElementById('serial-port-manual').value = c.meshcore.connection.port || '';
  await loadBuiltinCommands();
}

async function loadBuiltinCommands() {
  const r = await fetch('/api/commands/builtin');
  const commands = await r.json();
  renderBuiltinCommandList(commands);
}

async function refreshConnections() {
  const r = await fetch('/api/connections');
  const data = await r.json();
  const el = document.getElementById('serial-port');
  const current = data.current?.port || '';
  const ports = Array.isArray(data.serial_ports) ? data.serial_ports : [];
  el.innerHTML = ports.length
    ? ports.map(p => `<option value="${p.port}" ${p.port === current ? 'selected' : ''}>${p.port} ${p.description ? '- ' + p.description : ''}</option>`).join('')
    : `<option value="">Keine Ports erkannt</option>`;
}

async function saveSerialConnection() {
  const manualPort = document.getElementById('serial-port-manual').value.trim();
  const payload = {
    type: 'serial',
    port: manualPort || document.getElementById('serial-port').value || null,
    baudrate: Number(document.getElementById('serial-baudrate').value || 115200),
    auto_detect: document.getElementById('serial-auto-detect').value === 'true'
  };
  const r = await fetch('/api/connections/select', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload)
  });
  renderLog(await r.json());
  await refreshStatus();
}

async function testConnection() {
  const r = await fetch('/api/device/test', {method: 'POST'});
  renderLog(await r.json());
  await refreshStatus();
}

async function restartPi() {
  const r = await fetch('/api/actions/restart/pi', {method: 'POST'});
  renderLog(await r.json());
}

async function restartDevice() {
  const r = await fetch('/api/actions/restart/device', {method: 'POST'});
  renderLog(await r.json());
  await refreshStatus();
}

async function sendMessage() {
  const payload = {
    channel_idx: Number(document.getElementById('send-channel').value),
    text: document.getElementById('send-text').value
  };
  const r = await fetch('/api/send/channel', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload)
  });
  renderLog(await r.json());
  await loadMessages();
  await refreshStatus();
}

async function saveBotConfig() {
  const payload = {
    enabled: document.getElementById('bot-enabled').value === 'true',
    command_prefix: document.getElementById('bot-prefix').value,
    poll_seconds: Number(document.getElementById('bot-poll-seconds').value || 5),
    custom_commands: parseAutoResponses(document.getElementById('bot-custom-commands').value),
    bot_profiles: collectBotProfiles(),
    telegram_bridges: collectTelegramBridges()
  };
  const r = await fetch('/api/config/bot', {
    method: 'PUT',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload)
  });
  renderLog(await r.json());
  await loadBuiltinCommands();
}

async function applyDeviceConfig() {
  const payload = {
    name: document.getElementById('dev-name').value || null,
    tx_power: document.getElementById('dev-tx').value ? Number(document.getElementById('dev-tx').value) : null,
    radio: {
      freq: document.getElementById('radio-freq').value ? Number(document.getElementById('radio-freq').value) : null,
      bw: document.getElementById('radio-bw').value ? Number(document.getElementById('radio-bw').value) : null,
      sf: document.getElementById('radio-sf').value ? Number(document.getElementById('radio-sf').value) : null,
      cr: document.getElementById('radio-cr').value ? Number(document.getElementById('radio-cr').value) : null
    }
  };
  const r = await fetch('/api/config/device/apply', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload)
  });
  renderLog(await r.json());
}

async function sendAdvert(flood = false) {
  const r = await fetch(`/api/device/advert?flood=${flood ? 'true' : 'false'}`, {method: 'POST'});
  renderLog(await r.json());
}

async function loadDeviceChannels() {
  const r = await fetch('/api/device/channels');
  const payload = await r.json();
  const channels = Array.isArray(payload) ? payload : [];
  document.getElementById('device-channels').textContent =
    channels.map(ch => `idx=${ch.channel_idx} ${ch.name} ${ch.secret_hex || '(auto/leer)'}`).join('\\n')
    || (payload.detail || 'Keine Channels gefunden');
}

async function saveDeviceChannel() {
  const payload = {
    channel_idx: Number(document.getElementById('channel-idx').value),
    name: document.getElementById('channel-name').value,
    secret_hex: document.getElementById('channel-secret').value.trim() || null
  };
  const r = await fetch('/api/device/channels', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload)
  });
  renderLog(await r.json());
  await loadDeviceChannels();
}

async function removeDeviceChannel() {
  const idx = Number(document.getElementById('channel-idx').value);
  const r = await fetch(`/api/device/channels/${idx}`, {method: 'DELETE'});
  renderLog(await r.json());
  await loadDeviceChannels();
}

async function loadMessages() {
  const r = await fetch('/api/messages?limit=30');
  const msgs = await r.json();
  document.getElementById('messages').textContent =
    msgs.map(m => `[${m.created_at}] ch=${m.channel_idx} ${m.direction} ${m.sender || '-'}: ${m.text}`).join('\\n');
}

async function loadLogs() {
  const r = await fetch('/api/logs?limit=200');
  const logs = await r.json();
  document.getElementById('log').textContent =
    logs.map(entry => `[${entry.ts}] ${entry.level} ${entry.source}: ${entry.message}`).join('\\n');
}

async function loadSerialTrace() {
  const r = await fetch('/api/serial/logs?limit=300');
  const entries = await r.json();
  document.getElementById('serial-trace').textContent = entries.map(entry =>
    `[${entry.ts}] ${entry.direction} ${entry.kind} len=${entry.length} port=${entry.port || '-'}${entry.note ? ' | ' + entry.note : ''}\\nHEX  ${entry.hex}\\nTEXT ${entry.text}`
  ).join('\\n\\n');
}

async function sendSerialCommand() {
  const payload = {
    mode: document.getElementById('serial-send-mode').value,
    payload: document.getElementById('serial-send-payload').value,
    append_newline: document.getElementById('serial-send-newline').value === 'true'
  };
  const r = await fetch('/api/send/serial', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload)
  });
  const body = await r.json();
  if (!r.ok) {
    renderLog({ts: new Date().toISOString().slice(0, 19).replace('T', ' '), level: 'ERROR', source: 'webui', message: body.detail || 'Serial-Senden fehlgeschlagen'});
    return;
  }
  renderLog(body);
  await loadSerialTrace();
}

function downloadLogs() {
  window.open('/api/logs/download', '_blank');
}

refreshStatus();
loadConfig();
refreshConnections();
loadDeviceChannels();
loadMessages();
loadLogs();
loadSerialTrace();
setInterval(refreshStatus, 3000);
setInterval(loadMessages, 5000);
setInterval(loadLogs, 3000);
setInterval(loadSerialTrace, 3000);
</script>
</body>
</html>
"""


class SendChannelRequest(BaseModel):
    """Payload for manual channel messages sent from the web UI."""

    channel_idx: int = 0
    text: str = Field(min_length=1, max_length=MESHCORE_TEXT_LIMIT)


class SerialSendRequest(BaseModel):
    """Payload for direct UART writes from the web UI."""

    mode: str = Field(default="text", pattern="^(text|hex|meshcore_hex)$")
    payload: str = Field(min_length=1, max_length=4096)
    append_newline: bool = False


class BotConfigPatch(BaseModel):
    """Partial update model for global bot settings from the web UI."""

    enabled: Optional[bool] = None
    command_prefix: Optional[str] = None
    allowed_channels: Optional[list[int]] = None
    poll_seconds: Optional[float] = None
    custom_commands: Optional[dict[str, str]] = None
    bot_profiles: Optional[list[dict[str, Any]]] = None
    telegram_bridge: Optional[dict[str, Any]] = None
    telegram_bridges: Optional[list[dict[str, Any]]] = None


class DeviceApplyRequest(BaseModel):
    """Subset of device/radio settings that can be pushed to the node."""

    name: Optional[str] = None
    tx_power: Optional[int] = None
    radio: dict[str, Optional[float | int]] = Field(default_factory=dict)


class DeviceChannelUpsertRequest(BaseModel):
    """Create or update a MeshCore channel on the connected device."""

    channel_idx: int = Field(ge=0, le=255)
    name: str = Field(min_length=1, max_length=32)
    secret_hex: Optional[str] = Field(default=None, min_length=32, max_length=32)


class ConnectionSelectionRequest(BaseModel):
    """Connection settings for serial/TCP/BLE selection from the browser."""

    type: str = "serial"
    port: Optional[str] = None
    baudrate: Optional[int] = None
    auto_detect: Optional[bool] = None
    host: Optional[str] = None
    address: Optional[str] = None
    pin: Optional[str] = None


class ConfigStore:
    """Simple YAML-backed configuration store with deep-merge defaults."""

    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.save(DEFAULT_CONFIG)

    def load(self) -> dict[str, Any]:
        with self.path.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        return deep_merge(json.loads(json.dumps(DEFAULT_CONFIG)), raw)

    def save(self, data: dict[str, Any]) -> None:
        with self.path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)


class MessageStore:
    """SQLite-backed message history used for the UI and debugging."""

    def __init__(self, path: Path):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init()

    def _init(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                direction TEXT NOT NULL,
                channel_idx INTEGER,
                sender TEXT,
                text TEXT NOT NULL,
                meta_json TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self.conn.commit()

    def add(self, direction: str, channel_idx: Optional[int], sender: Optional[str], text: str, meta: dict[str, Any]) -> None:
        self.conn.execute(
            "INSERT INTO messages(direction, channel_idx, sender, text, meta_json) VALUES (?, ?, ?, ?, ?)",
            (direction, channel_idx, sender, text, json.dumps(meta, ensure_ascii=False)),
        )
        self.conn.commit()

    def list(self, limit: int = 50) -> list[dict[str, Any]]:
        rows = [
            dict(row)
            for row in self.conn.execute(
                "SELECT id, direction, channel_idx, sender, text, meta_json, created_at FROM messages ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        ]
        for row in rows:
            row["meta"] = json.loads(row.pop("meta_json") or "{}")
        return rows

    def cleanup(self, retention_days: int = 14, max_rows: int = 5000, vacuum: bool = True) -> dict[str, int]:
        """Delete old rows and cap table growth to keep disk usage bounded."""
        deleted_old = 0
        deleted_overflow = 0

        if retention_days > 0:
            cursor = self.conn.execute(
                "DELETE FROM messages WHERE created_at < datetime('now', ?)",
                (f"-{int(retention_days)} days",),
            )
            deleted_old = int(cursor.rowcount or 0)

        if max_rows > 0:
            total_row = self.conn.execute("SELECT COUNT(*) AS count FROM messages").fetchone()
            total = int(total_row["count"]) if total_row else 0
            overflow = max(total - int(max_rows), 0)
            if overflow > 0:
                cursor = self.conn.execute(
                    """
                    DELETE FROM messages
                    WHERE id IN (
                        SELECT id FROM messages
                        ORDER BY id ASC
                        LIMIT ?
                    )
                    """,
                    (overflow,),
                )
                deleted_overflow = int(cursor.rowcount or 0)

        self.conn.commit()

        if vacuum and (deleted_old or deleted_overflow):
            self.conn.execute("VACUUM")

        return {
            "deleted_old": deleted_old,
            "deleted_overflow": deleted_overflow,
        }


class LogBuffer:
    """In-memory rolling log buffer for the browser log pane."""

    def __init__(self, max_entries: int = 500):
        self.entries: deque[dict[str, Any]] = deque(maxlen=max_entries)

    def add(self, level: str, source: str, message: str, data: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        entry = {
            "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
            "level": level.upper(),
            "source": source,
            "message": message,
            "data": data or {},
        }
        self.entries.append(entry)
        return entry

    def list(self, limit: int = 200) -> list[dict[str, Any]]:
        return list(self.entries)[-limit:]

    def resize(self, max_entries: int) -> None:
        """Apply a new buffer size without losing the most recent entries."""
        max_entries = max(int(max_entries), 50)
        self.entries = deque(list(self.entries)[-max_entries:], maxlen=max_entries)


class SerialTraceBuffer:
    """Rolling buffer for low-level serial RX/TX visibility in the web UI."""

    def __init__(self, max_entries: int = 1000):
        self.entries: deque[dict[str, Any]] = deque(maxlen=max_entries)

    def add(
        self,
        direction: str,
        kind: str,
        payload: bytes,
        note: str = "",
        port: Optional[str] = None,
    ) -> dict[str, Any]:
        text_preview = payload.decode("utf-8", errors="replace")
        entry = {
            "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
            "direction": direction.upper(),
            "kind": kind,
            "port": port,
            "length": len(payload),
            "hex": payload.hex(),
            "text": text_preview,
            "note": note,
        }
        self.entries.append(entry)
        return entry

    def list(self, limit: int = 200) -> list[dict[str, Any]]:
        return list(self.entries)[-limit:]

    def resize(self, max_entries: int) -> None:
        max_entries = max(int(max_entries), 100)
        self.entries = deque(list(self.entries)[-max_entries:], maxlen=max_entries)


@dataclass
class BotState:
    """Small runtime snapshot used by the status API and dashboard cards."""

    started_at: float = field(default_factory=time.time)
    connected: bool = False
    last_error: Optional[str] = None
    last_ack: Optional[str] = None
    outbound_queue_size: int = 0
    channel_source: str = "config"
    synced_channels: int = 0
    active_port: Optional[str] = None


class MeshcoreBotService:
    """Owns runtime state, background loops and all integration logic.

    One service instance manages exactly one MeshCore connection, but it can
    host multiple logical bot profiles and multiple Telegram bridges on top of
    that shared connection.
    """

    def __init__(
        self,
        config_store: ConfigStore,
        message_store: MessageStore,
        log_buffer: LogBuffer,
        serial_trace_buffer: SerialTraceBuffer,
    ):
        self.config_store = config_store
        self.message_store = message_store
        self.log_buffer = log_buffer
        self.serial_trace_buffer = serial_trace_buffer
        self.state = BotState()
        self.mesh: Optional[MeshCore] = None
        self.stop_event = asyncio.Event()
        self.worker_tasks: list[asyncio.Task] = []
        self.dedupe_seen: dict[str, float] = {}
        self.recent_outbound = deque(maxlen=100)
        self.self_pubkey_prefix: Optional[str] = None
        self.device_info: dict[str, Any] = {}
        self.device_battery: dict[str, Any] = {}
        self.device_stats_radio: dict[str, Any] = {}
        self.device_stats_packets: dict[str, Any] = {}
        self.device_stats_core: dict[str, Any] = {}
        self.device_telemetry: dict[str, Any] = {}
        self.last_device_metrics_refresh: float = 0.0
        self.telegram_offsets: dict[str, int] = {}
        self.recent_telegram_outbound = deque(maxlen=100)
        self.subscriptions_ready = False
        self.connection_lock = asyncio.Lock()
        self.send_lock = asyncio.Lock()
        self.fetch_lock = asyncio.Lock()
        self.metrics_lock = asyncio.Lock()
        self.telegram_lock = asyncio.Lock()
        self._serial_hooks_ready = False

    def log(self, level: str, message: str, data: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        if data:
            try:
                suffix = json.dumps(data, ensure_ascii=False, default=str)
            except Exception:
                suffix = str(data)
            message = f"{message} | {suffix}"
        return self.log_buffer.add(level, "meshcore-bot", message, data)

    def trace_serial(self, direction: str, kind: str, payload: bytes, note: str = "") -> dict[str, Any]:
        return self.serial_trace_buffer.add(
            direction=direction,
            kind=kind,
            payload=payload,
            note=note,
            port=self.state.active_port,
        )

    def _install_serial_trace_hooks(self) -> None:
        if self._serial_hooks_ready or not self.mesh:
            return
        connection = getattr(getattr(self.mesh, "connection_manager", None), "connection", None)
        if connection is None:
            return

        original_handle_rx = getattr(connection, "handle_rx", None)
        original_send = getattr(connection, "send", None)

        if callable(original_handle_rx):
            def traced_handle_rx(data: bytearray) -> Any:
                raw = bytes(data)
                self.trace_serial("rx", "uart_chunk", raw, "Raw UART RX chunk")
                return original_handle_rx(data)

            connection.handle_rx = traced_handle_rx

        if callable(original_send):
            async def traced_send(data: bytes) -> Any:
                pkt = b"\x3c" + len(data).to_bytes(2, byteorder="little") + data
                self.trace_serial("tx", "meshcore_frame", pkt, "MeshCore framed TX")
                return await original_send(data)

            connection.send = traced_send

        self._serial_hooks_ready = True
        self.log("INFO", "Serial-Trace Hooks aktiviert", {"active_port": self.state.active_port})

    def _format_duration(self, seconds: float) -> str:
        total = max(int(seconds), 0)
        hours, remainder = divmod(total, 3600)
        minutes, secs = divmod(remainder, 60)
        if hours:
            return f"{hours}h {minutes}m {secs}s"
        if minutes:
            return f"{minutes}m {secs}s"
        return f"{secs}s"

    def _prune_dedupe_cache(self, now: Optional[float] = None, window_seconds: float = 180.0) -> None:
        now = time.time() if now is None else now
        expiry = max(float(window_seconds), 0.5)
        stale_keys = [key for key, seen_at in self.dedupe_seen.items() if (now - seen_at) > expiry]
        for key in stale_keys:
            self.dedupe_seen.pop(key, None)

    def _fit_meshcore_text(self, text: str, limit: int = MESHCORE_TEXT_LIMIT) -> str:
        clean = str(text or "").strip()
        if len(clean) <= limit:
            return clean
        if limit <= 3:
            return clean[:limit]
        return clean[: limit - 3].rstrip() + "..."

    def _short_value(self, value: Any, limit: int) -> str:
        return self._fit_meshcore_text(str(value or "-"), limit=limit)

    def _read_system_uptime(self) -> str:
        try:
            with Path("/proc/uptime").open("r", encoding="utf-8") as f:
                uptime_seconds = float(f.read().split()[0])
            return self._format_duration(uptime_seconds)
        except Exception:
            return self._format_duration(time.time() - self.state.started_at)

    def _read_memory_usage(self) -> str:
        try:
            values: dict[str, int] = {}
            with Path("/proc/meminfo").open("r", encoding="utf-8") as f:
                for line in f:
                    if ":" not in line:
                        continue
                    key, raw = line.split(":", 1)
                    values[key.strip()] = int(raw.strip().split()[0])
            total = values.get("MemTotal")
            available = values.get("MemAvailable")
            if total and available is not None:
                used = total - available
                pct = (used / total) * 100
                return f"{used // 1024}/{total // 1024} MB ({pct:.0f}%)"
        except Exception:
            pass
        return "-"

    def _read_disk_usage(self) -> str:
        try:
            usage = shutil.disk_usage(BASE_DIR)
            pct = usage.used / usage.total * 100
            return f"{usage.used // (1024**3)}/{usage.total // (1024**3)} GB ({pct:.0f}%)"
        except Exception:
            return "-"

    def _read_cpu_temp(self) -> str:
        try:
            raw = Path("/sys/class/thermal/thermal_zone0/temp").read_text(encoding="utf-8").strip()
            temp_c = int(raw) / 1000
            return f"{temp_c:.1f} C"
        except Exception:
            return "-"

    def _read_ip_address(self) -> str:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                sock.connect(("8.8.8.8", 80))
                return sock.getsockname()[0]
            finally:
                sock.close()
        except Exception:
            try:
                return socket.gethostbyname(socket.gethostname())
            except Exception:
                return "-"

    def get_system_metrics(self) -> dict[str, Any]:
        try:
            load1, _, _ = os.getloadavg()
            cpu_load = f"{load1:.2f}"
        except Exception:
            cpu_load = "-"
        return {
            "hostname": platform.node() or "-",
            "ip_address": self._read_ip_address(),
            "uptime": self._read_system_uptime(),
            "cpu_load": cpu_load,
            "memory": self._read_memory_usage(),
            "disk": self._read_disk_usage(),
            "temperature": self._read_cpu_temp(),
            "arch": platform.machine() or "-",
        }

    def get_device_metrics(self) -> dict[str, Any]:
        info = self.device_info or {}
        battery = self.device_battery or {}
        radio = self.device_stats_radio or {}
        packets = self.device_stats_packets or {}
        fw_main = info.get("ver") or "-"
        fw_build = info.get("fw_build")
        firmware = f"{fw_main} ({fw_build})" if fw_build else fw_main
        battery_level = battery.get("level")
        battery_text = f"{battery_level} mV" if battery_level not in (None, "") else "-"
        if battery.get("used_kb") and battery.get("total_kb"):
            battery_text += f" | {battery['used_kb']}/{battery['total_kb']} KB"
        radio_text = "-"
        if radio:
            radio_text = f"TX {radio.get('tx_air_secs', '-') }s | RX {radio.get('rx_air_secs', '-') }s"
        packet_text = "-"
        if packets:
            packet_text = f"RX {packets.get('recv', '-')} | TX {packets.get('sent', '-')}"
        last_snr = radio.get("last_snr")
        last_rssi = radio.get("last_rssi")
        return {
            "connected": self.state.connected,
            "model": info.get("model") or "-",
            "firmware": firmware,
            "battery": battery_text,
            "radio": radio_text,
            "packets": packet_text,
            "last_snr": f"{last_snr}" if last_snr not in (None, "") else "-",
            "last_rssi": f"{last_rssi}" if last_rssi not in (None, "") else "-",
        }

    def _normalize_telegram_bridge(self, cfg: dict[str, Any], index: int = 0) -> dict[str, Any]:
        return {
            "id": str(cfg.get("id") or f"bridge-{index + 1}"),
            "name": str(cfg.get("name") or f"Bridge {index + 1}").strip(),
            "enabled": bool(cfg.get("enabled", False)),
            "bot_token": str(cfg.get("bot_token", "") or "").strip(),
            "chat_id": str(cfg.get("chat_id", "") or "").strip(),
            "mesh_channel_idx": int(cfg.get("mesh_channel_idx", 0) or 0),
            "mesh_to_telegram": bool(cfg.get("mesh_to_telegram", True)),
            "telegram_to_mesh": bool(cfg.get("telegram_to_mesh", True)),
            "poll_seconds": max(float(cfg.get("poll_seconds", 4) or 4), 1.0),
        }

    def _get_telegram_bridges(self) -> list[dict[str, Any]]:
        bot_cfg = self.config_store.load()["bot"]
        raw = bot_cfg.get("telegram_bridges")
        bridges: list[dict[str, Any]] = []
        if isinstance(raw, list):
            for index, item in enumerate(raw):
                if isinstance(item, dict):
                    bridges.append(self._normalize_telegram_bridge(item, index))
        if bridges:
            return bridges
        legacy = bot_cfg.get("telegram_bridge")
        if isinstance(legacy, dict) and legacy:
            return [self._normalize_telegram_bridge(legacy, 0)]
        return []

    async def _telegram_api(self, bridge: dict[str, Any], method: str, payload: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        token = bridge.get("bot_token")
        if not token:
            raise RuntimeError("Telegram Bot Token fehlt")
        url = f"https://api.telegram.org/bot{token}/{method}"
        async with httpx.AsyncClient(timeout=40) as client:
            response = await client.post(url, json=payload or {})
            response.raise_for_status()
            data = response.json()
        if not data.get("ok", False):
            raise RuntimeError(f"Telegram API Fehler bei {method}: {data}")
        return data

    async def _send_to_telegram(self, bridge: dict[str, Any], text: str) -> None:
        chat_id = bridge.get("chat_id")
        if not chat_id:
            raise RuntimeError("Telegram Chat ID fehlt")
        await self._telegram_api(
            bridge,
            "sendMessage",
            {
                "chat_id": chat_id,
                "text": text,
                "disable_web_page_preview": True,
            },
        )
        self.recent_telegram_outbound.append(text)

    async def telegram_bridge_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                bridges = self._get_telegram_bridges()
                if not bridges:
                    await asyncio.sleep(3)
                    continue
                active = [bridge for bridge in bridges if bridge["enabled"] and bridge["bot_token"] and bridge["chat_id"]]
                if not active:
                    await asyncio.sleep(3)
                    continue
                for bridge in active:
                    await self.poll_telegram_updates(bridge)
                await asyncio.sleep(min(bridge["poll_seconds"] for bridge in active))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log("ERROR", "Telegram-Bridge fehlgeschlagen", {"error": str(exc)})
                await asyncio.sleep(5)

    async def refresh_device_metrics(self, force: bool = False) -> None:
        if not (self.mesh and self.mesh.is_connected):
            return
        if not force and (time.time() - self.last_device_metrics_refresh) < 15:
            return

        async with self.metrics_lock:
            if not (self.mesh and self.mesh.is_connected):
                return
            if not force and (time.time() - self.last_device_metrics_refresh) < 15:
                return

            try:
                device_query = await self.mesh.commands.send_device_query()
                if getattr(device_query, "type", None) != EventType.ERROR and isinstance(getattr(device_query, "payload", None), dict):
                    self.device_info = device_query.payload
            except Exception:
                pass

            try:
                bat = await self.mesh.commands.get_bat()
                if getattr(bat, "type", None) != EventType.ERROR and isinstance(getattr(bat, "payload", None), dict):
                    self.device_battery = bat.payload
            except Exception:
                pass

            try:
                stats_radio = await self.mesh.commands.get_stats_radio()
                if getattr(stats_radio, "type", None) != EventType.ERROR and isinstance(getattr(stats_radio, "payload", None), dict):
                    self.device_stats_radio = stats_radio.payload
            except Exception:
                pass

            try:
                stats_packets = await self.mesh.commands.get_stats_packets()
                if getattr(stats_packets, "type", None) != EventType.ERROR and isinstance(getattr(stats_packets, "payload", None), dict):
                    self.device_stats_packets = stats_packets.payload
            except Exception:
                pass

            try:
                stats_core = await self.mesh.commands.get_stats_core()
                if getattr(stats_core, "type", None) != EventType.ERROR and isinstance(getattr(stats_core, "payload", None), dict):
                    self.device_stats_core = stats_core.payload
            except Exception:
                pass

            try:
                telemetry = await self.mesh.commands.get_self_telemetry()
                if getattr(telemetry, "type", None) != EventType.ERROR and isinstance(getattr(telemetry, "payload", None), dict):
                    self.device_telemetry = telemetry.payload
            except Exception:
                pass

            self.last_device_metrics_refresh = time.time()

    def _build_help_text(self, bot_cfg: dict[str, Any]) -> str:
        prefix = str(bot_cfg.get("command_prefix", "!") or "!")
        return self._fit_meshcore_text(
            f"🤖 {prefix}help {prefix}ping {prefix}wetter {prefix}forecast {prefix}status {prefix}info"
        )

    def _normalize_custom_commands(self, value: Any) -> dict[str, str]:
        if not isinstance(value, dict):
            return {}
        return {
            str(key).strip().replace("!", "").lower(): str(reply).strip()
            for key, reply in value.items()
            if str(key).strip() and str(reply).strip()
        }

    def _get_bot_profiles(self, bot_cfg: dict[str, Any]) -> list[dict[str, Any]]:
        raw_profiles = bot_cfg.get("profiles")
        profiles: list[dict[str, Any]] = []
        if isinstance(raw_profiles, list):
            for index, raw in enumerate(raw_profiles):
                if not isinstance(raw, dict):
                    continue
                channels = raw.get("allowed_channels", [])
                if not isinstance(channels, list):
                    channels = []
                normalized_channels = []
                for channel in channels:
                    try:
                        normalized_channels.append(int(channel))
                    except Exception:
                        continue
                if not normalized_channels:
                    continue
                profiles.append(
                    {
                        "id": str(raw.get("id") or f"profile-{index + 1}"),
                        "name": str(raw.get("name") or f"Bot {index + 1}"),
                        "enabled": bool(raw.get("enabled", True)),
                        "allowed_channels": normalized_channels,
                        "custom_commands": self._normalize_custom_commands(raw.get("custom_commands")),
                    }
                )
        if profiles:
            return profiles
        return [
            {
                "id": "legacy-default",
                "name": str(bot_cfg.get("name") or "Standard"),
                "enabled": bool(bot_cfg.get("enabled", True)),
                "allowed_channels": [int(ch) for ch in bot_cfg.get("allowed_channels", [0])],
                "custom_commands": self._normalize_custom_commands(bot_cfg.get("custom_commands") or bot_cfg.get("auto_responses")),
            }
        ]

    def _find_profile_for_channel(self, bot_cfg: dict[str, Any], channel_idx: Any) -> Optional[dict[str, Any]]:
        try:
            channel_value = int(channel_idx)
        except Exception:
            return None
        for profile in self._get_bot_profiles(bot_cfg):
            if not profile.get("enabled", True):
                continue
            if channel_value in profile.get("allowed_channels", []):
                return profile
        return None

    async def start(self) -> None:
        """Start all long-running background tasks."""
        self.log("INFO", "Bot-Start initialisiert", {"config_path": str(CONFIG_PATH)})
        self.worker_tasks = [
            asyncio.create_task(self.connection_loop(), name="meshcore-connection-loop"),
            asyncio.create_task(self.message_fetch_loop(), name="meshcore-message-fetch-loop"),
            asyncio.create_task(self.telegram_bridge_loop(), name="meshcore-telegram-bridge-loop"),
            asyncio.create_task(self.maintenance_loop(), name="meshcore-maintenance-loop"),
        ]

    async def stop(self) -> None:
        """Stop background tasks and disconnect cleanly from the MeshCore node."""
        self.stop_event.set()
        self.log("INFO", "Bot wird gestoppt")
        for task in self.worker_tasks:
            task.cancel()
        if self.mesh:
            await self.mesh.disconnect()

    def list_available_serial_ports(self) -> list[dict[str, Any]]:
        ports: list[dict[str, Any]] = []
        seen: set[str] = set()
        if list_ports is not None:
            for port in list_ports.comports():
                device = getattr(port, "device", None)
                if not device or device in seen:
                    continue
                seen.add(device)
                ports.append(
                    {
                        "type": "serial",
                        "port": device,
                        "description": getattr(port, "description", "") or "",
                        "hwid": getattr(port, "hwid", "") or "",
                        "manufacturer": getattr(port, "manufacturer", None),
                        "product": getattr(port, "product", None),
                        "vid": getattr(port, "vid", None),
                        "pid": getattr(port, "pid", None),
                    }
                )
        for pattern in (
            "/dev/ttyACM*",
            "/dev/ttyUSB*",
            "/dev/ttyAMA*",
            "/dev/ttyS*",
            "/dev/serial0",
            "/dev/serial1",
            "/dev/rfcomm*",
            "/dev/serial/by-id/*",
        ):
            for device in sorted(glob.glob(pattern)):
                if device in seen:
                    continue
                seen.add(device)
                ports.append(
                    {
                        "type": "serial",
                        "port": device,
                        "description": "auto-detected",
                        "hwid": "",
                        "manufacturer": None,
                        "product": None,
                        "vid": None,
                        "pid": None,
                    }
                )
        ports = sorted(ports, key=lambda item: item["port"])
        self.log("INFO", "Serielle Ports gescannt", {"count": len(ports), "ports": ports})
        return ports

    def resolve_serial_port(self, conn: dict[str, Any]) -> Optional[str]:
        configured = conn.get("port")
        auto_detect = bool(conn.get("auto_detect", True))
        if configured and not auto_detect:
            return configured
        ports = self.list_available_serial_ports()
        available = {item["port"] for item in ports}
        if configured and configured in available:
            return configured
        if ports:
            return ports[0]["port"]
        return configured

    async def ensure_connected(self, force_reconnect: bool = False) -> None:
        """Create or reuse the active MeshCore connection.

        The lock prevents overlapping reconnect attempts from status checks,
        message sending and background loops.
        """
        async with self.connection_lock:
            if force_reconnect and self.mesh:
                await self.disconnect_current("Reconnect angefordert")
            if self.mesh and self.mesh.is_connected:
                return

            cfg = self.config_store.load()
            conn = cfg["meshcore"]["connection"]
            ctype = conn["type"]
            debug = bool(conn.get("debug", False))
            auto_reconnect = bool(conn.get("auto_reconnect", True))
            max_reconnect_attempts = int(conn.get("max_reconnect_attempts", 0)) or None

            self.log("INFO", "Verbindung wird aufgebaut", {"type": ctype, "connection": conn})

            mesh: Optional[MeshCore] = None
            active_port: Optional[str] = None

            if ctype == "serial":
                port = self.resolve_serial_port(conn)
                if not port:
                    raise RuntimeError("Kein serielles MeshCore-Geraet gefunden. Bitte Port waehlen oder Heltec verbinden.")
                mesh = await MeshCore.create_serial(
                    port,
                    int(conn.get("baudrate", 115200)),
                    debug=debug,
                    auto_reconnect=auto_reconnect,
                    max_reconnect_attempts=max_reconnect_attempts,
                )
                active_port = port
            elif ctype == "tcp":
                host = conn.get("host")
                if not host:
                    raise RuntimeError("TCP-Host fehlt in der Konfiguration")
                mesh = await MeshCore.create_tcp(
                    host,
                    int(conn.get("port", 4000)),
                    debug=debug,
                    auto_reconnect=auto_reconnect,
                    max_reconnect_attempts=max_reconnect_attempts,
                )
                active_port = f"{host}:{int(conn.get('port', 4000))}"
            elif ctype == "ble":
                mesh = await MeshCore.create_ble(
                    conn.get("address"),
                    pin=conn.get("pin"),
                    debug=debug,
                    auto_reconnect=auto_reconnect,
                    max_reconnect_attempts=max_reconnect_attempts,
                )
                active_port = conn.get("address")
            else:
                raise RuntimeError(f"Unsupported connection type: {ctype}")

            if mesh is None:
                raise RuntimeError("MeshCore-Verbindung konnte nicht initialisiert werden")

            self.mesh = mesh
            self.state.active_port = active_port
            self.subscriptions_ready = False
            self._serial_hooks_ready = False
            self._install_serial_trace_hooks()
            await self.after_connect()

    async def after_connect(self) -> None:
        """Run post-connect initialization for subscriptions and cached metadata."""
        assert self.mesh is not None
        if not self.subscriptions_ready:
            self.mesh.subscribe(EventType.CHANNEL_MSG_RECV, self.on_channel_message)
            self.mesh.subscribe(EventType.CONTACT_MSG_RECV, self.on_direct_message)
            self.mesh.subscribe(EventType.ACK, self.on_ack)
            self.mesh.subscribe(EventType.CONNECTED, self.on_connected)
            self.mesh.subscribe(EventType.DISCONNECTED, self.on_disconnected)
            self.subscriptions_ready = True

        await self.mesh.start_auto_message_fetching()

        appstart = await self.mesh.commands.send_appstart()
        if getattr(appstart, "payload", None) and isinstance(appstart.payload, dict):
            self.self_pubkey_prefix = str(appstart.payload.get("public_key", ""))[:12]

        device_query = await self.mesh.commands.send_device_query()
        if getattr(device_query, "payload", None) and isinstance(device_query.payload, dict):
            self.device_info = device_query.payload
            self.log("INFO", "Geraeteinformationen gelesen", self.device_info)

        self.state.connected = True
        self.state.last_error = None
        self.log("INFO", "MeshCore-Geraet verbunden", {"active_port": self.state.active_port})

        if self.config_store.load()["meshcore"]["device"].get("set_time_on_connect"):
            await self.mesh.commands.set_time(int(time.time()))
            self.log("INFO", "Zeit auf Geraet gesetzt")

        if self.config_store.load()["meshcore"]["channel_sync"].get("source", "device") == "device":
            await self.sync_channels_from_device()
        await self.refresh_device_metrics(force=True)

    async def disconnect_current(self, reason: str) -> None:
        if self.mesh:
            try:
                await self.mesh.disconnect()
            except Exception:
                pass
        self.mesh = None
        self._serial_hooks_ready = False
        self.state.connected = False
        self.log("WARNING", "Verbindung getrennt", {"reason": reason})

    async def connection_loop(self) -> None:
        """Keep the MeshCore connection alive and retry when it drops."""
        while not self.stop_event.is_set():
            try:
                await self.ensure_connected()
                while self.mesh and self.mesh.is_connected and not self.stop_event.is_set():
                    await asyncio.sleep(2)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.state.connected = False
                self.state.last_error = f"connection_loop: {exc}"
                self.log("ERROR", "Verbindungsaufbau fehlgeschlagen", {"error": str(exc)})
                await asyncio.sleep(5)

    async def message_fetch_loop(self) -> None:
        """Periodically pull pending messages as a safety net.

        MeshCore push events are still used, but this loop reduces the chance of
        missed messages when notifications arrive late or are lost.
        """
        while not self.stop_event.is_set():
            cfg = self.config_store.load()
            interval = max(float(cfg["bot"].get("poll_seconds", 5) or 5), 0.5)
            try:
                await asyncio.sleep(interval)
                if self.stop_event.is_set():
                    break
                if not (self.mesh and self.mesh.is_connected):
                    continue
                if self.connection_lock.locked() or self.send_lock.locked():
                    await asyncio.sleep(0.2)
                    continue
                await self.fetch_pending_messages("interval")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log("ERROR", "Nachrichtenabruf-Loop fehlgeschlagen", {"error": str(exc)})
                await asyncio.sleep(1)

    async def maintenance_loop(self) -> None:
        """Regular housekeeping to bound RAM and SQLite growth."""
        while not self.stop_event.is_set():
            cfg = self.config_store.load()
            maintenance_cfg = cfg["bot"].get("maintenance", {})
            if not maintenance_cfg.get("enabled", True):
                await asyncio.sleep(60)
                continue

            interval_minutes = max(float(maintenance_cfg.get("cleanup_interval_minutes", 30) or 30), 1.0)
            try:
                self.log_buffer.resize(int(maintenance_cfg.get("max_log_entries", 500) or 500))
                self.serial_trace_buffer.resize(max(int(maintenance_cfg.get("max_log_entries", 500) or 500) * 2, 200))
                stats = self.message_store.cleanup(
                    retention_days=int(maintenance_cfg.get("message_retention_days", 14) or 14),
                    max_rows=int(maintenance_cfg.get("max_message_rows", 5000) or 5000),
                    vacuum=bool(maintenance_cfg.get("vacuum_after_cleanup", True)),
                )
                if stats["deleted_old"] or stats["deleted_overflow"]:
                    self.log(
                        "INFO",
                        "Log-/Historien-Bereinigung ausgefuehrt",
                        {
                            "deleted_old": stats["deleted_old"],
                            "deleted_overflow": stats["deleted_overflow"],
                            "max_log_entries": int(maintenance_cfg.get("max_log_entries", 500) or 500),
                        },
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log("ERROR", "Wartungs-Loop fehlgeschlagen", {"error": str(exc)})

            await asyncio.sleep(interval_minutes * 60)

    async def poll_telegram_updates(self, bridge: dict[str, Any]) -> None:
        """Fetch updates for one Telegram bridge using an offset per bridge."""
        async with self.telegram_lock:
            offset_key = f"telegram_offset_{bridge['id']}"
            offset = int(self.telegram_offsets.get(offset_key, 0) or 0)
            data = await self._telegram_api(
                bridge,
                "getUpdates",
                {
                    "offset": offset,
                    "timeout": int(bridge["poll_seconds"]),
                    "allowed_updates": ["message", "channel_post"],
                },
            )
            for update in data.get("result", []):
                offset = max(offset, int(update.get("update_id", 0)) + 1)
                self.telegram_offsets[offset_key] = offset
                await self.handle_telegram_update(update, bridge)

    async def handle_telegram_update(self, update: dict[str, Any], bridge: dict[str, Any]) -> None:
        """Translate one Telegram update into a MeshCore channel message."""
        message = update.get("channel_post") or update.get("message")
        if not isinstance(message, dict):
            return
        chat = message.get("chat", {}) or {}
        if str(chat.get("id", "")) != str(bridge["chat_id"]):
            return
        text = (message.get("text") or message.get("caption") or "").strip()
        if not text:
            return
        if text in self.recent_telegram_outbound:
            return
        sender = message.get("from", {}) or {}
        if sender.get("is_bot"):
            return
        if not bridge.get("telegram_to_mesh", True):
            return

        sender_name = self._extract_telegram_sender_name(message)
        mesh_text = f"Nachricht aus Telegram von {sender_name}:\n{text}"
        await self.send_channel_message(int(bridge["mesh_channel_idx"]), mesh_text, meta={"bridge": "telegram_to_mesh"})
        self.log("INFO", "Telegram -> MeshCore weitergeleitet", {"chat_id": bridge["chat_id"], "channel_idx": bridge["mesh_channel_idx"], "text": text})

    async def bridge_mesh_to_telegram(self, channel_idx: Any, sender: Any, text: str) -> None:
        """Forward one MeshCore message to all matching Telegram bridges."""
        bot_cfg = self.config_store.load()["bot"]
        prefix = str(bot_cfg.get("command_prefix", "!") or "!")
        if self._is_bot_command_message(text, prefix):
            self.log("INFO", "Telegram-Bridge ueberspringt Bot-Befehl", {"channel_idx": channel_idx, "text": text})
            return
        embedded_sender, embedded_body = self._split_sender_prefix(text)
        sender_label = str(sender or "").strip()
        if not sender_label or sender_label in {"unbekannt", "?", "MeshCore"}:
            sender_label = embedded_sender or "MeshCore"
        body = embedded_body if embedded_sender and text.strip().startswith(f"{embedded_sender}:") else text
        telegram_text = f"Nachricht aus Meshcore Channel von {sender_label}:\n{body}"
        for bridge in self._get_telegram_bridges():
            if not bridge["enabled"] or not bridge["mesh_to_telegram"]:
                continue
            if int(channel_idx or 0) != int(bridge["mesh_channel_idx"]):
                continue
            await self._send_to_telegram(bridge, telegram_text)
            self.log("INFO", "MeshCore -> Telegram weitergeleitet", {"bridge": bridge["name"], "channel_idx": channel_idx, "chat_id": bridge["chat_id"], "text": text})

    def _normalize_secret_hex(self, payload: dict[str, Any]) -> Optional[str]:
        for key in ("secret_hex", "secret", "key", "psk", "channel_secret"):
            value = payload.get(key)
            if value in (None, ""):
                continue
            if isinstance(value, bytes):
                return value.hex()
            if isinstance(value, bytearray):
                return bytes(value).hex()
            if isinstance(value, list) and all(isinstance(item, int) for item in value):
                return bytes(value).hex()
            if isinstance(value, str):
                compact = value.strip().replace(" ", "")
                if compact and all(ch in "0123456789abcdefABCDEF" for ch in compact):
                    return compact.lower()
        return None

    def _extract_channel_from_payload(self, default_idx: int, payload: Any) -> Optional[dict[str, Any]]:
        if not isinstance(payload, dict):
            return None
        name = payload.get("name") or payload.get("channel_name")
        secret_hex = self._normalize_secret_hex(payload)
        channel_idx = payload.get("channel_idx", payload.get("idx", payload.get("channel", default_idx)))
        if name in (None, "") and secret_hex is None:
            return None
        return {
            "channel_idx": int(channel_idx),
            "name": str(name or f"channel-{channel_idx}"),
            "secret_hex": secret_hex,
        }

    async def get_device_channels(self, write_back_config: Optional[bool] = None) -> list[dict[str, Any]]:
        await self.ensure_connected()
        assert self.mesh is not None
        if not hasattr(self.mesh.commands, "get_channel"):
            raise RuntimeError("meshcore.commands.get_channel() ist in der installierten Version nicht verfuegbar")

        cfg = self.config_store.load()
        sync_cfg = cfg["meshcore"].get("channel_sync", {})
        max_channels = int(sync_cfg.get("max_channels", 8))
        if write_back_config is None:
            write_back_config = bool(sync_cfg.get("write_back_config", True))

        channels: list[dict[str, Any]] = []
        for idx in range(max_channels):
            result = await self.mesh.commands.get_channel(idx)
            if getattr(result, "type", None) == EventType.ERROR:
                continue
            channel = self._extract_channel_from_payload(idx, getattr(result, "payload", None))
            if channel is not None:
                channels.append(channel)

        if write_back_config and channels:
            cfg["meshcore"]["channels"] = channels
            self.config_store.save(cfg)
        self.state.channel_source = "device"
        self.state.synced_channels = len(channels)
        self.log("INFO", "Channels vom Geraet gelesen", {"count": len(channels), "channels": channels})
        return channels

    async def sync_channels_from_device(self) -> None:
        channels = await self.get_device_channels(write_back_config=True)
        if not channels:
            self.log("WARNING", "Keine Channels vom Geraet gefunden")

    async def send_channel_message(self, channel_idx: int, text: str, meta: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        """Serialize outbound MeshCore sends through a single lock.

        This avoids overlapping serial writes and keeps the web UI, bot replies
        and Telegram bridges from stepping on each other.
        """
        fitted_text = self._fit_meshcore_text(text)
        if fitted_text != text:
            self.log("WARNING", "Nachricht auf MeshCore-Limit gekuerzt", {"channel_idx": channel_idx, "original_length": len(text), "new_length": len(fitted_text)})
        text = fitted_text
        self.state.outbound_queue_size = 1
        self.log("INFO", "Nachricht wird gesendet", {"channel_idx": channel_idx, "text": text})
        try:
            async with self.send_lock:
                self.log("INFO", "Sende-Lock erhalten", {"channel_idx": channel_idx})
                if not (self.mesh and self.mesh.is_connected):
                    await asyncio.wait_for(self.ensure_connected(), timeout=8.0)
                self.log("INFO", "Verbindung vor Versand bestaetigt", {"channel_idx": channel_idx, "active_port": self.state.active_port})
                return await asyncio.wait_for(self._send_channel_now(channel_idx, text, meta), timeout=8.0)
        except Exception as exc:
            self.state.outbound_queue_size = 0
            self.state.last_error = f"send_channel_message: {exc}"
            self.log("ERROR", "Senden fehlgeschlagen", {"channel_idx": channel_idx, "text": text, "error": repr(exc), "error_type": type(exc).__name__})
            raise

    async def _send_channel_now(self, channel_idx: int, text: str, meta: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        assert self.mesh is not None
        if not hasattr(self.mesh.commands, "send_chan_msg"):
            raise RuntimeError("meshcore.commands.send_chan_msg() ist nicht verfuegbar")

        result = await self.mesh.commands.send_chan_msg(channel_idx, text)
        result_type = getattr(result, "type", None)
        payload = {
            "mode": "meshcore_send_chan_msg",
            "channel_idx": channel_idx,
            "event_type": getattr(result_type, "name", str(result_type)),
            "result": getattr(result, "payload", {}),
        }
        if result_type == EventType.ERROR:
            raise RuntimeError(str(getattr(result, "payload", {})))

        self.recent_outbound.append(self._outbound_echo_key(text, channel_idx, self.self_pubkey_prefix))
        self.message_store.add("out", channel_idx, self.self_pubkey_prefix, text, {"send_result": payload, **(meta or {})})
        self.state.outbound_queue_size = 0
        self.log("INFO", "Nachricht ueber MeshCore-API gesendet", {"channel_idx": channel_idx, "payload": payload})
        return {"ok": True, "ts": time.strftime("%Y-%m-%d %H:%M:%S"), "level": "INFO", "source": "meshcore-bot", "message": "Nachricht ueber MeshCore-API gesendet", "data": {"channel_idx": channel_idx, "payload": payload}}

    async def _send_direct_reply(self, destination: Any, text: str, meta: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        assert self.mesh is not None
        if not destination:
            raise RuntimeError("Kein gueltiger Direktnachrichten-Empfaenger vorhanden")
        if not hasattr(self.mesh.commands, "send_msg_with_retry"):
            raise RuntimeError("meshcore.commands.send_msg_with_retry() ist nicht verfuegbar")

        fitted_text = self._fit_meshcore_text(text)
        if fitted_text != text:
            self.log("WARNING", "Direktnachricht auf MeshCore-Limit gekuerzt", {"destination": str(destination), "original_length": len(text), "new_length": len(fitted_text)})
        text = fitted_text

        result = await self.mesh.commands.send_msg_with_retry(destination, text, min_timeout=2.0)
        result_type = getattr(result, "type", None)
        payload = {
            "mode": "meshcore_send_msg_with_retry",
            "destination": str(destination),
            "event_type": getattr(result_type, "name", str(result_type)),
            "result": getattr(result, "payload", {}),
        }
        if result is None:
            raise RuntimeError("Direktnachricht ohne ACK-Event abgelaufen")
        if result_type == EventType.ERROR:
            raise RuntimeError(str(getattr(result, "payload", {})))

        self.message_store.add("out", None, self.self_pubkey_prefix, text, {"send_result": payload, **(meta or {})})
        self.log("INFO", "Direktnachricht ueber MeshCore-API gesendet", payload)
        return payload

    async def send_direct_serial(self, req: SerialSendRequest) -> dict[str, Any]:
        await self.ensure_connected()
        assert self.mesh is not None

        payload_text = req.payload
        if req.mode == "text":
            raw_payload = payload_text.encode("utf-8")
            if req.append_newline:
                raw_payload += b"\n"
            note = "Direkter UART-Text"
        elif req.mode == "hex":
            compact = "".join(payload_text.split())
            if len(compact) % 2:
                raise RuntimeError("Hex-Payload muss eine gerade Anzahl Zeichen haben")
            raw_payload = bytes.fromhex(compact)
            note = "Direkter UART-Hexwrite"
        else:
            compact = "".join(payload_text.split())
            if len(compact) % 2:
                raise RuntimeError("MeshCore-Hex-Payload muss eine gerade Anzahl Zeichen haben")
            raw_payload = bytes.fromhex(compact)
            note = "Direkter MeshCore-Frame"

        connection = getattr(getattr(self.mesh, "connection_manager", None), "connection", None)
        if connection is None:
            raise RuntimeError("Keine aktive serielle Verbindung vorhanden")

        async with self.send_lock:
            if req.mode == "meshcore_hex":
                await connection.send(raw_payload)
            else:
                transport = getattr(connection, "transport", None)
                if transport is None:
                    raise RuntimeError("Serieller Transport ist nicht verbunden")
                self.trace_serial("tx", "uart_raw", raw_payload, note)
                transport.write(raw_payload)

        return self.log(
            "INFO",
            "Direkter Serial-Befehl gesendet",
            {"mode": req.mode, "bytes": len(raw_payload), "append_newline": req.append_newline},
        )

    async def apply_device_patch(self, req: DeviceApplyRequest) -> dict[str, Any]:
        await self.ensure_connected()
        assert self.mesh is not None
        cfg = self.config_store.load()
        if req.name is not None:
            cfg["meshcore"]["device"]["name"] = req.name
            await self.mesh.commands.set_name(req.name)
        if req.tx_power is not None:
            cfg["meshcore"]["device"]["tx_power"] = req.tx_power
            await self.mesh.commands.set_tx_power(req.tx_power)
        for key in ("freq", "bw", "sf", "cr"):
            val = req.radio.get(key)
            if val is not None:
                cfg["meshcore"]["radio"][key] = val
        radio = cfg["meshcore"]["radio"]
        if all(radio.get(k) is not None for k in ("freq", "bw", "sf", "cr")):
            await self.mesh.commands.set_radio(float(radio["freq"]), float(radio["bw"]), int(radio["sf"]), int(radio["cr"]))
        self.config_store.save(cfg)
        return self.log("INFO", "Geraetekonfiguration angewendet", {"meshcore": cfg["meshcore"]})

    async def send_device_advert(self, flood: bool = False) -> dict[str, Any]:
        await self.ensure_connected()
        assert self.mesh is not None
        if not hasattr(self.mesh.commands, "send_advert"):
            raise RuntimeError("meshcore.commands.send_advert() ist nicht verfuegbar")

        result = await self.mesh.commands.send_advert(flood=flood)
        result_type = getattr(result, "type", None)
        payload = {
            "flood": flood,
            "event_type": getattr(result_type, "name", str(result_type)),
            "result": getattr(result, "payload", {}),
        }
        if result_type == EventType.ERROR:
            raise RuntimeError(str(getattr(result, "payload", {})))
        return self.log("INFO", "Advert gesendet", payload)

    async def upsert_device_channel(self, req: DeviceChannelUpsertRequest) -> dict[str, Any]:
        await self.ensure_connected()
        assert self.mesh is not None
        secret = bytes.fromhex(req.secret_hex) if req.secret_hex else None
        result = await self.mesh.commands.set_channel(req.channel_idx, req.name, secret)
        if getattr(result, "type", None) == EventType.ERROR:
            raise RuntimeError(str(getattr(result, "payload", {})))
        channels = await self.get_device_channels(write_back_config=True)
        return self.log("INFO", "Channel gesetzt", {"channel_idx": req.channel_idx, "name": req.name, "channels": channels})

    async def remove_device_channel(self, channel_idx: int) -> dict[str, Any]:
        await self.ensure_connected()
        assert self.mesh is not None
        if not hasattr(self.mesh.commands, "remove_channel"):
            raise RuntimeError("Die installierte meshcore-Version unterstuetzt remove_channel() noch nicht.")
        result = await self.mesh.commands.remove_channel(channel_idx)
        if getattr(result, "type", None) == EventType.ERROR:
            raise RuntimeError(str(getattr(result, "payload", {})))
        channels = await self.get_device_channels(write_back_config=True)
        return self.log("INFO", "Channel entfernt", {"channel_idx": channel_idx, "channels": channels})

    async def update_connection_selection(self, req: ConnectionSelectionRequest) -> dict[str, Any]:
        cfg = self.config_store.load()
        conn = cfg["meshcore"]["connection"]
        conn["type"] = req.type
        if req.port is not None:
            conn["port"] = req.port
        if req.baudrate is not None:
            conn["baudrate"] = req.baudrate
        if req.auto_detect is not None:
            conn["auto_detect"] = req.auto_detect
        if req.host is not None:
            conn["host"] = req.host
        if req.address is not None:
            conn["address"] = req.address
        if req.pin is not None:
            conn["pin"] = req.pin
        self.config_store.save(cfg)
        await self.ensure_connected(force_reconnect=True)
        return self.log("INFO", "Verbindungskonfiguration gespeichert", {"connection": conn})

    async def test_connection(self) -> dict[str, Any]:
        await self.ensure_connected(force_reconnect=True)
        await self.refresh_device_metrics(force=True)
        return self.log("INFO", "Verbindungstest erfolgreich", {"active_port": self.state.active_port})

    async def reboot_device(self) -> dict[str, Any]:
        await self.ensure_connected()
        assert self.mesh is not None
        result = await self.mesh.commands.reboot()
        self.state.connected = False
        self.log("WARNING", "Heltec-Neustart angefordert", {"result": getattr(result, "payload", {})})
        return self.log("WARNING", "Heltec wird neu gestartet", {"active_port": self.state.active_port})

    async def reboot_pi(self) -> dict[str, Any]:
        command = "sleep 2 && (sudo systemctl reboot || sudo reboot || reboot)"
        try:
            subprocess.Popen(
                ["/bin/sh", "-c", command],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception as exc:
            self.log("ERROR", "Pi-Neustart konnte nicht gestartet werden", {"error": str(exc)})
            raise
        return self.log("WARNING", "Raspberry-Pi-Neustart angefordert")

    async def patch_bot_config(self, patch: BotConfigPatch) -> dict[str, Any]:
        """Persist bot, profile and bridge changes coming from the web UI."""
        cfg = self.config_store.load()
        bot = cfg["bot"]
        if patch.enabled is not None:
            bot["enabled"] = patch.enabled
        if patch.command_prefix is not None:
            bot["command_prefix"] = patch.command_prefix
        if patch.allowed_channels is not None:
            bot["allowed_channels"] = patch.allowed_channels
        if patch.poll_seconds is not None:
            bot["poll_seconds"] = max(float(patch.poll_seconds), 0.5)
        if patch.custom_commands is not None:
            normalized = {
                str(key).strip().replace("!", "").lower(): str(value).strip()
                for key, value in patch.custom_commands.items()
                if str(key).strip() and str(value).strip()
            }
            bot["custom_commands"] = normalized
            bot["auto_responses"] = normalized
        if patch.bot_profiles is not None:
            profiles = []
            for index, raw in enumerate(patch.bot_profiles):
                if not isinstance(raw, dict):
                    continue
                channels = []
                for channel in raw.get("allowed_channels", []):
                    try:
                        channels.append(int(channel))
                    except Exception:
                        continue
                if not channels:
                    continue
                profiles.append(
                    {
                        "id": str(raw.get("id") or f"profile-{index + 1}"),
                        "name": str(raw.get("name") or f"Bot {index + 1}").strip(),
                        "enabled": bool(raw.get("enabled", True)),
                        "allowed_channels": channels,
                        "custom_commands": self._normalize_custom_commands(raw.get("custom_commands")),
                    }
                )
            if profiles:
                bot["profiles"] = profiles
                bot["allowed_channels"] = sorted({channel for profile in profiles for channel in profile["allowed_channels"]})
        if patch.telegram_bridge is not None:
            current = bot.get("telegram_bridge", {})
            current.update(
                {
                    "enabled": bool(patch.telegram_bridge.get("enabled", False)),
                    "bot_token": str(patch.telegram_bridge.get("bot_token", "") or "").strip(),
                    "chat_id": str(patch.telegram_bridge.get("chat_id", "") or "").strip(),
                    "mesh_channel_idx": int(patch.telegram_bridge.get("mesh_channel_idx", 0) or 0),
                    "mesh_to_telegram": bool(patch.telegram_bridge.get("mesh_to_telegram", True)),
                    "telegram_to_mesh": bool(patch.telegram_bridge.get("telegram_to_mesh", True)),
                    "poll_seconds": max(float(patch.telegram_bridge.get("poll_seconds", 4) or 4), 1.0),
                }
            )
            bot["telegram_bridge"] = current
        if patch.telegram_bridges is not None:
            bridges = []
            for index, raw in enumerate(patch.telegram_bridges):
                if not isinstance(raw, dict):
                    continue
                normalized = self._normalize_telegram_bridge(raw, index)
                if not normalized["bot_token"] or not normalized["chat_id"]:
                    continue
                bridges.append(normalized)
            bot["telegram_bridges"] = bridges
            if bridges:
                bot["telegram_bridge"] = bridges[0]
        self.config_store.save(cfg)
        return self.log("INFO", "Bot-Konfiguration gespeichert", {"bot": bot})

    async def on_connected(self, event: Any) -> None:
        self.state.connected = True
        self.log("INFO", "Connected-Event empfangen", {"payload": getattr(event, "payload", {})})

    async def on_disconnected(self, event: Any) -> None:
        self.state.connected = False
        self.log("WARNING", "Disconnected-Event empfangen", {"payload": getattr(event, "payload", {})})

    async def on_ack(self, event: Any) -> None:
        payload = getattr(event, "payload", {}) or {}
        code = payload.get("code") if isinstance(payload, dict) else str(payload)
        self.state.last_ack = code
        self.log("INFO", "ACK empfangen", {"code": code, "payload": payload})

    async def on_direct_message(self, event: Any) -> None:
        await self._handle_incoming("direct", getattr(event, "payload", {}) or {})

    async def on_channel_message(self, event: Any) -> None:
        await self._handle_incoming("channel", getattr(event, "payload", {}) or {})

    def _dedupe_key(self, sender: Any, text: str, channel_idx: Any) -> str:
        return hashlib.sha256(f"{sender}|{channel_idx}|{text}".encode("utf-8")).hexdigest()

    def _outbound_echo_key(self, text: str, channel_idx: Any, sender: Any = None) -> str:
        return hashlib.sha256(f"{sender}|{channel_idx}|{text}".encode("utf-8")).hexdigest()

    def _extract_command_line(self, text: str, prefix: str) -> Optional[str]:
        if not text:
            return None
        clean = text.strip()
        candidates = [clean]
        for separator in (":", ">", "|"):
            if separator in clean:
                candidates.append(clean.rsplit(separator, 1)[-1].strip())
        for candidate in candidates:
            if candidate.startswith(prefix):
                return candidate[len(prefix):].strip()
        return None

    def _is_bot_command_message(self, text: str, prefix: str) -> bool:
        """Detect whether a message is intended for the bot and should not be bridged."""
        command_line = self._extract_command_line(text, prefix)
        if not command_line:
            return False
        parts = command_line.split()
        if not parts:
            return False
        cmd = parts[0].lower()
        bot_cfg = self.config_store.load()["bot"]
        builtin_commands = {"help", "hilfe", "ping", "wetter", "forecast", "vorhersage", "status", "info"}
        if cmd in builtin_commands:
            return True
        shared_commands = self._normalize_custom_commands(bot_cfg.get("custom_commands") or bot_cfg.get("auto_responses"))
        if cmd in shared_commands:
            return True
        for profile in self._get_bot_profiles(bot_cfg):
            if cmd in self._normalize_custom_commands(profile.get("custom_commands")):
                return True
        return False

    def _extract_display_sender(self, inbound_payload: dict[str, Any], prefix: str) -> str:
        sender = inbound_payload.get("pubkey_prefix") or inbound_payload.get("sender") or inbound_payload.get("from")
        if sender:
            return str(sender)

        text = str(inbound_payload.get("text", "")).strip()
        if not text:
            return "unbekannt"

        for separator in (":", ">", "|"):
            marker = f"{separator} {prefix}"
            idx = text.find(marker)
            if idx > 0:
                candidate = text[:idx].strip()
                if candidate:
                    return candidate

        return "unbekannt"

    def _split_sender_prefix(self, text: str) -> tuple[Optional[str], str]:
        clean = str(text or "").strip()
        for separator in (":", ">", "|"):
            if separator not in clean:
                continue
            left, right = clean.split(separator, 1)
            left = left.strip()
            right = right.strip()
            if left and right and len(left) <= 48:
                return left, right
        return None, clean

    def _extract_telegram_sender_name(self, message: dict[str, Any]) -> str:
        sender = message.get("from", {}) or {}
        sender_chat = message.get("sender_chat", {}) or {}
        forward_origin = message.get("forward_origin", {}) or {}
        chat = message.get("chat", {}) or {}

        full_name = " ".join(
            part for part in [sender.get("first_name"), sender.get("last_name")] if part
        ).strip()

        return (
            sender.get("username")
            or full_name
            or sender_chat.get("title")
            or sender_chat.get("username")
            or forward_origin.get("sender_user_name")
            or chat.get("title")
            or "Telegram"
        )

    def _build_channel_command(self, channel_idx: int, text: str, timestamp: Optional[int] = None) -> bytes:
        timestamp_bytes = int(timestamp if timestamp is not None else time.time()).to_bytes(4, "little")
        return b"\x03\x00" + int(channel_idx).to_bytes(1, "little") + timestamp_bytes + text.encode("utf-8")

    async def _send_channel_raw_fallback(self, channel_idx: int, text: str) -> dict[str, Any]:
        assert self.mesh is not None
        raw_payload = self._build_channel_command(channel_idx, text)
        self.log("INFO", "Direkter Serial-Write vorbereitet", {"channel_idx": channel_idx, "hex": raw_payload.hex()})
        connection = getattr(getattr(self.mesh, "connection_manager", None), "connection", None)
        if connection is not None and hasattr(connection, "send"):
            await connection.send(raw_payload)
        else:
            self.trace_serial("tx", "meshcore_frame", raw_payload, "MeshCore payload via connection manager fallback")
            await self.mesh.cx.send(raw_payload)
        self.log("WARNING", "Direkter Serial-Write abgeschlossen", {"channel_idx": channel_idx, "text": text})
        return {"mode": "raw_fallback", "channel_idx": channel_idx}

    async def fetch_pending_messages(self, reason: str = "manual") -> int:
        if not (self.mesh and self.mesh.is_connected):
            return 0

        async with self.fetch_lock:
            if not (self.mesh and self.mesh.is_connected):
                return 0

            drained = 0
            skipped = 0
            iterations = 0
            while self.mesh and self.mesh.is_connected and not self.stop_event.is_set():
                iterations += 1
                if iterations > 50:
                    self.log("WARNING", "Nachrichtenabruf abgebrochen, zu viele Iterationen", {"reason": reason, "drained": drained, "skipped": skipped})
                    break
                result = await self.mesh.commands.get_msg(timeout=2.5)
                if result is None:
                    break
                if result.type == EventType.NO_MORE_MSGS:
                    break
                if result.type == EventType.ERROR:
                    payload = getattr(result, "payload", {}) or {}
                    self.log("WARNING", "Nachrichtenabruf gestoppt", {"reason": reason, "payload": payload})
                    break
                if result.type in (EventType.CHANNEL_MSG_RECV, EventType.CONTACT_MSG_RECV):
                    drained += 1
                    continue
                skipped += 1
                self.log("INFO", "Nicht-Nachrichten-Event beim Drain uebersprungen", {"reason": reason, "event_type": getattr(result.type, "name", str(result.type))})
                continue

            if drained:
                self.log("INFO", "Ausstehende Nachrichten abgeholt", {"count": drained, "skipped": skipped, "reason": reason})
            return drained

    async def _handle_incoming(self, kind: str, payload: dict[str, Any]) -> None:
        """Central ingress path for channel and direct messages.

        The order matters:
        1. Ignore local echoes and duplicates.
        2. Store/log the raw inbound message.
        3. Run bridge forwarding.
        4. Resolve the matching bot profile and execute commands.
        """
        text = str(payload.get("text", "")).strip()
        sender = payload.get("pubkey_prefix") or payload.get("sender") or payload.get("from")
        channel_idx = payload.get("channel_idx")
        bot_cfg = self.config_store.load()["bot"]
        dedupe_window_seconds = max(float(bot_cfg.get("dedupe_window_seconds", 180) or 180), 0.5)
        now = time.time()
        self._prune_dedupe_cache(now=now, window_seconds=dedupe_window_seconds)
        outbound_echo_key = self._outbound_echo_key(text, channel_idx, sender)
        if outbound_echo_key in self.recent_outbound:
            self.log("INFO", "Eigenes Echo ignoriert", {"kind": kind, "channel_idx": channel_idx, "text": text})
            return
        dedupe_key = self._dedupe_key(sender=sender, text=text, channel_idx=channel_idx)
        previous_seen = self.dedupe_seen.get(dedupe_key)
        if previous_seen is not None and (now - previous_seen) <= dedupe_window_seconds:
            self.log("INFO", "Nachricht innerhalb Dedupe-Fenster ignoriert", {"kind": kind, "channel_idx": channel_idx, "sender": sender, "text": text, "window_seconds": dedupe_window_seconds})
            return
        self.dedupe_seen[dedupe_key] = now

        self.message_store.add("in", channel_idx, sender, text, payload)
        self.log("INFO", "Nachricht empfangen", {"kind": kind, "channel_idx": channel_idx, "sender": sender, "text": text})

        if kind == "channel":
            try:
                display_sender = sender or self._extract_display_sender(payload, bot_cfg.get("command_prefix", "!"))
                await self.bridge_mesh_to_telegram(channel_idx, display_sender, text)
            except Exception as exc:
                self.log("ERROR", "MeshCore -> Telegram fehlgeschlagen", {"error": str(exc), "channel_idx": channel_idx, "text": text})
        if not bot_cfg.get("enabled", True):
            return
        if bot_cfg.get("ignore_own_messages", True) and sender and self.self_pubkey_prefix and str(sender).startswith(self.self_pubkey_prefix):
            return

        prefix = bot_cfg.get("command_prefix", "!")
        command_line = self._extract_command_line(text, prefix)
        if not command_line:
            return

        profile = None
        if kind == "channel":
            profile = self._find_profile_for_channel(bot_cfg, channel_idx)
            if profile is None:
                return

        reply = await self.process_command(command_line, payload, profile=profile)
        if reply:
            async def _send_reply() -> None:
                try:
                    if kind == "direct":
                        await self._send_direct_reply(sender, reply, meta={"reply_to": payload, "reply_kind": "direct"})
                    else:
                        await self.send_channel_message(int(channel_idx or 0), reply, meta={"reply_to": payload, "reply_kind": "channel"})
                except Exception as exc:
                    self.log("ERROR", "Auto-Antwort fehlgeschlagen", {"error": str(exc), "reply": reply, "channel_idx": channel_idx, "kind": kind, "sender": sender})

            asyncio.create_task(_send_reply())

    async def process_command(self, command_line: str, inbound_payload: dict[str, Any], profile: Optional[dict[str, Any]] = None) -> Optional[str]:
        """Return a reply string for one parsed command or None if unhandled."""
        parts = command_line.split()
        if not parts:
            return None
        cmd = parts[0].lower()
        bot_cfg = self.config_store.load()["bot"]
        shared_commands = self._normalize_custom_commands(bot_cfg.get("custom_commands") or bot_cfg.get("auto_responses"))
        profile_commands = self._normalize_custom_commands((profile or {}).get("custom_commands"))
        custom_commands = {**shared_commands, **profile_commands}
        profile_name = (profile or {}).get("name") or bot_cfg.get("name", "MeshcoreBot")

        if cmd == "ping":
            hops = (
                inbound_payload.get("hop_count")
                or inbound_payload.get("hops")
                or inbound_payload.get("path_len")
            )
            snr = (
                inbound_payload.get("snr")
                or inbound_payload.get("SNR")
                or inbound_payload.get("last_snr")
            )
            channel_idx = inbound_payload.get("channel_idx", "?")
            sender = self._extract_display_sender(inbound_payload, bot_cfg.get("command_prefix", "!"))
            sender_ts = inbound_payload.get("sender_timestamp")
            hops = hops if hops not in (None, "") else "n/a"
            snr = snr if snr not in (None, "") else "n/a"
            if isinstance(sender_ts, (int, float)) and sender_ts:
                try:
                    sender_time = time.strftime("%H:%M:%S", time.localtime(sender_ts))
                except Exception:
                    sender_time = str(sender_ts)
            else:
                sender_time = "?"
            return self._fit_meshcore_text(
                f"🏓 {sender} CH:{channel_idx} H:{hops} SNR:{snr} {sender_time}"
            )

        if cmd == "status":
            last_error = self._short_value(self.state.last_error or "-", 22)
            active_port = self._short_value(self.state.active_port or "-", 18)
            return self._fit_meshcore_text(
                f"🤖 {profile_name} {'on' if self.state.connected else 'off'} | {active_port} | ACK:{self.state.last_ack or '-'} | Err:{last_error}"
            )

        if cmd in {"help", "hilfe"}:
            return self._build_help_text(bot_cfg)

        if cmd in custom_commands:
            return custom_commands[cmd]

        if cmd == "wetter":
            return self._fit_meshcore_text(
                f"🌤️ {self._short_value(self.config_store.load()['bot']['weather'].get('label', 'Standort'), 18)}: {await self.fetch_weather()}"
            )

        if cmd in {"forecast", "vorhersage"}:
            return await self.fetch_weather_forecast()

        if cmd == "info":
            host = platform.node() or "?"
            arch = platform.machine() or "?"
            uptime = self._format_duration(time.time() - self.state.started_at)
            now = time.strftime("%H:%M")
            return self._fit_meshcore_text(
                f"🧠 {self._short_value(profile_name, 16)} | {self._short_value(host, 16)} {self._short_value(arch, 8)} | {self._short_value(self.state.active_port or '-', 16)} | Up:{uptime} | {now}"
            )

        return self._fit_meshcore_text(f"Unbekannt: {cmd}. Nutze !help")

    async def fetch_weather(self) -> str:
        cfg = self.config_store.load()["bot"]["weather"]
        params = {
            "latitude": cfg["latitude"],
            "longitude": cfg["longitude"],
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code",
            "timezone": "auto",
        }
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.get("https://api.open-meteo.com/v1/forecast", params=params)
            response.raise_for_status()
            data = response.json()["current"]
        return (
            f"{data['temperature_2m']}C {data['relative_humidity_2m']}% "
            f"{data['wind_speed_10m']}km/h C{data['weather_code']}"
        )

    async def fetch_weather_forecast(self) -> str:
        cfg = self.config_store.load()["bot"]["weather"]
        params = {
            "latitude": cfg["latitude"],
            "longitude": cfg["longitude"],
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_probability_max,weather_code",
            "timezone": "auto",
            "forecast_days": 4,
        }
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.get("https://api.open-meteo.com/v1/forecast", params=params)
            response.raise_for_status()
            daily = response.json()["daily"]

        label = cfg.get("label", "Standort")
        days: list[str] = []
        times = daily.get("time", [])
        max_values = daily.get("temperature_2m_max", [])
        min_values = daily.get("temperature_2m_min", [])
        precip_values = daily.get("precipitation_probability_max", [])

        for index in range(1, min(len(times), 4)):
            day_str = str(times[index])
            try:
                weekday = time.strftime("%a", time.strptime(day_str, "%Y-%m-%d"))
            except Exception:
                weekday = day_str
            max_temp = max_values[index] if index < len(max_values) else "-"
            min_temp = min_values[index] if index < len(min_values) else "-"
            precip = precip_values[index] if index < len(precip_values) else "-"
            days.append(f"{weekday} {max_temp}/{min_temp}C {precip}%")

        if not days:
            return f"📅 {self._short_value(label, 18)}: keine Daten"

        return self._fit_meshcore_text(f"📅 {self._short_value(label, 18)}: " + " | ".join(days))

    async def get_builtin_command_previews(self) -> list[dict[str, str]]:
        bot_cfg = self.config_store.load()["bot"]
        prefix = str(bot_cfg.get("command_prefix", "!") or "!")
        profiles = self._get_bot_profiles(bot_cfg)
        profile = profiles[0] if profiles else None
        sample_channel = int((profile or {}).get("allowed_channels", [0])[0] if (profile or {}).get("allowed_channels") else 0)
        sample_ping_payload = {
            "channel_idx": sample_channel,
            "pubkey_prefix": "NODE123",
            "hop_count": 1,
            "snr": self.device_stats_radio.get("last_snr", 9.5) if isinstance(self.device_stats_radio, dict) else 9.5,
            "sender_timestamp": int(time.time()),
        }

        async def preview(command_line: str, payload: Optional[dict[str, Any]] = None) -> str:
            try:
                return await self.process_command(command_line, payload or {"channel_idx": sample_channel}, profile=profile) or "-"
            except Exception as exc:
                return self._fit_meshcore_text(f"Fehler: {exc}")

        return [
            {
                "command": f"{prefix}help",
                "description": "zeigt die feste Befehlsuebersicht",
                "response": await preview("help"),
                "config_hint": "Prefix: Webinterface oder config.yaml -> bot.command_prefix | Text: app.py -> _build_help_text()",
            },
            {
                "command": f"{prefix}ping",
                "description": "liefert Hops, SNR und Zeitstempel",
                "response": await preview("ping", sample_ping_payload),
                "config_hint": "Textformat: app.py -> process_command() Fall 'ping' | Werte kommen aus der empfangenen Nachricht",
            },
            {
                "command": f"{prefix}wetter",
                "description": "aktuelles Wetter fuer den konfigurierten Standort",
                "response": await preview("wetter"),
                "config_hint": "Prefix: Webinterface oder config.yaml -> bot.command_prefix | Standort: config.yaml -> bot.weather | Textformat: app.py -> process_command()/fetch_weather()",
            },
            {
                "command": f"{prefix}forecast",
                "description": "Vorhersage fuer die naechsten Tage",
                "response": await preview("forecast"),
                "config_hint": "Prefix: Webinterface oder config.yaml -> bot.command_prefix | Standort: config.yaml -> bot.weather | Textformat: app.py -> process_command()/fetch_weather_forecast()",
            },
            {
                "command": f"{prefix}status",
                "description": "Verbindungs- und Bot-Status",
                "response": await preview("status"),
                "config_hint": "Textformat: app.py -> process_command() Fall 'status' | Inhalte indirekt ueber Verbindung, ACKs und Bot-Konfiguration",
            },
            {
                "command": f"{prefix}info",
                "description": "Host-, Port- und Laufzeitinfos",
                "response": await preview("info"),
                "config_hint": "Textformat: app.py -> process_command() Fall 'info' | Inhalte indirekt ueber Hostsystem und aktive Verbindung",
            },
        ]


def deep_merge(base: dict[str, Any], other: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge user config onto defaults."""
    for key, value in other.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            base[key] = deep_merge(base[key], value)
        else:
            base[key] = value
    return base


config_store = ConfigStore(CONFIG_PATH)
message_store = MessageStore(DB_PATH)
log_buffer = LogBuffer()
serial_trace_buffer = SerialTraceBuffer()
service = MeshcoreBotService(config_store, message_store, log_buffer, serial_trace_buffer)

meshcore_logger = logging.getLogger("meshcore")
meshcore_logger.setLevel(logging.DEBUG)


class BufferHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        log_buffer.add(record.levelname, record.name, record.getMessage())


if not any(isinstance(handler, BufferHandler) for handler in meshcore_logger.handlers):
    meshcore_logger.addHandler(BufferHandler())


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI startup/shutdown hook that controls the bot service."""
    await service.start()
    yield
    await service.stop()


app = FastAPI(title="MeshCore Bot", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=config_store.load()["web"].get("cors", ["*"]),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return INDEX_HTML


@app.get("/api/status")
async def get_status() -> dict[str, Any]:
    if service.mesh and service.mesh.is_connected:
        await service.refresh_device_metrics(force=False)
    return {
        "connected": service.state.connected,
        "last_error": service.state.last_error,
        "last_ack": service.state.last_ack,
        "outbound_queue_size": service.state.outbound_queue_size,
        "uptime_seconds": int(time.time() - service.state.started_at),
        "channel_source": service.state.channel_source,
        "synced_channels": service.state.synced_channels,
        "active_port": service.state.active_port,
        "system_metrics": service.get_system_metrics(),
        "device_metrics": service.get_device_metrics(),
    }


@app.get("/api/config")
async def get_config() -> dict[str, Any]:
    return config_store.load()


@app.get("/api/commands/builtin")
async def get_builtin_commands() -> list[dict[str, str]]:
    return await service.get_builtin_command_previews()


@app.get("/api/connections")
async def get_connections() -> dict[str, Any]:
    current = config_store.load()["meshcore"]["connection"]
    serial_ports = service.list_available_serial_ports()
    return {"current": current, "serial_ports": serial_ports}


@app.post("/api/connections/select")
async def select_connection(req: ConnectionSelectionRequest) -> dict[str, Any]:
    try:
        return await service.update_connection_selection(req)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/device/test")
async def test_device() -> dict[str, Any]:
    try:
        return await service.test_connection()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/device/channels")
async def get_device_channels() -> list[dict[str, Any]]:
    try:
        return await service.get_device_channels(write_back_config=True)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/device/channels")
async def upsert_device_channel(req: DeviceChannelUpsertRequest) -> dict[str, Any]:
    try:
        return await service.upsert_device_channel(req)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.delete("/api/device/channels/{channel_idx}")
async def delete_device_channel(channel_idx: int) -> dict[str, Any]:
    try:
        return await service.remove_device_channel(channel_idx)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.put("/api/config/bot")
async def update_bot_config(patch: BotConfigPatch) -> dict[str, Any]:
    return await service.patch_bot_config(patch)


@app.post("/api/config/device/apply")
async def apply_device_config(req: DeviceApplyRequest) -> dict[str, Any]:
    try:
        return await service.apply_device_patch(req)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/device/advert")
async def send_device_advert(flood: bool = False) -> dict[str, Any]:
    try:
        return await service.send_device_advert(flood=flood)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/messages")
async def get_messages(limit: int = 50) -> list[dict[str, Any]]:
    return message_store.list(min(max(limit, 1), 200))


@app.get("/api/logs")
async def get_logs(limit: int = 200) -> list[dict[str, Any]]:
    return log_buffer.list(min(max(limit, 1), 500))


@app.get("/api/serial/logs")
async def get_serial_logs(limit: int = 200) -> list[dict[str, Any]]:
    return serial_trace_buffer.list(min(max(limit, 1), 1000))


@app.get("/api/logs/download")
async def download_logs() -> PlainTextResponse:
    lines = [
        f"[{entry['ts']}] {entry['level']} {entry['source']}: {entry['message']}"
        for entry in log_buffer.list(500)
    ]
    return PlainTextResponse(
        "\n".join(lines),
        headers={"Content-Disposition": 'attachment; filename="meshcorebot-logs.txt"'},
    )


@app.post("/api/send/channel")
async def send_channel(req: SendChannelRequest) -> JSONResponse:
    try:
        result = await service.send_channel_message(req.channel_idx, req.text)
        return JSONResponse(result)
    except Exception as exc:
        service.log("ERROR", "API /api/send/channel fehlgeschlagen", {"channel_idx": req.channel_idx, "text": req.text, "error": repr(exc), "error_type": type(exc).__name__})
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/send/serial")
async def send_serial(req: SerialSendRequest) -> JSONResponse:
    try:
        result = await service.send_direct_serial(req)
        return JSONResponse(result)
    except Exception as exc:
        service.log(
            "ERROR",
            "API /api/send/serial fehlgeschlagen",
            {"mode": req.mode, "error": repr(exc), "error_type": type(exc).__name__},
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/actions/restart/device")
async def restart_device() -> dict[str, Any]:
    try:
        return await service.reboot_device()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/actions/restart/pi")
async def restart_pi() -> dict[str, Any]:
    try:
        return await service.reboot_pi()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


if __name__ == "__main__":
    cfg = config_store.load()["web"]
    uvicorn.run("app:app", host=cfg.get("host", "0.0.0.0"), port=int(cfg.get("port", 8080)), reload=False)
