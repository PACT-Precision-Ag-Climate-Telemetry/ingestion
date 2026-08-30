# AgriCarbon IoT — Architecture & Data Flow Diagrams

> เอกสารนี้อธิบายสถาปัตยกรรมและการไหลของข้อมูลทั้งระบบ รวมถึง TCP endpoint ที่เพิ่มใหม่

---

## 1. System Architecture (ภาพรวมสถาปัตยกรรม)

```
                          ┌─────────────────────────────────────────────────────────┐
                          │                    FIELD (แปลงนา)                         │
                          │                                                           │
                          │   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
                          │   │  CO₂ Sensor │    │  CH₄ Sensor │    │ Water Level │  │
                          │   │  (MH-Z19B)  │    │   (MQ-4)    │    │ (HC-SR04P)  │  │
                          │   └──────┬──────┘    └──────┬──────┘    └──────┬──────┘  │
                          │          │                  │                  │          │
                          │   ┌──────┴──────┐    ┌──────┴──────┐    ┌──────┴──────┐  │
                          │   │ Soil Sensor │    │   GNSS GPS  │    │  Battery +  │  │
                          │   │ (Capacitive)│    │ (SIM7670G)  │    │   Solar     │  │
                          │   └──────┬──────┘    └──────┬──────┘    └──────┬──────┘  │
                          │          │                  │                  │          │
                          │          └──────────┬───────┴──────────────────┘          │
                          │                     ▼                                      │
                          │            ┌─────────────────┐                             │
                          │            │   ACU (ESP32-S3) │                             │
                          │            │   + SIM7670G 4G  │                             │
                          │            │                  │                             │
                          │            │  • Read sensors  │                             │
                          │            │  • Average 1 min │                             │
                          │            │  • Buffer flash  │                             │
                          │            │  • Send 15-30min │                             │
                          │            └────────┬─────────┘                             │
                          └─────────────────────┼───────────────────────────────────────┘
                                                │
                                    ┌───────────┴───────────┐
                                    │   4G/LTE Network      │
                                    │   (SIM7670G/SIM7600)  │
                                    └───────────┬───────────┘
                                                │
                         ┌──────────────────────┴──────────────────────┐
                         │                                               │
                    HTTP POST                              TCP (NDJSON)
                    (Port 8000)                            (Port 9600)
                         │                                               │
                         ▼                                               ▼
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                              BACKEND (FastAPI + Uvicorn)                               │
│                                                                                        │
│  ┌─────────────────────┐          ┌──────────────────────────────────────────────┐   │
│  │   HTTP REST API     │          │         TCP Server (asyncio)                  │   │
│  │   (FastAPI Router)  │          │         Port 9600 — NDJSON Protocol           │   │
│  │                     │          │                                                │   │
│  │  POST /devices/     │          │  Client connects → sends JSON per line       │   │
│  │   {id}/telemetry    │          │  Server responds: {"status":"ok","id":N}     │   │
│  │                     │          │  Streaming: multiple lines per connection    │   │
│  │  GET  /devices/     │          │                                                │   │
│  │   {id}/telemetry    │          │  ┌────────────────────────────────────────┐  │   │
│  │                     │          │  │  _handle_client()                      │  │   │
│  │  GET  /plots/       │          │  │    • Read 64KB chunks                  │  │   │
│  │   {id}/dashboard    │          │  │    • Split by \n (NDJSON framing)      │  │   │
│  │                     │          │  │    • _process_line() per line          │  │   │
│  │  POST /farmers      │          │  │       → json.loads()                   │  │   │
│  │  POST /plots        │          │  │       → TelemetryIn (Pydantic)         │  │   │
│  │  ...etc             │          │  │       → asyncio.to_thread(ingest)      │  │   │
│  └────────┬────────────┘          │  └──────────────┬─────────────────────────┘  │   │
│           │                       └─────────────────┼────────────────────────────┘   │
│           │                                         │                               │
│           └──────────────┬──────────────────────────┘                               │
│                          ▼                                                          │
│           ┌──────────────────────────────────────────────┐                          │
│           │     Shared Ingestion Service                 │                          │
│           │     (telemetry_ingest.py)                    │                          │
│           │                                              │                          │
│           │  1. Validate / auto-register device          │                          │
│           │  2. Calibrate raw sensor values              │                          │
│           │     • ch4_raw_adc → ppm (MQ-4 curve)        │                          │
│           │     • moisture_raw → pct (capacitive)        │                          │
│           │     • raw_distance → water_level_cm          │                          │
│           │     • battery_voltage → battery_pct          │                          │
│           │  3. Write Telemetry record                   │                          │
│           │  4. Update Device state (last_seen, etc.)    │                          │
│           │  5. Update active Season (current values)    │                          │
│           │  6. Run alert checks                         │                          │
│           │     • GPS theft (>50m move)                  │                          │
│           │     • Low battery (<20%)                     │                          │
│           │     • Sensor fault                           │                          │
│           └──────────────┬───────────────────────────────┘                          │
│                          │                                                           │
│                          ▼                                                           │
│           ┌──────────────────────────────────────────────┐                          │
│           │         SQLite Database (WAL mode)           │                          │
│           │                                              │                          │
│           │  farmers │ devices │ plots │ seasons         │                          │
│           │  telemetry │ awd_cycles │ emissions_daily    │                          │
│           │  alerts │ growth_milestones                  │                          │
│           └──────────────────────────────────────────────┘                          │
│                                                                                        │
│           ┌──────────────────────────────────────────────┐                          │
│           │     Background Jobs (APScheduler)            │                          │
│           │                                              │                          │
│           │  • AWD cycle detection    — every 15 min     │                          │
│           │  • Emission aggregation   — daily at 00:05   │                          │
│           │  • Device offline check   — every 10 min     │                          │
│           └──────────────────────────────────────────────┘                          │
│                                                                                        │
└──────────────────────────────────────────────────────────────────────────────────────┘
                                                │
                                                │ REST API (JSON)
                                                ▼
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                          FRONTEND (React.js + Vite)                                   │
│                                                                                        │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐       │
│  │  S0     │→ │  S1     │→ │  S2     │→ │  S3     │→ │  S4     │→ │  S5     │       │
│  │ Splash  │  │ ลงทะเบีย│  │ ข้อมูล   │  │ เตรียมนา│  │ สรุป    │  │ Success │       │
│  │         │  │ เกษตรกร │  │ แปลงนา  │  │ baseline│  │ +credit │  │ +bind   │       │
│  └─────────┘  └─────────┘  └─────────┘  └─────────┘  └─────────┘  └─────────┘       │
│                                                              │                         │
│                                                              ▼                         │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐                                              │
│  │  S6     │  │  S7     │  │  S8     │                                              │
│  │Dashboard│  │ Milestone│  │ แปลงของ │                                              │
│  │AWD+Gas  │  │ เจริญเติบ│  │ ฉัน      │                                              │
│  │+Soil    │  │ โต      │  │         │                                              │
│  └─────────┘  └─────────┘  └─────────┘                                              │
│                                                                                        │
│  API Client (client.js) → fetch() → http://localhost:8000/api/v1/*                   │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Sensor Data Flow — TCP vs HTTP

```
                    ┌──────────────────┐
                    │   ACU / Mock     │
                    │   (Sensor Data)  │
                    └────────┬─────────┘
                             │
                    JSON Payload
                    (TelemetryIn schema)
                             │
              ┌──────────────┴──────────────┐
              │                             │
         HTTP POST                     TCP Socket
         Port 8000                     Port 9600
              │                             │
              ▼                             ▼
    ┌─────────────────┐          ┌─────────────────────┐
    │  FastAPI Router │          │  asyncio TCP Server │
    │  devices.py     │          │  tcp_server.py      │
    │                 │          │                     │
    │  • X-API-Key    │          │  • NDJSON framing   │
    │    auth check   │          │  • Pydantic valid.  │
    │  • URL path     │          │  • device_id from   │
    │    device_id    │          │    payload (no URL) │
    └────────┬────────┘          └──────────┬──────────┘
             │                              │
             │    ┌─────────────────────────┘
             │    │
             ▼    ▼
    ┌─────────────────────────────┐
    │  telemetry_ingest.py        │
    │  (Shared Logic)             │
    │                             │
    │  1. Device lookup/register  │
    │  2. Calibration             │
    │  3. DB write (Telemetry)    │
    │  4. Device state update     │
    │  5. Season update           │
    │  6. Alert checks            │
    └────────────┬────────────────┘
                 │
                 ▼
    ┌─────────────────────────────┐
    │  SQLite (agricarbon.db)     │
    │  Table: telemetry           │
    └─────────────────────────────┘
                 │
                 │ Background Jobs (async)
                 ▼
    ┌─────────────────────────────┐
    │  APScheduler                │
    │                             │
    │  Every 15 min:              │
    │   → detect_awd_cycles()     │
    │     → awd_cycles table      │
    │                             │
    │  Daily 00:05:               │
    │   → calculate_daily_emission│
    │     → emissions_daily table │
    │                             │
    │  Every 10 min:              │
    │   → check_device_offline()  │
    │     → alerts table          │
    └─────────────────────────────┘
```

---

## 3. TCP Protocol Detail (NDJSON)

```
Client                              Server (Port 9600)
  │                                    │
  │──── TCP Connect ─────────────────▶│  [TCP] Client connected: 127.0.0.1:xxxxx
  │                                    │
  │──── {"device_id":"ACU-...",        │  → json.loads(line)
  │      "timestamp":"...",            │  → TelemetryIn(**payload)  (Pydantic)
  │      "gas":{...},                  │  → asyncio.to_thread(ingest_telemetry)
  │      "water":{...},                │  → DB write + calibrate + alerts
  │      ...} \n ─────────────────────▶│
  │                                    │
  │◀─── {"status":"ok",                │  ← response
  │      "id":1020,                    │
  │      "device_id":"ACU-...",        │
  │      "message":"OK id=1020 ..."}\n │
  │                                    │
  │──── (next payload) \n ────────────▶│  → streaming: process next line
  │                                    │
  │◀─── {"status":"ok",...} \n ────────│
  │                                    │
  │──── TCP Close ────────────────────▶│  [TCP] Client disconnected
  │                                    │
```

**Error Responses:**

```
{"status":"error","error":"invalid_json","detail":"JSON parse error: ..."}
{"status":"error","error":"missing_device_id","detail":"Field 'device_id' is required"}
{"status":"error","error":"validation_error","detail":[...pydantic errors...]}
{"status":"error","error":"device_not_found","detail":"ไม่พบอุปกรณ์: ACU-..."}
{"status":"error","error":"internal_error","detail":"..."}
```

---

## 4. Backend Module Architecture

```
backend/
├── app/
│   ├── main.py                          ← FastAPI app + lifespan (start TCP + scheduler)
│   │
│   ├── core/
│   │   ├── config.py                    ← Settings (TCP_ENABLED, TCP_HOST, TCP_PORT, ...)
│   │   ├── database.py                  ← SQLAlchemy engine + SessionLocal + WAL mode
│   │   └── timeutil.py                  ← utc_now() helper
│   │
│   ├── models/                          ← SQLAlchemy ORM Models
│   │   ├── farmer.py                    ← farmers table
│   │   ├── device.py                    ← devices table (ACU registry)
│   │   ├── plot.py                      ← plots table (แปลงนา + baseline)
│   │   ├── season.py                    ← seasons table (ฤดูกาลปลูก)
│   │   ├── telemetry.py                 ← telemetry table (time-series sensor data)
│   │   ├── awd_cycle.py                 ← awd_cycles table (wetting/drying cycles)
│   │   ├── emission_daily.py            ← emissions_daily table (flux + tCO₂e)
│   │   ├── alert.py                     ← alerts table
│   │   └── growth_milestone.py          ← growth_milestones table
│   │
│   ├── schemas/
│   │   └── schemas.py                   ← Pydantic schemas (TelemetryIn, DashboardResponse, ...)
│   │
│   ├── routers/                         ← HTTP REST API Endpoints
│   │   ├── farmers.py                   ← /api/v1/farmers
│   │   ├── plots.py                     ← /api/v1/plots (dashboard, credit, AWD, emissions)
│   │   ├── devices.py                   ← /api/v1/devices (telemetry ingest HTTP, status)
│   │   ├── seasons.py                   ← /api/v1/seasons (milestones)
│   │   └── alerts.py                    ← /api/v1/alerts
│   │
│   ├── services/                        ← Business Logic
│   │   ├── telemetry_ingest.py          ← ★ SHARED ingestion (HTTP + TCP)
│   │   ├── tcp_server.py                ← ★ TCP NDJSON server (asyncio)
│   │   ├── calibration.py               ← Sensor calibration curves
│   │   ├── awd_detection.py             ← AWD cycle state machine
│   │   ├── emission_calc.py             ← Flux → tCO₂e calculation
│   │   ├── alerts.py                    ← Alert checks (theft, battery, fault, offline)
│   │   └── mock_data.py                 ← Mock data seeding
│   │
│   └── jobs/
│       └── background.py                ← APScheduler (AWD, emission, offline check)
│
├── mock_data/
│   ├── arduino_telemetry.json           ← Sample sensor payloads (9 records)
│   ├── send_mock.py                     ← HTTP mock sender (urllib)
│   ├── send_tcp.py                      ← ★ TCP mock sender (socket + NDJSON)
│   └── live_simulator.py                ← Continuous HTTP live simulator
│
├── agricarbon.db                        ← SQLite database (WAL mode)
├── .env                                 ← Config (TCP_ENABLED, TCP_HOST, TCP_PORT, ...)
└── requirements.txt                     ← Python dependencies
```

---

## 5. Database ER Diagram

```
┌─────────────┐       ┌─────────────┐       ┌─────────────────┐
│  farmers    │       │   devices   │       │    plots        │
│─────────────│       │─────────────│       │─────────────────│
│ id (PK)     │◄──┐   │ id (PK)     │   ┌──▶│ id (PK)         │
│ national_id │   │   │ device_id   │   │   │ farmer_id (FK)──│──▶ farmers
│ title       │   │   │ firmware    │   │   │ device_id       │──▶ devices
│ first_name  │   │   │ status      │   │   │ plot_name       │
│ last_name   │   │   │ install_lat │   │   │ area_rai        │
│ phone       │   │   │ install_lng │   │   │ rice_variety    │
│ address     │   │   │ battery_pct │   │   │ planting_method │
│ created_at  │   │   │ last_seen   │   │   │ estimated_tco2e │
└─────────────┘   │   └──────┬──────┘   │   │ status          │
                  │          │          │   └────────┬────────┘
                  │          │          │            │
                  │          │          │            │ 1:N
                  │          │          │            ▼
                  │          │          │   ┌─────────────────┐
                  │          │          │   │   seasons       │
                  │          │          │   │─────────────────│
                  │          │          │   │ id (PK)         │
                  │          │          │   │ plot_id (FK)    │──▶ plots
                  │          │          │   │ season_name     │
                  │          │          │   │ seed_date       │
                  │          │          │   │ status          │
                  │          │          │   │ current_awd_    │
                  │          │          │   │   phase         │
                  │          │          │   │ current_water_  │
                  │          │          │   │   level_cm      │
                  │          │          │   │ total_credit_   │
                  │          │          │   │   tco2e         │
                  │          │          │   └────────┬────────┘
                  │          │          │            │
                  │          │          │            │ 1:N
                  │          │          │            ├──────────────────┐
                  │          │          │            ▼                  ▼
                  │          │          │   ┌──────────────┐  ┌──────────────────┐
                  │          │          │   │ awd_cycles   │  │ emissions_daily  │
                  │          │          │   │──────────────│  │──────────────────│
                  │          │          │   │ id (PK)      │  │ id (PK)          │
                  │          │          │   │ plot_id(FK)  │  │ plot_id (FK)     │
                  │          │          │   │ season_id(FK)│  │ season_id (FK)   │
                  │          │          │   │ cycle_number │  │ date             │
                  │          │          │   │ phase        │  │ co2_flux_kg/ha   │
                  │          │          │   │ start_time   │  │ ch4_flux_kg/ha   │
                  │          │          │   │ end_time     │  │ total_tco2e      │
                  │          │          │   │ avg/min/max  │  └──────────────────┘
                  │          │          │   │   water_level│
                  │          │          │   │ status       │
                  │          │          │   └──────────────┘
                  │          │          │
                  │          │          │   ┌────────────────────┐
                  │          │          │   │ growth_milestones  │
                  │          │          │   │────────────────────│
                  │          │          │   │ id (PK)            │
                  │          │          └──▶│ season_id (FK)     │
                  │          │              │ milestone_type     │
                  │          │              │ recorded_date      │
                  │          │              │ water_level_cm     │
                  │          │              └────────────────────┘
                  │          │
                  │          │          ┌─────────────────┐
                  │          └─────────▶│   telemetry     │
                  │                      │─────────────────│
                  │                      │ id (PK)         │
                  │                      │ device_id (idx) │──▶ devices
                  │                      │ timestamp (idx) │
                  │                      │ lat, lng        │
                  │                      │ co2_ppm         │
                  │                      │ ch4_ppm         │
                  │                      │ water_level_cm  │
                  │                      │ moisture_pct    │
                  │                      │ battery_pct     │
                  │                      │ signal_csq      │
                  │                      │ ...             │
                  │                      └─────────────────┘
                  │
                  │          ┌─────────────────┐
                  └─────────▶│    alerts       │
                             │─────────────────│
                             │ id (PK)         │
                             │ device_id       │──▶ devices
                             │ alert_type      │
                             │ severity        │
                             │ message         │
                             │ is_read         │
                             │ resolved_at     │
                             └─────────────────┘
```

---

## 6. Telemetry Ingestion Pipeline (Detail)

```
                    ┌──────────────────────────────────────┐
                    │         Incoming Payload              │
                    │                                      │
                    │  {                                   │
                    │    "device_id": "ACU-LKB-0007",      │
                    │    "firmware_version": "1.2.0",      │
                    │    "timestamp": "2026-07-03T14:30",  │
                    │    "location": {lat, lng, gnss, sat},│
                    │    "gas": {co2, ch4_raw, ch4_ppm},   │
                    │    "water": {level, distance},       │
                    │    "soil": {moisture_raw, pct},      │
                    │    "power": {battery, voltage, ...}, │
                    │    "network": {signal, registered},  │
                    │    "diagnostics": {uptime, errors}   │
                    │  }                                   │
                    └──────────────────┬───────────────────┘
                                       │
                    ┌──────────────────▼───────────────────┐
                    │  Step 1: Validate (Pydantic)         │
                    │  TelemetryIn schema                  │
                    │  → Reject if invalid                 │
                    └──────────────────┬───────────────────┘
                                       │
                    ┌──────────────────▼───────────────────┐
                    │  Step 2: Device Lookup               │
                    │  → Query Device by device_id         │
                    │  → If not found + MOCK_MODE:         │
                    │    auto-register (pilot lat/lng)     │
                    │  → If not found + production:        │
                    │    reject (404 / error)              │
                    └──────────────────┬───────────────────┘
                                       │
                    ┌──────────────────▼───────────────────┐
                    │  Step 3: Calibration                 │
                    │                                      │
                    │  ch4_raw_adc ──▶ calibrate_ch4()     │
                    │    MQ-4 curve: ppm = 1000*(Rs/R0)^-2.5│
                    │                                      │
                    │  moisture_raw ──▶ calibrate_soil()   │
                    │    (3000-dry, 1300-wet) → %          │
                    │                                      │
                    │  raw_distance ──▶ calibrate_water()  │
                    │    level = 16cm - distance           │
                    │                                      │
                    │  battery_volt ──▶ calibrate_battery()│
                    │    (3.0V-4.2V) → 0-100%              │
                    │                                      │
                    │  co2_ppm ──▶ calibrate_co2()         │
                    │    (pass-through, validate)          │
                    └──────────────────┬───────────────────┘
                                       │
                    ┌──────────────────▼───────────────────┐
                    │  Step 4: Write Telemetry Record      │
                    │  → INSERT into telemetry table       │
                    └──────────────────┬───────────────────┘
                                       │
                    ┌──────────────────▼───────────────────┐
                    │  Step 5: Update Device State         │
                    │  → battery, voltage, last_seen       │
                    │  → status = "online"                 │
                    │  → charging, solar, signal           │
                    └──────────────────┬───────────────────┘
                                       │
                    ┌──────────────────▼───────────────────┐
                    │  Step 6: Update Active Season        │
                    │  → Find Plot by device_id            │
                    │  → Find active Season for Plot       │
                    │  → Update current_water_level_cm     │
                    │  → Update co2_ppm, ch4_ppm, moisture │
                    └──────────────────┬───────────────────┘
                                       │
                    ┌──────────────────▼───────────────────┐
                    │  Step 7: Alert Checks                │
                    │  → check_gps_theft (if lat/lng)      │
                    │    Compare with install location     │
                    │    If moved >50m → create alert      │
                    │  → check_battery                     │
                    │    If <20% → create alert            │
                    │  → check_sensor_fault                │
                    │    Check error_flags, null values    │
                    └──────────────────┬───────────────────┘
                                       │
                    ┌──────────────────▼───────────────────┐
                    │  Step 8: Return Result               │
                    │  HTTP: TelemetryResponse (200 OK)    │
                    │  TCP:  {"status":"ok","id":N,...}    │
                    └──────────────────────────────────────┘
```

---

## 7. Frontend Screen Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Frontend (React Mobile App)                   │
│                                                                      │
│  ┌──────┐                                                            │
│  │  S0  │  Splash / Landing                                          │
│  └──┬───┘                                                            │
│     │ goTo('s1')                                                     │
│     ▼                                                                │
│  ┌──────┐    POST /farmers                                           │
│  │  S1  │──────────────────▶ Backend                                 │
│  │ ลงทะเบียน │    (create farmer)                                      │
│  │ เกษตรกร │                                                        │
│  └──┬───┘                                                            │
│     │ goTo('s2')                                                     │
│     ▼                                                                │
│  ┌──────┐                                                            │
│  │  S2  │  ข้อมูลแปลงนา (ชื่อ, ขนาด, พันธุ์, วิธีปลูก, แหล่งน้ำ)          │
│  └──┬───┘  (state only — no API call yet)                            │
│     │ goTo('s3')                                                     │
│     ▼                                                                │
│  ┌──────┐                                                            │
│  │  S3  │  Baseline (ฟาง, ปุ๋ย, เดือนลงเมล็ด)                          │
│  └──┬───┘  (state only)                                              │
│     │ goTo('s4')                                                     │
│     ▼                                                                │
│  ┌──────┐    POST /plots                  ┌──────────────────┐       │
│  │  S4  │──────────────────▶ Backend ────▶│ Plot created     │       │
│  │ สรุป  │    GET /plots/{id}/credit-est   │ + estimated_tco2e│       │
│  │+credit│◀───────────────────────────────│                  │       │
│  └──┬───┘                                  └──────────────────┘       │
│     │ goTo('s5')                                                     │
│     ▼                                                                │
│  ┌──────┐    POST /plots/{id}/confirm     ┌──────────────────┐       │
│  │  S5  │──────────────────▶ Backend ────▶│ Season created   │       │
│  │Success│    (bind device + season)       │ Device bound     │       │
│  │+bind  │                                │ Plot = active    │       │
│  └──┬───┘                                └──────────────────┘       │
│     │ goTo('s6')                                                     │
│     ▼                                                                │
│  ┌──────┐    GET /plots/{id}/dashboard    ┌──────────────────┐       │
│  │  S6  │◀─────────────────── Backend ────│ AWD status       │       │
│  │Dash- │    (real-time sensor data)      │ Gas snapshot     │       │
│  │board │                                │ Soil snapshot    │       │
│  │      │                                │ Charts 7-day     │       │
│  │AWD   │                                │ Alerts           │       │
│  │Gas   │                                │ Carbon credits   │       │
│  │Soil  │                                └──────────────────┘       │
│  └──┬───┘                                                            │
│     │ goTo('s7') / goTo('s8')                                        │
│     ▼                                                                │
│  ┌──────┐    GET/POST /seasons/{id}/milestones                       │
│  │  S7  │──────────────────▶ Backend                                 │
│  │ระยะ  │    (record growth milestone)                               │
│  │เจริญ │                                                            │
│  │เติบโต│                                                            │
│  └──────┘                                                            │
│                                                                      │
│  ┌──────┐    GET /plots/{id}                                         │
│  │  S8  │◀─────────────────── Backend                                │
│  │แปลง  │    (plot details)                                          │
│  │ของฉัน│                                                            │
│  └──────┘                                                            │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 8. Background Jobs Timeline

```
Time ─────────────────────────────────────────────────────────────────▶

  ┌─────────────┐
  │  Telemetry  │  Every 15-30 min (real) / 8 sec (simulator)
  │  Ingestion  │  HTTP POST or TCP NDJSON
  └──────┬──────┘
         │
         ▼  (writes to telemetry table)
         │
         │  ┌────────────────┐
         ├─▶│ AWD Detection  │  Every 15 minutes (APScheduler)
         │  └───────┬────────┘
         │          │  (analyzes water_level_cm → wetting/drying)
         │          ▼
         │    awd_cycles table
         │
         │  ┌────────────────┐
         ├─▶│ Emission Calc  │  Daily at 00:05 (APScheduler)
         │  └───────┬────────┘
         │          │  (flux → tCO₂e per day)
         │          ▼
         │    emissions_daily table
         │
         │  ┌────────────────┐
         ├─▶│ Offline Check  │  Every 10 minutes (APScheduler)
         │  └───────┬────────┘
         │          │  (if last_seen > threshold → alert)
         │          ▼
         │    alerts table
         │
         │  ┌────────────────┐
         └─▶│ Alert Checks   │  Every telemetry record (inline)
            └───────┬────────┘
                    │  • GPS theft (>50m)
                    │  • Low battery (<20%)
                    │  • Sensor fault
                    ▼
              alerts table
```

---

## 9. Configuration & Ports

```
┌─────────────────────────────────────────────────────────────┐
│                    Port Allocation                           │
├─────────────────────────────────────────────────────────────┤
│  Port 8000  → HTTP REST API (FastAPI/Uvicorn)               │
│              • Frontend calls /api/v1/*                     │
│              • HTTP telemetry ingest                        │
│              • Swagger docs at /docs                        │
├─────────────────────────────────────────────────────────────┤
│  Port 9600  → TCP Sensor Ingestion (asyncio)                │
│              • NDJSON protocol (newline-delimited JSON)     │
│              • ACU devices send sensor data                 │
│              • Streaming connections (multiple per client)  │
├─────────────────────────────────────────────────────────────┤
│  Port 5173  → Frontend Dev Server (Vite)                    │
│              • React app served to browser                  │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                    Environment Variables                     │
├─────────────────────────────────────────────────────────────┤
│  # Core                                                     │
│  DATABASE_URL=sqlite:///./agricarbon.db                     │
│  DEVICE_API_KEY=mock-device-api-key-2026                    │
│  MOCK_MODE=true                                             │
│                                                             │
│  # TCP Server                                               │
│  TCP_ENABLED=true                                           │
│  TCP_HOST=0.0.0.0                                           │
│  TCP_PORT=9600                                              │
│                                                             │
│  # Pilot Site                                               │
│  PILOT_LAT=13.7278                                          │
│  PILOT_LNG=100.7773                                         │
│                                                             │
│  # Thresholds                                               │
│  AWD_WET_THRESHOLD_CM=5.0                                   │
│  AWD_DRY_THRESHOLD_CM=-5.0                                  │
│  THEFT_DISTANCE_THRESHOLD_M=50.0                            │
│  BATTERY_LOW_THRESHOLD_PCT=20.0                             │
└─────────────────────────────────────────────────────────────┘
```

---

## 10. How to Run

```bash
# 1. Start Backend (HTTP + TCP both start automatically)
npm run dev:backend
# → HTTP on port 8000
# → TCP on port 9600

# 2. Start Frontend
npm run dev:frontend
# → Vite dev server on port 5173

# 3. Test TCP sensor data
cd backend
.venv\Scripts\python.exe mock_data\send_tcp.py --device ACU-LKB-0007
# → Sends 9 records via TCP NDJSON

# 4. Test TCP live mode
.venv\Scripts\python.exe mock_data\send_tcp.py --live --interval 3
# → Continuous sensor simulation via TCP

# 5. Test HTTP (existing)
.venv\Scripts\python.exe mock_data\send_mock.py
# → Sends 9 records via HTTP POST

# 6. Verify data
curl http://localhost:8000/api/v1/devices/ACU-LKB-0007/telemetry/latest
```
