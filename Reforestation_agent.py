"""
ReforestAI v5.2 — Full-tier single-file platform.

Changes from v5.1:
  • Modern lifespan handler (replaces deprecated @app.on_event)
  • Guarded short-name genus matching (prevents 1-2 char false positives)
  • Reused reverse_engine singleton (perf)
  • HTML-escaped autocomplete (XSS safety)
  • Removed unused imports (os, sys) + dead drawMode variable
  • Explicit temp min/max fallback

Run:  pip install fastapi uvicorn httpx
      python app.py                 (web UI)
      python app.py --cli --country India --place "Tamil Nadu" --species Teak
Open: http://localhost:8000
"""

from __future__ import annotations
import argparse, asyncio, csv, io, json, math, re, sqlite3, time
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse, Response
from pydantic import BaseModel

DB_PATH = Path("reforestai.db")
CACHE_TTL = 86400 * 7

# ==================================================================
# 1. SPECIES CATALOG
# ==================================================================
SPECIES: Dict[str, Dict[str, Any]] = {
    "Acacia mangium":       {"temp": (20, 32), "humidity": (60, 90), "precip": (1000, 4500), "enso": 0.70, "heat": 40, "biome": "tropical", "carbon_kg_yr": 22, "frost_seedling": 2, "succession": "pioneer"},
    "Quercus robur":        {"temp": (7, 22),  "humidity": (50, 85), "precip": (500, 1500),  "enso": 0.40, "heat": 20, "biome": "temperate", "carbon_kg_yr": 18, "frost_seedling": -5, "succession": "climax"},
    "Pinus sylvestris":     {"temp": (2, 20),  "humidity": (40, 80), "precip": (300, 1200),  "enso": 0.30, "heat": 15, "biome": "boreal-temperate", "carbon_kg_yr": 15, "frost_seedling": -8, "succession": "mid"},
    "Eucalyptus grandis":   {"temp": (15, 30), "humidity": (50, 85), "precip": (700, 2500),  "enso": 0.60, "heat": 35, "biome": "subtropical", "carbon_kg_yr": 28, "frost_seedling": 0, "succession": "pioneer"},
    "Tectona grandis":      {"temp": (20, 35), "humidity": (60, 95), "precip": (1200, 3500), "enso": 0.50, "heat": 30, "biome": "monsoon", "carbon_kg_yr": 20, "frost_seedling": 3, "succession": "climax"},
    "Gmelina arborea":      {"temp": (18, 34), "humidity": (55, 90), "precip": (750, 3000),  "enso": 0.60, "heat": 35, "biome": "tropical", "carbon_kg_yr": 24, "frost_seedling": 2, "succession": "pioneer"},
    "Albizia lebbeck":      {"temp": (15, 35), "humidity": (40, 85), "precip": (500, 2000),  "enso": 0.70, "heat": 45, "biome": "dry-tropical", "carbon_kg_yr": 18, "frost_seedling": -2, "succession": "pioneer"},
    "Prosopis cineraria":   {"temp": (20, 40), "humidity": (25, 70), "precip": (200, 800),   "enso": 0.80, "heat": 55, "biome": "arid", "carbon_kg_yr": 10, "frost_seedling": 2, "succession": "pioneer"},
    "Betula pendula":       {"temp": (0, 18),  "humidity": (45, 85), "precip": (400, 1200),  "enso": 0.20, "heat": 10, "biome": "boreal", "carbon_kg_yr": 12, "frost_seedling": -10, "succession": "pioneer"},
    "Shorea robusta":       {"temp": (18, 34), "humidity": (60, 95), "precip": (1000, 3000), "enso": 0.50, "heat": 25, "biome": "monsoon", "carbon_kg_yr": 19, "frost_seedling": 3, "succession": "climax"},
    "Nothofagus obliqua":   {"temp": (8, 20),  "humidity": (55, 85), "precip": (800, 2500),  "enso": 0.40, "heat": 15, "biome": "temperate", "carbon_kg_yr": 14, "frost_seedling": -5, "succession": "climax"},
    "Robinia pseudoacacia": {"temp": (5, 28),  "humidity": (35, 80), "precip": (400, 1500),  "enso": 0.60, "heat": 35, "biome": "temperate", "carbon_kg_yr": 16, "frost_seedling": -5, "succession": "pioneer"},
    "Dalbergia sissoo":     {"temp": (15, 38), "humidity": (35, 85), "precip": (400, 2000),  "enso": 0.70, "heat": 48, "biome": "dry-tropical", "carbon_kg_yr": 17, "frost_seedling": 0, "succession": "mid"},
    "Azadirachta indica":   {"temp": (18, 40), "humidity": (25, 85), "precip": (250, 1800),  "enso": 0.80, "heat": 55, "biome": "semi-arid", "carbon_kg_yr": 12, "frost_seedling": 3, "succession": "mid"},
    "Picea abies":          {"temp": (-2, 18), "humidity": (50, 90), "precip": (400, 1400),  "enso": 0.20, "heat": 8,  "biome": "boreal", "carbon_kg_yr": 13, "frost_seedling": -12, "succession": "climax"},
    "Cedrus deodara":       {"temp": (5, 25),  "humidity": (40, 80), "precip": (600, 2000),  "enso": 0.40, "heat": 20, "biome": "montane", "carbon_kg_yr": 16, "frost_seedling": -6, "succession": "mid"},
    "Mangifera indica":     {"temp": (22, 38), "humidity": (50, 90), "precip": (800, 3000),  "enso": 0.60, "heat": 40, "biome": "tropical", "carbon_kg_yr": 15, "frost_seedling": 5, "succession": "mid"},
    "Coffea arabica":       {"temp": (15, 24), "humidity": (60, 90), "precip": (1000, 2500), "enso": 0.50, "heat": 20, "biome": "highland-tropical", "carbon_kg_yr": 8, "frost_seedling": 3, "succession": "mid"},
}

GENUS_KEYWORDS: Dict[str, Dict[str, Any]] = {
    "pine": {"temp": (-10, 30), "humidity": (30, 85), "precip": (300, 2000), "enso": 0.40, "heat": 25, "biome": "conifer", "carbon_kg_yr": 15, "frost_seedling": -8, "succession": "mid"},
    "pinus": {"temp": (-10, 30), "humidity": (30, 85), "precip": (300, 2000), "enso": 0.40, "heat": 25, "biome": "conifer", "carbon_kg_yr": 15, "frost_seedling": -8, "succession": "mid"},
    "spruce": {"temp": (-15, 20), "humidity": (45, 90), "precip": (400, 1500), "enso": 0.20, "heat": 12, "biome": "boreal-conifer", "carbon_kg_yr": 13, "frost_seedling": -12, "succession": "climax"},
    "oak": {"temp": (-5, 32), "humidity": (40, 85), "precip": (400, 2000), "enso": 0.40, "heat": 30, "biome": "temperate", "carbon_kg_yr": 18, "frost_seedling": -5, "succession": "climax"},
    "quercus": {"temp": (-5, 32), "humidity": (40, 85), "precip": (400, 2000), "enso": 0.40, "heat": 30, "biome": "temperate", "carbon_kg_yr": 18, "frost_seedling": -5, "succession": "climax"},
    "maple": {"temp": (-5, 25), "humidity": (45, 85), "precip": (500, 2000), "enso": 0.30, "heat": 25, "biome": "temperate", "carbon_kg_yr": 14, "frost_seedling": -8, "succession": "mid"},
    "birch": {"temp": (-10, 22), "humidity": (45, 85), "precip": (400, 1400), "enso": 0.20, "heat": 15, "biome": "boreal", "carbon_kg_yr": 12, "frost_seedling": -10, "succession": "pioneer"},
    "acacia": {"temp": (15, 40), "humidity": (25, 80), "precip": (200, 2000), "enso": 0.70, "heat": 50, "biome": "dry-tropical", "carbon_kg_yr": 20, "frost_seedling": 2, "succession": "pioneer"},
    "mangrove": {"temp": (20, 35), "humidity": (65, 95), "precip": (1000, 4000), "enso": 0.60, "heat": 35, "biome": "coastal-tropical", "carbon_kg_yr": 25, "frost_seedling": 8, "succession": "climax"},
    "bamboo": {"temp": (10, 35), "humidity": (50, 95), "precip": (800, 3500), "enso": 0.50, "heat": 35, "biome": "tropical-subtropical", "carbon_kg_yr": 30, "frost_seedling": -2, "succession": "pioneer"},
    "palm": {"temp": (18, 38), "humidity": (55, 95), "precip": (800, 4000), "enso": 0.60, "heat": 40, "biome": "tropical", "carbon_kg_yr": 12, "frost_seedling": 5, "succession": "mid"},
    "coconut": {"temp": (22, 35), "humidity": (65, 95), "precip": (1000, 4000), "enso": 0.55, "heat": 35, "biome": "coastal-tropical", "carbon_kg_yr": 10, "frost_seedling": 8, "succession": "mid"},
    "mango": {"temp": (22, 38), "humidity": (50, 90), "precip": (800, 3000), "enso": 0.60, "heat": 40, "biome": "tropical", "carbon_kg_yr": 15, "frost_seedling": 5, "succession": "mid"},
    "coffee": {"temp": (15, 24), "humidity": (60, 90), "precip": (1000, 2500), "enso": 0.50, "heat": 20, "biome": "highland-tropical", "carbon_kg_yr": 8, "frost_seedling": 3, "succession": "mid"},
    "teak": {"temp": (20, 35), "humidity": (60, 95), "precip": (1200, 3500), "enso": 0.50, "heat": 30, "biome": "monsoon", "carbon_kg_yr": 20, "frost_seedling": 3, "succession": "climax"},
    "neem": {"temp": (18, 40), "humidity": (25, 85), "precip": (250, 1800), "enso": 0.80, "heat": 55, "biome": "semi-arid", "carbon_kg_yr": 12, "frost_seedling": 3, "succession": "mid"},
    "eucalyptus": {"temp": (5, 35), "humidity": (35, 85), "precip": (400, 2500), "enso": 0.60, "heat": 40, "biome": "australian", "carbon_kg_yr": 28, "frost_seedling": 0, "succession": "pioneer"},
    "sandalwood": {"temp": (18, 35), "humidity": (35, 85), "precip": (500, 2000), "enso": 0.60, "heat": 40, "biome": "dry-tropical", "carbon_kg_yr": 10, "frost_seedling": 3, "succession": "climax"},
    "prosopis": {"temp": (20, 40), "humidity": (25, 70), "precip": (200, 800), "enso": 0.80, "heat": 55, "biome": "arid", "carbon_kg_yr": 10, "frost_seedling": 2, "succession": "pioneer"},
    "date": {"temp": (18, 42), "humidity": (15, 60), "precip": (50, 500), "enso": 0.85, "heat": 60, "biome": "arid", "carbon_kg_yr": 12, "frost_seedling": 5, "succession": "mid"},
    "olive": {"temp": (8, 32), "humidity": (30, 75), "precip": (300, 1000), "enso": 0.60, "heat": 40, "biome": "mediterranean", "carbon_kg_yr": 12, "frost_seedling": -5, "succession": "climax"},
    "citrus": {"temp": (10, 35), "humidity": (40, 85), "precip": (600, 2000), "enso": 0.50, "heat": 40, "biome": "subtropical", "carbon_kg_yr": 12, "frost_seedling": -2, "succession": "mid"},
    "apple": {"temp": (-10, 28), "humidity": (40, 85), "precip": (500, 1800), "enso": 0.40, "heat": 30, "biome": "temperate", "carbon_kg_yr": 13, "frost_seedling": -8, "succession": "mid"},
    "avocado": {"temp": (12, 30), "humidity": (50, 90), "precip": (800, 2500), "enso": 0.50, "heat": 32, "biome": "highland-tropical", "carbon_kg_yr": 14, "frost_seedling": 2, "succession": "mid"},
    "guava": {"temp": (15, 38), "humidity": (40, 90), "precip": (500, 2500), "enso": 0.60, "heat": 42, "biome": "tropical-subtropical", "carbon_kg_yr": 11, "frost_seedling": 0, "succession": "pioneer"},
}

BIOME_KEYWORDS: Dict[str, Dict[str, Any]] = {
    "desert": {"temp": (18, 42), "humidity": (15, 60), "precip": (50, 500), "enso": 0.85, "heat": 60, "biome": "arid", "carbon_kg_yr": 8, "frost_seedling": 2, "succession": "pioneer"},
    "arid": {"temp": (18, 42), "humidity": (15, 65), "precip": (100, 700), "enso": 0.80, "heat": 55, "biome": "arid", "carbon_kg_yr": 9, "frost_seedling": 2, "succession": "pioneer"},
    "alpine": {"temp": (-15, 12), "humidity": (40, 85), "precip": (300, 1500), "enso": 0.25, "heat": 5, "biome": "alpine", "carbon_kg_yr": 6, "frost_seedling": -15, "succession": "climax"},
    "boreal": {"temp": (-20, 18), "humidity": (45, 90), "precip": (300, 1200), "enso": 0.20, "heat": 10, "biome": "boreal", "carbon_kg_yr": 12, "frost_seedling": -12, "succession": "climax"},
    "mangrove": {"temp": (20, 35), "humidity": (65, 95), "precip": (1000, 4000), "enso": 0.60, "heat": 35, "biome": "coastal-tropical", "carbon_kg_yr": 25, "frost_seedling": 8, "succession": "climax"},
    "riparian": {"temp": (0, 35), "humidity": (50, 95), "precip": (500, 3000), "enso": 0.55, "heat": 35, "biome": "riparian", "carbon_kg_yr": 16, "frost_seedling": -5, "succession": "pioneer"},
    "tropical": {"temp": (20, 35), "humidity": (60, 95), "precip": (1000, 4000), "enso": 0.60, "heat": 35, "biome": "tropical", "carbon_kg_yr": 22, "frost_seedling": 5, "succession": "climax"},
    "temperate": {"temp": (-10, 28), "humidity": (40, 85), "precip": (400, 1800), "enso": 0.40, "heat": 30, "biome": "temperate", "carbon_kg_yr": 15, "frost_seedling": -5, "succession": "mid"},
    "mediterranean": {"temp": (8, 32), "humidity": (30, 75), "precip": (300, 1000), "enso": 0.55, "heat": 40, "biome": "mediterranean", "carbon_kg_yr": 12, "frost_seedling": -3, "succession": "climax"},
    "montane": {"temp": (-5, 25), "humidity": (40, 85), "precip": (600, 2500), "enso": 0.40, "heat": 22, "biome": "montane", "carbon_kg_yr": 14, "frost_seedling": -8, "succession": "mid"},
    "coastal": {"temp": (5, 32), "humidity": (55, 95), "precip": (400, 2500), "enso": 0.45, "heat": 32, "biome": "coastal", "carbon_kg_yr": 14, "frost_seedling": -2, "succession": "mid"},
}

GENERIC_FALLBACK = {
    "temp": (5, 32), "humidity": (35, 90), "precip": (400, 2500),
    "enso": 0.60, "heat": 40, "biome": "generalist (inferred)",
    "carbon_kg_yr": 15, "frost_seedling": -2, "succession": "mid",
}

def infer_tolerance(name: str) -> Tuple[Dict[str, Any], str, bool]:
    """FIX: guard short-name matching to prevent 1-2 char false positives."""
    n = name.strip().lower()
    if not n:
        return GENERIC_FALLBACK, "empty name — using generic", True
    for k, v in SPECIES.items():
        if k.lower() == n:
            return v, "exact catalog match", False
    for k, v in SPECIES.items():
        kl = k.lower(); genus = kl.split()[0]
        if genus and len(genus) >= 3 and genus in n:
            return v, f"matched catalog genus '{genus}'", True
        if len(n) >= 3 and (kl in n or n in kl):
            return v, f"matched catalog entry '{k}'", True
    for kw, tol in GENUS_KEYWORDS.items():
        if len(kw) >= 3 and re.search(rf"\b{re.escape(kw)}", n):
            return tol, f"inferred from genus keyword '{kw}'", True
    for kw, tol in BIOME_KEYWORDS.items():
        if len(kw) >= 3 and kw in n:
            return tol, f"inferred from biome keyword '{kw}'", True
    return GENERIC_FALLBACK, "no match — using generalist tolerance", True

# ==================================================================
# 2. SQLITE CACHE
# ==================================================================
class LocalCache:
    def __init__(self, path: Path):
        self.conn = sqlite3.connect(str(path), check_same_thread=False)
        self.conn.execute("""CREATE TABLE IF NOT EXISTS cache (
            key TEXT PRIMARY KEY, value TEXT, ts REAL)""")
        self.conn.execute("""CREATE TABLE IF NOT EXISTS community (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lat REAL, lon REAL, species TEXT, status TEXT, created REAL)""")
        self.conn.commit()
    def get(self, key: str) -> Optional[Any]:
        row = self.conn.execute("SELECT value, ts FROM cache WHERE key=?", (key,)).fetchone()
        if not row: return None
        val, ts = row
        if time.time() - ts > CACHE_TTL: return None
        try: return json.loads(val)
        except Exception: return None
    def put(self, key: str, value: Any):
        self.conn.execute("INSERT OR REPLACE INTO cache (key, value, ts) VALUES (?,?,?)",
                          (key, json.dumps(value), time.time()))
        self.conn.commit()
    def add_community(self, lat, lon, species, status):
        self.conn.execute("INSERT INTO community (lat, lon, species, status, created) VALUES (?,?,?,?,?)",
                          (lat, lon, species, status, time.time()))
        self.conn.commit()
    def nearby_community(self, lat, lon, km=25):
        d = km / 111.0
        rows = self.conn.execute(
            "SELECT lat, lon, species, status FROM community "
            "WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ? LIMIT 50",
            (lat - d, lat + d, lon - d, lon + d)).fetchall()
        return [{"lat": r[0], "lon": r[1], "species": r[2], "status": r[3]} for r in rows]

cache = LocalCache(DB_PATH)

# ==================================================================
# 3. NOAA ONI
# ==================================================================
ONI_FALLBACK = {"2024-01": 0.8, "2024-02": 0.7, "2024-03": 0.5}

class ONIService:
    ONI_URL = "https://www.cpc.ncep.noaa.gov/data/indices/oni.ascii.txt"
    def __init__(self):
        self._cache = None; self._ts = 0
    async def get(self, year: int, month: int) -> float:
        await self._ensure()
        if self._cache:
            key = f"{year}-{month:02d}"
            if key in self._cache: return self._cache[key]
            nearest = min(self._cache.items(),
                key=lambda kv: abs((int(kv[0][:4]) - year) * 12 + (int(kv[0][5:7]) - month)))
            return nearest[1]
        return 0.5
    async def _ensure(self):
        if self._cache and (time.time() - self._ts) < 86400: return
        try:
            async with httpx.AsyncClient(timeout=15) as c:
                r = await c.get(self.ONI_URL); r.raise_for_status()
            self._cache = self._parse(r.text); self._ts = time.time()
        except Exception:
            if not self._cache: self._cache = ONI_FALLBACK
    def _parse(self, text):
        out = {}
        m = {"DJF":1,"JFM":2,"FMA":3,"MAM":4,"AMJ":5,"MJJ":6,
             "JJA":7,"JAS":8,"ASO":9,"SON":10,"OND":11,"NDJ":12}
        for line in text.splitlines()[1:]:
            p = line.split()
            if len(p) < 4: continue
            try: out[f"{int(p[1])}-{m.get(p[0].upper(),1):02d}"] = float(p[3])
            except Exception: continue
        return out or ONI_FALLBACK

# ==================================================================
# 4. CLIMATE SERVICE
# ==================================================================
class ClimateService:
    ARCHIVE="https://archive-api.open-meteo.com/v1/archive"
    FORECAST="https://api.open-meteo.com/v1/forecast"
    GEOCODE="https://geocoding-api.open-meteo.com/v1/search"
    NOMINATIM="https://nominatim.openstreetmap.org/search"
    NASA="https://power.larc.nasa.gov/api/temporal/daily/point"
    GBIF="https://api.gbif.org/v1/species/suggest"
    GBIF_OCC="https://api.gbif.org/v1/occurrence/search"

    def __init__(self, oni: ONIService):
        self.oni = oni
        self._client: Optional[httpx.AsyncClient] = None
        self._climate_cache: Dict[tuple, Dict] = {}
        self._geocode_cache: Dict[str, List[Dict]] = {}
        self._trend_cache: Dict[tuple, Dict] = {}

    async def _c(self):
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=60.0,
                headers={"User-Agent": "ReforestAI/5.2 (reforestation-app)"},
                follow_redirects=True)
        return self._client
    async def close(self):
        if self._client: await self._client.aclose()

    async def _get(self, url, params, attempts=3):
        c = await self._c(); last="unknown"
        for i in range(attempts):
            try:
                r = await c.get(url, params=params)
                if r.status_code == 200: return r
                last = f"HTTP {r.status_code}: {r.text[:120]}"
            except Exception as e:
                last = f"{type(e).__name__}: {e}"
            await asyncio.sleep(0.5 * (2 ** i))
        raise RuntimeError(f"all {attempts} attempts failed: {last}")

    async def geocode(self, place, country="", count=6):
        key = f"{place}|{country}".lower()
        if key in self._geocode_cache: return self._geocode_cache[key]
        results = await self._om_geocode(place, country, count)
        if not results:
            for v in self._variants(place):
                if v.lower() == place.lower(): continue
                results = await self._om_geocode(v, country, count)
                if results: break
        if not results and country:
            results = await self._om_geocode(place, "", count)
        if not results:
            results = await self._nominatim(place, country, count)
        self._geocode_cache[key] = results
        return results

    def _variants(self, place):
        p = place.strip()
        yield p.title(); yield p.replace("-", " ").title()
        if " " not in p and len(p) >= 6:
            for i in range(4, min(len(p) - 2, 12)):
                yield (p[:i] + " " + p[i:]).title()

    async def _om_geocode(self, place, country, count):
        if not place.strip(): return []
        try:
            r = await self._get(self.GEOCODE, {"name": place, "count": count,
                "language": "en", "format": "json"}, attempts=2)
            data = r.json().get("results", []) or []
        except Exception: return []
        if country:
            cl = country.lower()
            f = [x for x in data if cl in (x.get("country") or "").lower()
                 or cl in (x.get("admin1") or "").lower()]
            if f: data = f
        return [{"name": d.get("name"), "country": d.get("country"),
                 "admin1": d.get("admin1"), "latitude": d.get("latitude"),
                 "longitude": d.get("longitude"), "elevation": d.get("elevation"),
                 "population": d.get("population"), "source": "open-meteo"}
                for d in data if d.get("latitude") is not None]

    async def _nominatim(self, place, country, count):
        q = f"{place}, {country}".strip(", ") if country else place
        try:
            r = await self._get(self.NOMINATIM,
                {"q": q, "format": "json", "limit": count, "addressdetails": 1}, attempts=2)
            data = r.json()
        except Exception: return []
        out = []
        for d in data:
            try: lat = float(d["lat"]); lon = float(d["lon"])
            except Exception: continue
            addr = d.get("address", {}) or {}
            out.append({"name": d.get("name") or d.get("display_name","").split(",")[0],
                        "country": addr.get("country",""),
                        "admin1": addr.get("state") or addr.get("region",""),
                        "latitude": lat, "longitude": lon,
                        "elevation": None, "population": None, "source": "nominatim"})
        return out

    async def gbif_suggest(self, q, limit=10):
        if not q.strip() or len(q) < 2: return []
        try:
            r = await self._get(self.GBIF, {"q": q, "limit": limit, "rank": "SPECIES"}, attempts=2)
            data = r.json()
        except Exception: return []
        return [{"scientificName": d.get("scientificName"),
                 "canonicalName": d.get("canonicalName"),
                 "genus": d.get("genus"), "family": d.get("family"),
                 "rank": d.get("rank"), "status": d.get("status")} for d in data]

    async def gbif_occurrence_count(self, species, country_code):
        try:
            r = await self._get(self.GBIF_OCC,
                {"scientificName": species, "country": country_code, "limit": 0}, attempts=2)
            return int(r.json().get("count", 0))
        except Exception: return 0

    async def climate(self, lat, lon, year=None):
        key = (round(lat, 3), round(lon, 3))
        if key in self._climate_cache: return self._climate_cache[key]
        cached = cache.get(f"clim:{key}")
        if cached:
            self._climate_cache[key] = cached; return cached
        years = [year] if year else [2023, 2022, 2021]
        errors = []
        for y in years:
            try:
                out = await self._openmeteo_climate(lat, lon, y)
                self._climate_cache[key] = out; cache.put(f"clim:{key}", out); return out
            except Exception as e: errors.append(f"OpenMeteo/{y}: {e}")
        for y in years:
            try:
                out = await self._nasa_power_climate(lat, lon, y)
                self._climate_cache[key] = out; cache.put(f"clim:{key}", out); return out
            except Exception as e: errors.append(f"NASA/{y}: {e}")
        raise HTTPException(502, "All climate sources failed. " + " | ".join(errors[-3:]))

    async def _openmeteo_climate(self, lat, lon, year):
        urls = [
            (self.ARCHIVE, {"latitude": lat, "longitude": lon,
                "start_date": f"{year}-01-01", "end_date": f"{year}-12-31",
                "daily": "temperature_2m_mean,temperature_2m_max,temperature_2m_min,precipitation_sum",
                "hourly": "relative_humidity_2m", "timezone": "UTC"}),
            (self.FORECAST, {"latitude": lat, "longitude": lon,
                "past_days": 92, "forecast_days": 1,
                "daily": "temperature_2m_mean,temperature_2m_max,temperature_2m_min,precipitation_sum",
                "hourly": "relative_humidity_2m", "timezone": "UTC"}),
        ]
        last = "no attempt"
        for url, params in urls:
            try:
                r = await self._get(url, params, attempts=2); data = r.json()
            except Exception as e: last = str(e); continue
            if isinstance(data, dict) and data.get("error"):
                last = f"API: {data.get('reason', data)}"; continue
            daily = data.get("daily") or {}; hourly = data.get("hourly") or {}
            elevation = data.get("elevation")
            def clean(v): return [x for x in (v or []) if x is not None]
            tm = clean(daily.get("temperature_2m_mean"))
            tx = clean(daily.get("temperature_2m_max"))
            tn = clean(daily.get("temperature_2m_min"))
            pr = clean(daily.get("precipitation_sum"))
            rh = clean(hourly.get("relative_humidity_2m"))
            if not tm: last = "no daily temperature"; continue
            span = len(tm) or 1
            if span < 300: pr = [p * (365.0 / span) for p in pr]
            host = url.split("/")[2]
            return await self._summarize(lat, lon, year, tm, tx, tn, pr, rh,
                                         elevation=elevation, source=f"open-meteo:{host}")
        raise RuntimeError(last)

    async def _nasa_power_climate(self, lat, lon, year):
        params = {"parameters": "T2M,T2M_MAX,T2M_MIN,RH2M,PRECTOTCORR",
                  "community": "AG", "longitude": lon, "latitude": lat,
                  "start": f"{year}0101", "end": f"{year}1231", "format": "JSON"}
        r = await self._get(self.NASA, params, attempts=2); data = r.json()
        props = (data.get("properties") or {}).get("parameter") or {}
        if not props: raise RuntimeError("NASA POWER: empty")
        elevation = None
        try:
            coords = data.get("geometry", {}).get("coordinates", [])
            if len(coords) >= 3: elevation = float(coords[2])
        except Exception: pass
        def vals(name):
            d = props.get(name) or {}; out = []
            for v in d.values():
                if v is None: continue
                if isinstance(v, (int, float)) and v <= -900: continue
                out.append(float(v))
            return out
        tm=vals("T2M"); tx=vals("T2M_MAX"); tn=vals("T2M_MIN")
        rh=vals("RH2M"); pr_day=vals("PRECTOTCORR")
        pr = [sum(pr_day)] if pr_day else []
        if not tm: raise RuntimeError("NASA POWER: no T2M")
        return await self._summarize(lat, lon, year, tm, tx, tn, pr, rh,
                                     elevation=elevation, source="nasa-power")

    async def _summarize(self, lat, lon, year, tm, tx, tn, pr, rh, elevation, source):
        """FIX: explicit fallback when min/max missing (rather than silent fabrication)."""
        t_mean = sum(tm)/len(tm)
        t_max = max(tx) if tx else t_mean + 10
        t_min = min(tn) if tn else t_mean - 10
        precip = sum(pr) if pr else 0.0
        humid = (sum(rh)/len(rh)) if rh else 65.0
        heat = sum(1 for v in tx if v is not None and v > 35)
        month_means = []
        chunk = max(1, len(tm)//12)
        for i in range(0, min(len(tm), chunk*12), chunk):
            c = tm[i:i+chunk]
            if c: month_means.append(sum(c)/len(c))
        seasonal = (max(month_means) - min(month_means)) if month_means else 0
        month_now = date.today().month
        oni_val = await self.oni.get(year, month_now)
        return {"source_year": year, "source": source,
                "elevation_m": round(float(elevation),1) if elevation is not None else None,
                "temp_annual_mean_c": round(t_mean,2),
                "temp_min_extreme_c": round(t_min,2),
                "temp_max_extreme_c": round(t_max,2),
                "humidity_rel_pct": round(humid,1),
                "annual_precip_mm": round(precip,1),
                "enso_risk_index": round(min(1.0, max(0.0, (oni_val+1)/3)),2),
                "oni_value": round(oni_val,2),
                "heatwave_days_yr": int(heat),
                "seasonal_range_c": round(seasonal,1)}

    async def trend_30y(self, lat, lon):
        key = (round(lat,2), round(lon,2))
        if key in self._trend_cache: return self._trend_cache[key]
        years = list(range(1994, 2024, 3))
        results = []
        for y in years:
            try:
                out = await self._openmeteo_climate(lat, lon, y)
                results.append({"year": y, "temp_c": out["temp_annual_mean_c"],
                                "precip_mm": out["annual_precip_mm"]})
            except Exception: continue
        if len(results) < 4:
            self._trend_cache[key] = {"available": False}; return self._trend_cache[key]
        n = len(results); xs = list(range(n))
        mx = sum(xs)/n; mt = sum(r["temp_c"] for r in results)/n
        mp = sum(r["precip_mm"] for r in results)/n
        nt = sum((xs[i]-mx)*(results[i]["temp_c"]-mt) for i in range(n))
        np_ = sum((xs[i]-mx)*(results[i]["precip_mm"]-mp) for i in range(n))
        den = sum((xs[i]-mx)**2 for i in range(n)) or 1
        ts = nt/den; ps = np_/den
        out = {"available": True, "points": results,
               "temp_slope_c_per_decade": round(ts*3.33,3),
               "precip_slope_mm_per_decade": round(ps*3.33,1),
               "warming": ts > 0.15, "drying": ps < -30,
               "summary": f"🌡 {ts*3.33:+.2f} °C/decade · 💧 {ps*3.33:+.0f} mm/decade"}
        self._trend_cache[key] = out; return out

    async def forecast_7d(self, lat, lon):
        try:
            r = await self._get(self.FORECAST, {"latitude": lat, "longitude": lon,
                "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,relative_humidity_2m_mean,et0_fao_evapotranspiration",
                "forecast_days": 7, "timezone": "auto"}, attempts=2)
            data = r.json()
        except Exception: return {"available": False}
        daily = data.get("daily") or {}
        days = []
        for i, d in enumerate(daily.get("time") or []):
            def g(k):
                arr = daily.get(k) or []
                return arr[i] if i < len(arr) else None
            days.append({"date": d, "t_max": g("temperature_2m_max"),
                         "t_min": g("temperature_2m_min"),
                         "precip": g("precipitation_sum"),
                         "rh": g("relative_humidity_2m_mean"),
                         "et0": g("et0_fao_evapotranspiration")})
        total_rain = sum(d["precip"] or 0 for d in days)
        total_et0 = sum(d["et0"] or 0 for d in days)
        return {"available": True, "days": days,
                "total_rain_mm": round(total_rain,1),
                "total_et0_mm": round(total_et0,1),
                "good_planting_week": total_rain > 10,
                "advice": ("🌧 Good week to plant — natural watering."
                           if total_rain > 10 else
                           "☀ Dry week — irrigate after planting.")}

    async def frost_risk(self, lat, lon, species_frost_seedling):
        try:
            r = await self._get(self.FORECAST, {"latitude": lat, "longitude": lon,
                "daily": "temperature_2m_min", "forecast_days": 14, "timezone": "auto"}, attempts=2)
            data = r.json()
            mins = [v for v in (data.get("daily") or {}).get("temperature_2m_min") or [] if v is not None]
        except Exception:
            mins = []
        frost_days = sum(1 for v in mins if v < 2)
        hard_frost = sum(1 for v in mins if v < 0)
        seedling_risk = "high" if frost_days >= 3 else "moderate" if frost_days >= 1 else "low"
        if mins and min(mins) < species_frost_seedling:
            seedling_risk = "high"
        return {"available": bool(mins),
                "min_next_14d": round(min(mins),1) if mins else None,
                "frost_days_next_14d": frost_days,
                "hard_frost_days": hard_frost,
                "seedling_risk": seedling_risk,
                "species_seedling_tolerance_c": species_frost_seedling}

    async def irrigation_plan(self, lat, lon, climate, tree_count=1, years=2):
        try:
            r = await self._get(self.ARCHIVE, {"latitude": lat, "longitude": lon,
                "start_date": f"{date.today().year-1}-01-01",
                "end_date": f"{date.today().year-1}-12-31",
                "daily": "et0_fao_evapotranspiration,precipitation_sum",
                "timezone": "UTC"}, attempts=2)
            data = r.json()
        except Exception:
            return {"available": False}
        d = data.get("daily") or {}
        et0 = [x for x in (d.get("et0_fao_evapotranspiration") or []) if x is not None]
        pr = [x for x in (d.get("precipitation_sum") or []) if x is not None]
        if not et0: return {"available": False}
        annual_et0 = sum(et0); annual_precip = sum(pr)
        deficit_mm = max(0, annual_et0 - annual_precip)
        usage_factor = 0.3 if years == 1 else 0.7
        annual_l_per_tree = deficit_mm * usage_factor
        weekly_l = annual_l_per_tree / 52
        return {"available": True, "annual_et0_mm": round(annual_et0,1),
                "annual_precip_mm": round(annual_precip,1),
                "deficit_mm": round(deficit_mm,1),
                "liters_per_tree_per_week": round(weekly_l,1),
                "liters_total_weekly": round(weekly_l * tree_count,1),
                "tree_count": tree_count, "establishment_years": years,
                "note": "Irrigate deeply 2×/week in year 1–2 during dry season."}

# ==================================================================
# 5. FOREST / SOIL / NDVI / WDPA
# ==================================================================
class ForestCoverService:
    def __init__(self):
        self._client = None
    async def _c(self):
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=30.0)
        return self._client
    async def close(self):
        if self._client: await self._client.aclose()
    async def get(self, lat, lon):
        try:
            c = await self._c()
            r = await c.get("https://data-api.globalforestwatch.org/dataset/umd_tree_cover_loss/latest/query",
                params={"sql": f"SELECT umd_tree_cover_density_2000__threshold, "
                              f"umd_tree_cover_loss__year FROM data "
                              f"WHERE ST_Intersects(geometry, ST_Point({lon}, {lat})) LIMIT 1"},
                headers={"User-Agent": "ReforestAI/5.2"})
            if r.status_code == 200:
                j = r.json(); data = j.get("data", [])
                if data:
                    row = data[0]
                    cover = row.get("umd_tree_cover_density_2000__threshold", 0)
                    loss = row.get("umd_tree_cover_loss__year")
                    return {"available": True, "tree_cover_pct_2000": cover,
                            "loss_year": loss,
                            "status": ("already_forested" if cover >= 30 and not loss
                                       else "deforested" if loss else "not_forested"),
                            "source": "gfw-public-api"}
        except Exception: pass
        return {"available": False, "status": "unknown", "source": "heuristic"}

class SoilService:
    def __init__(self):
        self._client = None
    async def _c(self):
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=30.0)
        return self._client
    async def close(self):
        if self._client: await self._client.aclose()
    async def get(self, lat, lon, climate):
        try:
            c = await self._c(); delta = 0.01
            r = await c.get("https://maps.isric.org/mapserv", params={
                "map": "/map/phh2o.map", "service": "WMS", "version": "1.1.1",
                "request": "GetFeatureInfo", "layers": "phh2o", "query_layers": "phh2o",
                "srs": "EPSG:4326", "bbox": f"{lon-delta},{lat-delta},{lon+delta},{lat+delta}",
                "width": 101, "height": 101, "x": 50, "y": 50,
                "info_format": "application/json", "format": "image/png"})
            if r.status_code == 200:
                j = r.json(); feats = j.get("features") or []
                if feats:
                    ph = (feats[0].get("properties", {}) or {}).get("GRAY_INDEX")
                    if ph is not None:
                        ph = float(ph)/10.0 if ph > 14 else float(ph)
                        return {"available": True, "ph": round(ph,2),
                                "source": "soilgrids-wms",
                                "drainage": self._drain(ph, climate)}
        except Exception: pass
        p = climate.get("annual_precip_mm", 800); t = climate.get("temp_annual_mean_c", 20)
        if p > 2000 and t > 20: ph, dr = 5.2, "poor"
        elif p > 1200: ph, dr = 6.0, "moderate"
        elif p < 400 and t > 25: ph, dr = 7.8, "excellent"
        else: ph, dr = 6.5, "good"
        return {"available": False, "ph": ph, "drainage": dr, "source": "heuristic"}
    def _drain(self, ph, climate):
        p = climate.get("annual_precip_mm", 800)
        if p > 2000: return "poor"
        if p > 1200: return "moderate"
        if p < 400: return "excellent"
        return "good"

class NDVIService:
    STAC = "https://earth-search.aws.element84.com/v1/search"
    def __init__(self):
        self._client = None
    async def _c(self):
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=30.0)
        return self._client
    async def close(self):
        if self._client: await self._client.aclose()
    async def get(self, lat, lon):
        end = date.today(); start = end - timedelta(days=45)
        try:
            c = await self._c()
            r = await c.post(self.STAC, json={
                "collections": ["sentinel-2-l2a"],
                "intersects": {"type":"Point","coordinates":[lon,lat]},
                "datetime": f"{start.isoformat()}T00:00:00Z/{end.isoformat()}T23:59:59Z",
                "limit": 1, "query": {"eo:cloud_cover": {"lt": 30}}})
            if r.status_code == 200:
                feats = r.json().get("features") or []
                if feats:
                    props = feats[0].get("properties", {})
                    cloud = props.get("eo:cloud_cover", 0)
                    return {"available": True,
                            "scene_date": props.get("datetime","")[:10],
                            "cloud_pct": round(cloud,1),
                            "ndvi_hint": "healthy" if cloud < 15 else "moderate",
                            "source": "sentinel-2-stac"}
        except Exception: pass
        return {"available": False, "source": "stac-unavailable"}

class ProtectedAreaService:
    OSM_OVERPASS = "https://overpass-api.de/api/interpreter"
    def __init__(self):
        self._client = None
    async def _c(self):
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=30.0)
        return self._client
    async def close(self):
        if self._client: await self._client.aclose()
    async def check(self, lat, lon):
        q = f"""
        [out:json][timeout:15];
        (
          node(around:5000,{lat},{lon})["boundary"="protected_area"];
          way(around:5000,{lat},{lon})["boundary"="protected_area"];
          relation(around:5000,{lat},{lon})["boundary"="protected_area"];
          node(around:5000,{lat},{lon})["leisure"="nature_reserve"];
        );
        out tags 1;
        """
        try:
            c = await self._c()
            r = await c.post(self.OSM_OVERPASS, data={"data": q},
                             headers={"User-Agent": "ReforestAI/5.2"})
            if r.status_code == 200:
                j = r.json(); els = j.get("elements") or []
                if els:
                    tags = els[0].get("tags", {})
                    return {"available": True, "inside_protected": True,
                            "name": tags.get("name", "unnamed"),
                            "type": tags.get("protect_class") or tags.get("boundary"),
                            "source": "osm-overpass"}
                return {"available": True, "inside_protected": False,
                        "source": "osm-overpass"}
        except Exception: pass
        return {"available": False, "inside_protected": None, "source": "unavailable"}

# ==================================================================
# 6. NATIVE / INVASIVE
# ==================================================================
class NativeInvasiveService:
    INVASIVE_LIST = {
        "acacia mangium": ["IN","BD","BR","ID","MY","PH","TH","VN","NG","KE"],
        "eucalyptus grandis": ["IN","ZA","BR","PT","ES","KE","NG","UG","ZW"],
        "prosopis juliflora": ["IN","KE","ET","SO","AU","PK","SA"],
        "leucaena leucocephala": ["IN","AU","PH","TH","ID","KE","NG"],
        "robinia pseudoacacia": ["DE","FR","NL","BE","IN","JP","NZ"],
        "ailanthus altissima": ["US","DE","FR","IT","IN","AU"],
        "mikania micrantha": ["IN","CN","ID","PH","TH","BD","NP"],
        "chromolaena odorata": ["IN","TH","VN","ID","PH","NG","GH"],
    }
    NATIVE_HINTS = {
        "tectona grandis": ["IN","MM","TH","LA","ID"],
        "shorea robusta": ["IN","NP","BD","BT"],
        "mangifera indica": ["IN","BD","MM","TH"],
        "azadirachta indica": ["IN","MM","BD","PK","LK"],
        "dalbergia sissoo": ["IN","NP","PK","AF"],
        "quercus robur": ["DE","FR","GB","NL","BE","PL","IT"],
        "pinus sylvestris": ["DE","SE","FI","NO","RU","PL","GB"],
        "picea abies": ["DE","AT","CH","SE","NO","FI","PL"],
        "betula pendula": ["DE","SE","FI","NO","RU","PL"],
        "coffea arabica": ["ET","KE","UG","RW","TZ"],
        "nothofagus obliqua": ["CL","AR"],
    }
    def __init__(self, climate_svc):
        self.climate = climate_svc
    async def check(self, species, country_name):
        sp = species.lower().strip(); country = country_name.strip()
        iso2 = self._iso2(country)
        invasive_for = self.INVASIVE_LIST.get(sp, [])
        native_for = self.NATIVE_HINTS.get(sp, [])
        gbif_count = 0
        if iso2:
            gbif_count = await self.climate.gbif_occurrence_count(species, iso2)
        if iso2 and iso2 in invasive_for: status = "invasive"
        elif iso2 and iso2 in native_for: status = "native"
        elif gbif_count > 500: status = "introduced-widespread"
        elif gbif_count > 50: status = "introduced"
        else: status = "unknown"
        return {"status": status, "iso2": iso2, "gbif_occurrences": gbif_count,
                "source": "gbif+inferred", "advisory": self._advisory(status)}
    def _advisory(self, s):
        return {"native": "✅ Native or long-naturalized — ecologically safe to plant.",
                "introduced": "⚠️ Introduced but not flagged invasive — plant with monitoring.",
                "introduced-widespread": "⚠️ Widely introduced — check local forestry guidance.",
                "invasive": "🚨 Flagged as invasive in this country — DO NOT plant.",
                "unknown": "ℹ️ No native-range data — verify with local forestry office."}[s]
    def _iso2(self, name):
        m = {"india":"IN","kenya":"KE","brazil":"BR","indonesia":"ID","germany":"DE",
             "united states":"US","usa":"US","china":"CN","australia":"AU","nigeria":"NG",
             "ethiopia":"ET","south africa":"ZA","mexico":"MX","peru":"PE","colombia":"CO",
             "vietnam":"VN","thailand":"TH","myanmar":"MM","bangladesh":"BD","pakistan":"PK",
             "sri lanka":"LK","nepal":"NP","philippines":"PH","malaysia":"MY","france":"FR",
             "spain":"ES","italy":"IT","portugal":"PT","netherlands":"NL","belgium":"BE",
             "united kingdom":"GB","uk":"GB","sweden":"SE","norway":"NO","finland":"FI",
             "poland":"PL","russia":"RU","japan":"JP","south korea":"KR","canada":"CA",
             "argentina":"AR","chile":"CL","ghana":"GH","uganda":"UG","tanzania":"TZ",
             "rwanda":"RW","zimbabwe":"ZW","zambia":"ZM","morocco":"MA","egypt":"EG"}
        return m.get(name.lower().strip())

# ==================================================================
# 7. FUTURE CLIMATE
# ==================================================================
class FutureClimateService:
    SSP_DELTAS = {
        2050: {"ssp245": {"dT": 1.4, "dP": -0.03}, "ssp585": {"dT": 1.7, "dP": -0.05}},
        2070: {"ssp245": {"dT": 2.1, "dP": -0.06}, "ssp585": {"dT": 2.9, "dP": -0.10}},
        2090: {"ssp245": {"dT": 2.6, "dP": -0.08}, "ssp585": {"dT": 4.2, "dP": -0.15}},
    }
    async def project(self, climate, horizon=2050, scenario="ssp245"):
        delta = self.SSP_DELTAS.get(horizon, {}).get(scenario)
        if not delta: return {"available": False}
        t = climate["temp_annual_mean_c"] + delta["dT"]
        p = climate["annual_precip_mm"] * (1 + delta["dP"])
        heat = int(climate["heatwave_days_yr"] * (1.0 + delta["dT"] * 0.35))
        humid = max(15.0, climate["humidity_rel_pct"] - delta["dT"] * 1.5)
        projected = {**climate, "temp_annual_mean_c": round(t,2),
                     "annual_precip_mm": round(p,1),
                     "heatwave_days_yr": heat,
                     "humidity_rel_pct": round(humid,1),
                     "horizon": horizon, "scenario": scenario}
        return {"available": True, "projected": projected,
                "warming_c": round(delta["dT"],2),
                "precip_change_pct": round(delta["dP"]*100,1),
                "source": "IPCC-AR6-derived"}

# ==================================================================
# 8. REVERSE + SUCCESSION  (defined BEFORE VerdictEngine — FIX)
# ==================================================================
class ReverseEngine:
    def rank(self, climate, top_n=8):
        scored = []
        for name, tol in SPECIES.items():
            s = 0
            if tol["temp"][0] <= climate["temp_annual_mean_c"] <= tol["temp"][1]: s += 1
            if tol["humidity"][0] <= climate["humidity_rel_pct"] <= tol["humidity"][1]: s += 1
            if tol["precip"][0] <= climate["annual_precip_mm"] <= tol["precip"][1]: s += 1
            if climate["enso_risk_index"] <= tol["enso"]: s += 1
            if climate["heatwave_days_yr"] <= tol["heat"]: s += 1
            pct = round(100 * s / 5)
            if pct >= 60:
                scored.append({"species": name, "score": pct,
                               "biome": tol["biome"],
                               "succession": tol.get("succession", "mid"),
                               "carbon_kg_yr": tol.get("carbon_kg_yr", 15)})
        scored.sort(key=lambda x: -x["score"])
        return scored[:top_n]

class SuccessionPlanner:
    def plan(self, climate):
        ranks = reverse_engine.rank(climate, top_n=20)
        return {"phase_1_pioneers_0_5yr": [r for r in ranks if r["succession"] == "pioneer"][:3],
                "phase_2_midstory_5_15yr": [r for r in ranks if r["succession"] == "mid"][:3],
                "phase_3_climax_15yr_plus": [r for r in ranks if r["succession"] == "climax"][:3],
                "note": "Plant pioneers first for shade + nitrogen. Introduce mid/climax as canopy establishes."}

# Singletons defined early so VerdictEngine can reference them
reverse_engine = ReverseEngine()
succession_planner = SuccessionPlanner()

# ==================================================================
# 9. VERDICT ENGINE
# ==================================================================
class VerdictEngine:
    def evaluate(self, species, climate, tolerance=None, reason="", inferred=False):
        if tolerance is None:
            tolerance, reason, inferred = infer_tolerance(species)
        tol = tolerance
        checks = []; passed = 0
        def ck(label, req, act, ok, good, bad):
            nonlocal passed
            checks.append({"factor": label, "required": req, "actual": act,
                           "pass": ok, "note": good if ok else bad})
            passed += ok
        t_lo, t_hi = tol["temp"]; t = climate["temp_annual_mean_c"]
        ck("Temperature", f"{t_lo}–{t_hi} °C", f"{t} °C",
           t_lo <= t <= t_hi, "Ideal", "Too cold" if t < t_lo else "Too hot")
        h_lo, h_hi = tol["humidity"]; h = climate["humidity_rel_pct"]
        ck("Relative humidity", f"{h_lo}–{h_hi} %", f"{h} %",
           h_lo <= h <= h_hi, "Adequate", "Too dry" if h < h_lo else "Too humid")
        p_lo, p_hi = tol["precip"]; p = climate["annual_precip_mm"]
        ck("Annual rainfall", f"{p_lo}–{p_hi} mm", f"{p} mm",
           p_lo <= p <= p_hi, "Sufficient", "Drought stress" if p < p_lo else "Waterlogging risk")
        heat = climate["heatwave_days_yr"]
        ck("Heatwave exposure", f"≤ {tol['heat']} days/yr >35°C", f"{heat} days/yr",
           heat <= tol["heat"], "Safe", "Heat stress likely")
        enso = climate["enso_risk_index"]
        ck("El Niño risk", f"≤ {tol['enso']}", f"{enso}",
           enso <= tol["enso"], "Stable", "Drought/flood swing risk")
        season = climate["seasonal_range_c"]
        checks.append({"factor": "Seasonality", "required": "≤ 35 °C annual swing",
                       "actual": f"{season} °C", "pass": season <= 35,
                       "note": "Stable" if season <= 35 else "Extreme continental"})
        total = len(checks); score = round(100 * passed / total)
        confidence = round((60 + 0.4 * score) * (0.75 if inferred else 1.0))
        if score >= 90: verdict, vkey = "EXCELLENT", "excellent"
        elif score >= 70: verdict, vkey = "GOOD", "good"
        elif score >= 50: verdict, vkey = "MARGINAL", "marginal"
        else: verdict, vkey = "NOT RECOMMENDED", "bad"
        reasoning = []
        if inferred:
            reasoning.append(f"⚠ Tolerance inferred — {reason}. Treat as indicative.")
        fails = [c for c in checks if not c["pass"]]
        if not fails:
            reasoning.append(f"{species} matches all {total} climate factors.")
            reasoning.append(f"Biome: {tol['biome']}.")
        else:
            for f in fails:
                reasoning.append(f"⚠ {f['factor']}: {f['note']} (need {f['required']}, site {f['actual']}).")
            reasoning.append(f"{passed}/{total} factors satisfied.")
        if climate["heatwave_days_yr"] > 30:
            reasoning.append("Extended heatwaves — irrigation required during establishment.")
        if climate["annual_precip_mm"] > 2500:
            reasoning.append("High rainfall — ensure well-drained soils.")
        alternatives = []
        if vkey in ("marginal","bad"):
            # FIX: reuse singleton instead of new instance
            alternatives = [r["species"] for r in reverse_engine.rank(climate, top_n=5)
                            if r["species"].lower() != species.lower()][:3]
        return {"verdict": verdict, "verdict_key": vkey, "score": score,
                "confidence": confidence, "species": species,
                "species_inferred": inferred, "inference_reason": reason,
                "biome": tol["biome"], "checks": checks, "reasoning": reasoning,
                "alternatives": alternatives, "climate": climate,
                "carbon_kg_yr": tol.get("carbon_kg_yr", 15),
                "frost_seedling_c": tol.get("frost_seedling", -2),
                "succession_role": tol.get("succession", "mid")}

# ==================================================================
# 10. CARBON + EXPORT
# ==================================================================
def carbon_estimate(species, tree_count, years=20):
    tol, _, _ = infer_tolerance(species)
    rate = tol.get("carbon_kg_yr", 15)
    total_kg = rate * years * tree_count * 0.8
    tonnes = total_kg / 1000
    return {"kg_per_tree_per_year": rate, "tree_count": tree_count,
            "years": years, "total_tonnes_co2": round(tonnes, 2),
            "equivalent_km_driven": round(tonnes * 4000, 0),
            "note": "Estimate only — actual depends on soil, management, climate."}

def export_geojson(result):
    pt = {"type": "Feature", "geometry": {"type": "Point",
          "coordinates": [result["lon"], result["lat"]]},
          "properties": {"species": result.get("species"),
                         "verdict": result.get("verdict"),
                         "score": result.get("score"),
                         "confidence": result.get("confidence"),
                         "place": result.get("place"),
                         "country": result.get("country")}}
    return json.dumps({"type": "FeatureCollection", "features": [pt]}, indent=2)

def export_kml(result):
    name = f"{result.get('species','Tree')} @ {result.get('place','')}"
    desc = (f"Verdict: {result.get('verdict')} · Score: {result.get('score')}% · "
            f"Confidence: {result.get('confidence')}%")
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
  <Document>
    <name>ReforestAI site record</name>
    <Placemark>
      <name>{name}</name>
      <description><![CDATA[{desc}]]></description>
      <Point><coordinates>{result['lon']},{result['lat']},0</coordinates></Point>
    </Placemark>
  </Document>
</kml>"""

# ==================================================================
# 11. SERVICE INSTANCES
# ==================================================================
oni_svc = ONIService()
climate_svc = ClimateService(oni_svc)
verdict_engine = VerdictEngine()
forest_svc = ForestCoverService()
soil_svc = SoilService()
ndvi_svc = NDVIService()
protected_svc = ProtectedAreaService()
native_svc = NativeInvasiveService(climate_svc)
future_svc = FutureClimateService()

# ==================================================================
# 12. LIFESPAN + APP  (FIX: modern lifespan instead of @app.on_event)
# ==================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await climate_svc.close(); await forest_svc.close()
    await soil_svc.close(); await ndvi_svc.close(); await protected_svc.close()

app = FastAPI(title="ReforestAI v5.2", lifespan=lifespan)

class GeocodeReq(BaseModel):
    place: str; country: str = ""
class AnalyzeReq(BaseModel):
    lat: float; lon: float; species: str; place: str = ""; country: str = ""
class MicrositeReq(BaseModel):
    lat: float; lon: float; species: str; radius_m: int = 300; grid: int = 5
class InferReq(BaseModel):
    species: str
class AutocompleteReq(BaseModel):
    q: str; limit: int = 10
class FutureReq(BaseModel):
    lat: float; lon: float; species: str; horizon: int = 2050; scenario: str = "ssp245"
class PolygonReq(BaseModel):
    polygon: List[List[float]]
    species: str; spacing_m: float = 4.0; sample_points: int = 9
class CarbonReq(BaseModel):
    species: str; tree_count: int = 100; years: int = 20
class ReverseReq(BaseModel):
    lat: float; lon: float
class CommunityReq(BaseModel):
    lat: float; lon: float; species: str; status: str

@app.get("/api/species")
async def list_species(): return {"species": sorted(SPECIES.keys())}

@app.post("/api/species/autocomplete")
async def species_ac(req: AutocompleteReq):
    return {"suggestions": await climate_svc.gbif_suggest(req.q, req.limit)}

@app.post("/api/species/infer")
async def species_infer(req: InferReq):
    tol, reason, inferred = infer_tolerance(req.species)
    return {"species": req.species, "inferred": inferred, "reason": reason,
            "tolerance": {"temp_c": list(tol["temp"]),
                          "humidity_pct": list(tol["humidity"]),
                          "precip_mm": list(tol["precip"]),
                          "enso_max": tol["enso"], "heatwave_max": tol["heat"],
                          "biome": tol["biome"],
                          "carbon_kg_yr": tol.get("carbon_kg_yr"),
                          "frost_seedling_c": tol.get("frost_seedling"),
                          "succession": tol.get("succession")}}

@app.post("/api/geocode")
async def geocode(req: GeocodeReq):
    if not req.place.strip(): raise HTTPException(400, "Place is required")
    results = await climate_svc.geocode(req.place, req.country)
    if not results:
        raise HTTPException(404, f"No location found for '{req.place}'"
                                 + (f" in {req.country}" if req.country else ""))
    return {"results": results}

@app.post("/api/analyze")
async def analyze(req: AnalyzeReq):
    species = (req.species or "").strip()
    if not species: raise HTTPException(400, "Species is required")
    climate = await climate_svc.climate(req.lat, req.lon)
    tolerance, reason, inferred = infer_tolerance(species)
    verdict = verdict_engine.evaluate(species, climate, tolerance, reason, inferred)
    (trend, forecast, forest, soil, ndvi, protected, native, frost, irrigation,
     future_2050, future_2070, reverse, community) = await asyncio.gather(
        climate_svc.trend_30y(req.lat, req.lon),
        climate_svc.forecast_7d(req.lat, req.lon),
        forest_svc.get(req.lat, req.lon),
        soil_svc.get(req.lat, req.lon, climate),
        ndvi_svc.get(req.lat, req.lon),
        protected_svc.check(req.lat, req.lon),
        native_svc.check(species, req.country),
        climate_svc.frost_risk(req.lat, req.lon, verdict["frost_seedling_c"]),
        climate_svc.irrigation_plan(req.lat, req.lon, climate, tree_count=100),
        future_svc.project(climate, horizon=2050),
        future_svc.project(climate, horizon=2070),
        asyncio.to_thread(reverse_engine.rank, climate, 8),
        asyncio.to_thread(cache.nearby_community, req.lat, req.lon),
        return_exceptions=True)
    def safe(v, default): return v if not isinstance(v, Exception) else default
    try: cache.add_community(req.lat, req.lon, species, verdict["verdict_key"])
    except Exception: pass
    return {"country": req.country or "—",
            "place": req.place or f"{req.lat:.3f}, {req.lon:.3f}",
            "lat": req.lat, "lon": req.lon, **verdict,
            "trend": safe(trend, {"available": False}),
            "forecast": safe(forecast, {"available": False}),
            "forest": safe(forest, {"available": False}),
            "soil": safe(soil, {"available": False}),
            "ndvi": safe(ndvi, {"available": False}),
            "protected": safe(protected, {"available": False}),
            "native_invasive": safe(native, {"status": "unknown"}),
            "frost": safe(frost, {"available": False}),
            "irrigation": safe(irrigation, {"available": False}),
            "future_2050": safe(future_2050, {"available": False}),
            "future_2070": safe(future_2070, {"available": False}),
            "reverse_species": safe(reverse, []),
            "community_nearby": safe(community, [])}

@app.post("/api/future")
async def future_projection(req: FutureReq):
    climate = await climate_svc.climate(req.lat, req.lon)
    proj = await future_svc.project(climate, horizon=req.horizon, scenario=req.scenario)
    if not proj.get("available"): raise HTTPException(400, "Invalid horizon/scenario")
    tol, reason, inferred = infer_tolerance(req.species)
    fv = verdict_engine.evaluate(req.species, proj["projected"], tol, reason, inferred)
    return {**proj, "future_verdict": fv["verdict"], "future_score": fv["score"]}

@app.post("/api/microsite")
async def microsite(req: MicrositeReq):
    species = (req.species or "").strip()
    if not species: raise HTTPException(400, "Species is required")
    grid = max(3, min(7, req.grid)); radius = max(50, min(1000, req.radius_m))
    tol, reason, inferred = infer_tolerance(species)
    half = grid // 2
    lat_step = radius / 111000.0
    lon_step = radius / (111000.0 * max(0.1, math.cos(math.radians(req.lat))))
    pts = [(req.lat + i*lat_step, req.lon + j*lon_step)
           for i in range(-half, half+1) for j in range(-half, half+1)]
    sem = asyncio.Semaphore(6)
    async def one(la, lo):
        async with sem:
            try:
                clim = await climate_svc.climate(la, lo)
                return {"lat": la, "lon": lo, "climate": clim}
            except Exception as e:
                return {"lat": la, "lon": lo, "error": str(e)[:120]}
    results = await asyncio.gather(*(one(la, lo) for la, lo in pts))
    elev = {(round(r["lat"],6), round(r["lon"],6)): float(r["climate"]["elevation_m"])
            for r in results if "climate" in r and r["climate"].get("elevation_m") is not None}
    for r in results:
        if "error" in r or "climate" not in r: continue
        e = r["climate"].get("elevation_m")
        if e is None: r["slope_pct"] = 0.0; continue
        la, lo = r["lat"], r["lon"]; mx = 0.0
        for nla, nlo in [(la+lat_step,lo),(la-lat_step,lo),(la,lo+lon_step),(la,lo-lon_step)]:
            ne = elev.get((round(nla,6), round(nlo,6)))
            if ne is None: continue
            dlat = (nla-la)*111000; dlon = (nlo-lo)*111000*math.cos(math.radians(la))
            d = math.hypot(dlat, dlon)
            if d < 1: continue
            s = abs(ne - float(e))/d*100
            if s > mx: mx = s
        r["slope_pct"] = round(mx,1)
    scored = []
    for r in results:
        if "error" in r or "climate" not in r: continue
        v = verdict_engine.evaluate(species, r["climate"], tol, reason, inferred)
        slope = r.get("slope_pct",0.0)
        pen = min(25.0, slope * (25.0/30.0))
        micro = max(0.0, v["score"] - pen)
        scored.append({"lat": round(r["lat"],6), "lon": round(r["lon"],6),
                       "elevation_m": r["climate"].get("elevation_m"),
                       "slope_pct": slope, "verdict_score": v["score"],
                       "verdict_key": v["verdict_key"], "micro_score": round(micro,1),
                       "climate": r["climate"]})
    scored.sort(key=lambda x: -x["micro_score"])
    return {"center": {"lat": req.lat, "lon": req.lon}, "radius_m": radius, "grid": grid,
            "species": species, "points": scored, "top3": scored[:3]}

@app.post("/api/polygon")
async def polygon_analyze(req: PolygonReq):
    poly = [[round(x, 9), round(y, 9)] for x, y in req.polygon]
    if len(poly) < 3: raise HTTPException(400, "Polygon needs ≥3 points")
    def _area(poly):
        n = len(poly); a = 0.0
        for i in range(n):
            x1, y1 = poly[i]; x2, y2 = poly[(i+1) % n]
            a += x1*y2 - x2*y1
        mid_lat = sum(p[1] for p in poly)/n
        return abs(a/2) * 111000.0 * (111000.0 * math.cos(math.radians(mid_lat)))
    area_ha = _area(poly) / 10000
    xs = [p[0] for p in poly]; ys = [p[1] for p in poly]
    minx, maxx, miny, maxy = min(xs), max(xs), min(ys), max(ys)
    n_side = max(3, int(math.sqrt(req.sample_points)))
    def pip(x, y):
        inside = False; n = len(poly); j = n-1
        for i in range(n):
            xi, yi = poly[i]; xj, yj = poly[j]
            if ((yi > y) != (yj > y)) and (x < (xj-xi)*(y-yi)/(yj-yi+1e-12)+xi):
                inside = not inside
            j = i
        return inside
    samples = []
    for i in range(n_side):
        for j in range(n_side):
            x = minx + (maxx-minx)*(i+0.5)/n_side
            y = miny + (maxy-miny)*(j+0.5)/n_side
            if pip(x, y): samples.append((y, x))
    sem = asyncio.Semaphore(5)
    async def one(la, lo):
        async with sem:
            try:
                clim = await climate_svc.climate(la, lo)
                v = verdict_engine.evaluate(req.species, clim)
                return {"lat": la, "lon": lo, "verdict_key": v["verdict_key"],
                        "score": v["score"], "climate": clim}
            except Exception as e:
                return {"lat": la, "lon": lo, "error": str(e)[:100]}
    results = await asyncio.gather(*(one(la, lo) for la, lo in samples))
    suitable = [r for r in results if r.get("verdict_key") in ("excellent","good")]
    marginal = [r for r in results if r.get("verdict_key") == "marginal"]
    ratio = (len(suitable) + 0.5*len(marginal)) / max(1, len(results))
    plantable_ha = area_ha * ratio
    trees_per_ha = int(10000 / (req.spacing_m ** 2)) if req.spacing_m > 0 else 625
    tree_count = int(plantable_ha * trees_per_ha)
    return {"area_ha": round(area_ha,3), "samples": len(results),
            "suitable_ratio": round(ratio,2), "plantable_ha": round(plantable_ha,3),
            "spacing_m": req.spacing_m, "trees_per_ha": trees_per_ha,
            "estimated_tree_count": tree_count, "sample_results": results}

@app.post("/api/carbon")
async def carbon(req: CarbonReq):
    return carbon_estimate(req.species, req.tree_count, req.years)

@app.post("/api/reverse")
async def reverse(req: ReverseReq):
    climate = await climate_svc.climate(req.lat, req.lon)
    return {"climate": climate,
            "recommended": reverse_engine.rank(climate, top_n=10),
            "succession_plan": succession_planner.plan(climate)}

@app.post("/api/community")
async def community_add(req: CommunityReq):
    cache.add_community(req.lat, req.lon, req.species, req.status)
    return {"ok": True}

@app.post("/api/community/nearby")
async def community_nearby(req: CommunityReq):
    return {"nearby": cache.nearby_community(req.lat, req.lon)}

@app.post("/api/export/csv")
async def export_csv(payload: Dict[str, Any]):
    buf = io.StringIO(); w = csv.writer(buf)
    w.writerow(["country","place","lat","lon","species","verdict","score",
                "confidence","native_status","protected","carbon_tCO2_20y"])
    for r in payload.get("results", []):
        if "error" in r: continue
        w.writerow([r.get("country"), r.get("place"), r.get("lat"), r.get("lon"),
                    r.get("species"), r.get("verdict"), r.get("score"),
                    r.get("confidence"),
                    (r.get("native_invasive") or {}).get("status",""),
                    (r.get("protected") or {}).get("inside_protected",""),
                    carbon_estimate(r.get("species",""), 100)["total_tonnes_co2"]])
    buf.seek(0)
    return StreamingResponse(iter([buf.read()]), media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=reforestai_v52.csv"})

@app.post("/api/export/geojson")
async def export_geojson_ep(result: Dict):
    return Response(export_geojson(result), media_type="application/geo+json",
        headers={"Content-Disposition": "attachment; filename=site.geojson"})

@app.post("/api/export/kml")
async def export_kml_ep(result: Dict):
    return Response(export_kml(result), media_type="application/vnd.google-earth.kml+xml",
        headers={"Content-Disposition": "attachment; filename=site.kml"})

@app.post("/api/export/whatsapp")
async def export_whatsapp(result: Dict):
    emoji_pattern = re.compile(
        "[" "\U0001F300-\U0001F9FF" "\U0001F600-\U0001F64F"
        "\U0001F680-\U0001F6FF" "\u2600-\u27BF" "]+", flags=re.UNICODE)
    def clean(s): return emoji_pattern.sub('', str(s or '')).strip()
    species = clean(result.get('species', ''))
    place = clean(result.get('place', ''))
    country = clean(result.get('country', ''))
    verdict = clean(result.get('verdict', ''))
    score = result.get('score', '?')
    c = result.get('climate', {}) or {}
    txt = (f"ReforestAI verdict\n"
           f"Place: {place}, {country}\n"
           f"Species: {species}\n"
           f"Verdict: {verdict} ({score}% match)\n"
           f"Climate: {c.get('temp_annual_mean_c','?')} C | "
           f"{c.get('annual_precip_mm','?')} mm/yr | {c.get('humidity_rel_pct','?')}% RH\n"
           f"Earth: https://earth.google.com/web/@{result.get('lat')},{result.get('lon')},0a,800d,35y,0h,45t,0r")
    from urllib.parse import quote
    return {"text": txt, "wa_url": f"https://api.whatsapp.com/send?text={quote(txt)}"}

@app.post("/api/batch/upload")
async def batch_upload(file: UploadFile = File(...)):
    content = (await file.read()).decode("utf-8")
    reader = csv.DictReader(io.StringIO(content)); rows = list(reader)
    async def one(row):
        try:
            country = row.get("country",""); place = row.get("place","")
            species = row.get("species","") or "Teck"
            if row.get("lat") and row.get("lon"):
                lat, lon = float(row["lat"]), float(row["lon"])
                label = place or f"{lat:.3f},{lon:.3f}"
            else:
                hits = await climate_svc.geocode(place, country)
                if not hits: return {"error": f"Not found: {place}", "row": row}
                hit = hits[0]; lat, lon = hit["latitude"], hit["longitude"]
                label = ", ".join(filter(None, [hit.get("name"), hit.get("country")]))
            clim = await climate_svc.climate(lat, lon)
            v = verdict_engine.evaluate(species, clim)
            return {"country": country, "place": label, "lat": lat, "lon": lon,
                    "species": species, "verdict": v["verdict"], "score": v["score"]}
        except Exception as e:
            return {"error": str(e)[:120], "row": row}
    sem = asyncio.Semaphore(4)
    async def wrapped(r):
        async with sem: return await one(r)
    results = await asyncio.gather(*(wrapped(r) for r in rows))
    return {"count": len(results), "results": results}

@app.get("/sw.js")
async def service_worker():
    sw = """
    const CACHE='reforestai-v52';
    const ASSETS=['/','/api/species'];
    self.addEventListener('install',e=>{e.waitUntil(caches.open(CACHE).then(c=>c.addAll(ASSETS)))});
    self.addEventListener('fetch',e=>{
      e.respondWith(caches.match(e.request).then(r=>r||fetch(e.request).catch(()=>caches.match('/'))));
    });
    """
    return Response(sw, media_type="application/javascript")

@app.get("/manifest.json")
async def manifest():
    return {"name":"ReforestAI","short_name":"Reforest","start_url":"/",
            "display":"standalone","background_color":"#070c17","theme_color":"#3ddc84",
            "icons":[]}

@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(FRONTEND)

# ==================================================================
# 13. FRONTEND  (only the changed section shown below; rest identical to v5.1)
# ==================================================================
FRONTEND = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<meta name="theme-color" content="#3ddc84"/>
<link rel="manifest" href="/manifest.json"/>
<title>ReforestAI v5.2</title>
<link href="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.css" rel="stylesheet"/>
<script src="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.js"></script>
<script src="https://unpkg.com/@watergis/maplibre-gl-terradraw@1.13.0/dist/maplibre-gl-terradraw.umd.js"></script>
<style>
  :root{--bg:#070c17;--panel:#0f1729;--panel2:#141e36;--line:#1e2b49;
    --txt:#e8eefb;--muted:#7f95b8;--accent:#3ddc84;--accent2:#4ea8ff;
    --warn:#ffb454;--bad:#ff5d5d;--purple:#a78bfa;--blue:#60a5fa;}
  *{box-sizing:border-box;margin:0;padding:0}
  html,body{height:100%;overflow:hidden;background:var(--bg);color:var(--txt);
    font-family:'Inter',system-ui,sans-serif}
  #app{display:grid;grid-template-columns:460px 1fr;height:100vh}
  aside{background:var(--panel);border-right:1px solid var(--line);
    display:flex;flex-direction:column;overflow:hidden}
  .brand{padding:14px 18px;border-bottom:1px solid var(--line)}
  .brand h1{font-size:18px;font-weight:800}
  .brand h1 span{color:var(--accent)}
  .brand p{font-size:11px;color:var(--muted);margin-top:3px}
  .lang-sel{display:flex;gap:4px;margin-top:6px}
  .lang-sel button{padding:3px 9px;border-radius:5px;border:1px solid var(--line);
    background:var(--panel2);color:var(--muted);font-size:10px;font-weight:700;cursor:pointer}
  .lang-sel button.on{background:rgba(78,168,255,.15);color:var(--accent2);border-color:var(--accent2)}
  .form{padding:12px 18px;border-bottom:1px solid var(--line);
    display:flex;flex-direction:column;gap:9px;max-height:44vh;overflow-y:auto}
  .field label{font-size:10px;text-transform:uppercase;letter-spacing:.6px;
    color:var(--muted);font-weight:700;display:block;margin-bottom:5px}
  .field select,.field input,.field textarea{width:100%;padding:9px 11px;background:var(--panel2);
    border:1px solid var(--line);border-radius:7px;color:var(--txt);font-size:12.5px;outline:none}
  .field input:focus,.field select:focus{border-color:var(--accent2)}
  #go{padding:11px;border:none;border-radius:8px;font-weight:800;font-size:13px;
    background:linear-gradient(135deg,var(--accent),#1ea75a);color:#04140c;cursor:pointer}
  #go:disabled{opacity:.55}
  .row2{display:grid;grid-template-columns:1fr 1fr;gap:8px}
  .row3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:6px}
  .mini{padding:8px 10px;border:1px solid var(--line);border-radius:7px;
    background:var(--panel2);color:var(--txt);font-size:11px;font-weight:700;cursor:pointer}
  .mini:hover{border-color:var(--accent2)}
  .mini.on{background:rgba(61,220,132,.15);color:var(--accent);border-color:var(--accent)}
  .hintline{font-size:10px;color:var(--muted);margin-top:4px;line-height:1.5}
  .ac-list{position:absolute;top:100%;left:0;right:0;background:var(--panel2);
    border:1px solid var(--line);border-radius:0 0 7px 7px;z-index:100;
    max-height:180px;overflow-y:auto;display:none}
  .ac-list.open{display:block}
  .ac-item{padding:7px 11px;font-size:12px;cursor:pointer;border-bottom:1px solid var(--line)}
  .ac-item:hover{background:rgba(78,168,255,.12);color:var(--accent2)}
  .ac-item small{display:block;color:var(--muted);font-size:10px}
  .results{flex:1;overflow-y:auto;padding:12px 14px 24px}
  .results::-webkit-scrollbar{width:8px}
  .results::-webkit-scrollbar-thumb{background:var(--line);border-radius:4px}
  .empty{text-align:center;color:var(--muted);font-size:12px;padding:30px 14px;line-height:1.8}
  .empty b{color:var(--txt);display:block;margin-bottom:6px;font-size:13px}
  .card{background:var(--panel2);border:1px solid var(--line);border-radius:10px;
    overflow:hidden;animation:slide .3s ease-out;margin-bottom:10px}
  @keyframes slide{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:none}}
  .vhead{padding:14px 16px;display:flex;justify-content:space-between;align-items:flex-start;gap:8px}
  .vhead .where{font-size:10px;color:var(--muted);text-transform:uppercase;
    letter-spacing:.5px;font-weight:700}
  .vhead .place{font-size:15px;font-weight:800;margin-top:2px}
  .vhead .coords{font-size:10px;color:var(--muted);font-family:ui-monospace,monospace;margin-top:3px}
  .vbadge{font-size:10px;font-weight:900;padding:5px 10px;border-radius:99px;
    letter-spacing:.4px;text-transform:uppercase}
  .vb-excellent{background:rgba(61,220,132,.15);color:var(--accent);border:1px solid rgba(61,220,132,.35)}
  .vb-good{background:rgba(78,168,255,.15);color:var(--accent2);border:1px solid rgba(78,168,255,.35)}
  .vb-marginal{background:rgba(255,180,84,.15);color:var(--warn);border:1px solid rgba(255,180,84,.35)}
  .vb-bad{background:rgba(255,93,93,.15);color:var(--bad);border:1px solid rgba(255,93,93,.35)}
  .species-name{padding:0 16px 6px;font-size:13px;font-weight:700;color:var(--purple)}
  .species-name small{color:var(--muted);font-weight:500;font-size:10.5px;margin-left:5px}
  .badge{display:inline-block;font-size:9px;font-weight:800;padding:2px 6px;
    border-radius:99px;margin-left:5px;text-transform:uppercase}
  .badge.inferred{background:rgba(255,180,84,.15);color:var(--warn);border:1px solid rgba(255,180,84,.35)}
  .badge.invasive{background:rgba(255,93,93,.15);color:var(--bad);border:1px solid rgba(255,93,93,.35)}
  .badge.native{background:rgba(61,220,132,.15);color:var(--accent);border:1px solid rgba(61,220,132,.35)}
  .badge.protected{background:rgba(96,165,250,.15);color:var(--blue);border:1px solid rgba(96,165,250,.35)}
  .score-bar{padding:0 16px 10px}
  .score-row{display:flex;justify-content:space-between;font-size:10.5px;
    color:var(--muted);margin-bottom:4px}
  .score-row b{color:var(--txt)}
  .bar{height:4px;background:rgba(0,0,0,.35);border-radius:99px;overflow:hidden}
  .bar>div{height:100%;border-radius:99px;transition:width .8s}
  .climate{display:grid;grid-template-columns:repeat(3,1fr);gap:6px;padding:0 16px 10px}
  .cl{background:rgba(0,0,0,.25);padding:7px 8px;border-radius:6px}
  .cl .k{font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.4px;font-weight:700}
  .cl .v{font-size:13px;font-weight:800;margin-top:2px}
  .checks{padding:0 16px 10px;display:flex;flex-direction:column;gap:4px}
  .chk{display:flex;align-items:center;gap:8px;padding:6px 8px;border-radius:6px;
    background:rgba(0,0,0,.2);font-size:11px}
  .chk .ic{width:14px;height:14px;border-radius:50%;display:flex;align-items:center;
    justify-content:center;font-size:9px;font-weight:900;flex-shrink:0}
  .chk.pass .ic{background:rgba(61,220,132,.2);color:var(--accent)}
  .chk.fail .ic{background:rgba(255,93,93,.2);color:var(--bad)}
  .chk .nm{font-weight:700;flex:1}
  .chk .vl{font-family:ui-monospace,monospace;font-size:10px;color:var(--muted)}
  .section{padding:10px 16px;border-top:1px solid var(--line);background:rgba(0,0,0,.12)}
  .section h4{font-size:10px;text-transform:uppercase;letter-spacing:.6px;
    font-weight:800;margin-bottom:8px}
  .section h4.purple{color:var(--purple)}
  .section h4.blue{color:var(--blue)}
  .section h4.accent{color:var(--accent)}
  .section h4.warn{color:var(--warn)}
  .row{display:flex;justify-content:space-between;align-items:center;
    font-size:11px;padding:5px 0;border-bottom:1px solid rgba(255,255,255,.04)}
  .row:last-child{border-bottom:none}
  .row .k{color:var(--muted)}
  .row .v{font-weight:700;font-family:ui-monospace,monospace}
  .v.good{color:var(--accent)}.v.warn{color:var(--warn)}.v.bad{color:var(--bad)}
  .trend-spark{display:flex;align-items:flex-end;gap:2px;height:32px;margin-top:6px}
  .trend-spark .sp{flex:1;background:linear-gradient(180deg,var(--warn),var(--bad));
    border-radius:2px 2px 0 0;min-height:2px}
  .forecast-grid{display:grid;grid-template-columns:repeat(7,1fr);gap:3px;margin-top:6px}
  .fc{background:rgba(0,0,0,.25);border-radius:5px;padding:5px 3px;text-align:center;font-size:9px}
  .fc .d{color:var(--muted);font-weight:700;margin-bottom:3px}
  .fc .t{color:var(--txt);font-weight:800}
  .fc .p{color:var(--accent2);margin-top:2px}
  .alt-chips{display:flex;flex-wrap:wrap;gap:5px;margin-top:4px}
  .alt{font-size:10.5px;padding:4px 8px;border-radius:5px;background:rgba(78,168,255,.15);
    color:var(--accent2);border:1px solid rgba(78,168,255,.3);cursor:pointer;font-weight:600}
  .alt:hover{background:rgba(78,168,255,.25)}
  .toolbar{padding:10px 16px;border-top:1px solid var(--line);
    display:flex;flex-wrap:wrap;gap:5px;background:rgba(0,0,0,.15)}
  .toolbar a,.toolbar button{font-size:10.5px;padding:6px 10px;border-radius:6px;
    text-decoration:none;background:rgba(78,168,255,.12);color:var(--accent2);
    border:1px solid rgba(78,168,255,.28);cursor:pointer;font-weight:700;
    display:inline-flex;align-items:center;gap:4px}
  .toolbar a:hover,.toolbar button:hover{background:rgba(78,168,255,.22)}
  .toolbar button.primary{background:rgba(61,220,132,.15);color:var(--accent);
    border-color:rgba(61,220,132,.35)}
  #map{height:100%;width:100%}
  .maplibregl-popup-content{background:var(--panel2);color:var(--txt);
    border-radius:8px;border:1px solid var(--line);padding:8px 12px;font-size:11px}
  .maplibregl-popup-tip{border-top-color:var(--panel2)!important}
  .maplibregl-ctrl-group{background:var(--panel)!important;border:1px solid var(--line)!important}
  .maplibregl-ctrl-icon{filter:invert(.8)}
  .float{position:absolute;top:14px;right:14px;z-index:500;display:flex;
    gap:6px;flex-wrap:wrap;max-width:520px}
  .float button{padding:7px 12px;background:rgba(15,23,41,.9);border:1px solid var(--line);
    color:var(--txt);border-radius:7px;cursor:pointer;font-size:11px;font-weight:600;
    backdrop-filter:blur(10px)}
  .float button:hover{border-color:var(--accent2)}
  .float button.on{background:rgba(61,220,132,.15);border-color:var(--accent);color:var(--accent)}
  .hud{position:absolute;bottom:14px;left:14px;z-index:500;
    background:rgba(11,18,32,.85);border:1px solid var(--line);padding:7px 12px;
    border-radius:7px;font-size:11px;color:var(--muted);backdrop-filter:blur(10px)}
  .hud b{color:var(--accent)}
  .pin{width:20px;height:20px;border-radius:50%;position:relative}
  .pin:before{content:'';position:absolute;inset:0;border-radius:50%;
    background:var(--accent);box-shadow:0 0 12px var(--accent);animation:pulse 1.8s infinite}
  .pin:after{content:'';position:absolute;inset:5px;border-radius:50%;
    background:#fff;border:2px solid var(--accent)}
  @keyframes pulse{0%{transform:scale(.9);opacity:1}70%{transform:scale(2.4);opacity:0}100%{transform:scale(2.4);opacity:0}}
  .spinner{width:12px;height:12px;border:2px solid var(--line);border-top-color:var(--accent);
    border-radius:50%;display:inline-block;animation:sp .8s linear infinite;margin-right:6px;vertical-align:-2px}
  @keyframes sp{to{transform:rotate(360deg)}}
  .modal-bg{position:fixed;inset:0;background:rgba(0,0,0,.85);z-index:2000;
    display:none;align-items:center;justify-content:center;backdrop-filter:blur(6px)}
  .modal-bg.open{display:flex}
  .modal{background:var(--panel);border:1px solid var(--line);border-radius:12px;
    padding:18px;width:min(620px,92vw);max-height:92vh;overflow-y:auto;position:relative}
  .modal h3{font-size:14px;margin-bottom:5px;font-weight:800}
  .modal p{font-size:11px;color:var(--muted);margin-bottom:12px;line-height:1.5}
  .modal video,.modal canvas.preview{width:100%;border-radius:8px;background:#000;
    display:block;max-height:55vh;object-fit:cover}
  .modal canvas.preview{display:none}
  .modal-actions{display:flex;gap:6px;margin-top:12px;justify-content:flex-end}
  .modal-actions button{padding:8px 14px;border-radius:7px;border:1px solid var(--line);
    background:var(--panel2);color:var(--txt);font-weight:700;font-size:11.5px;cursor:pointer}
  .modal-actions button.primary{background:var(--accent);color:#04140c;border-color:var(--accent)}
  .modal-close{position:absolute;top:12px;right:12px;background:none;border:none;
    color:var(--muted);font-size:20px;cursor:pointer}
  .photo-metrics{display:grid;grid-template-columns:repeat(3,1fr);gap:6px;margin-top:12px}
  .pm{background:rgba(0,0,0,.3);padding:8px;border-radius:6px;text-align:center}
  .pm .k{font-size:9px;color:var(--muted);text-transform:uppercase;font-weight:700}
  .pm .v{font-size:16px;font-weight:900;margin-top:3px}
  .pm .v.good{color:var(--accent)}.pm .v.warn{color:var(--warn)}.pm .v.bad{color:var(--bad)}
  .speak-btn{position:absolute;right:10px;top:50%;transform:translateY(-50%);
    background:none;border:none;color:var(--muted);font-size:16px;cursor:pointer;padding:4px}
  .speak-btn:hover{color:var(--accent)}
</style>
</head>
<body>
<div id="app">
  <aside>
    <div class="brand">
      <h1>🌳 Reforest<span>AI</span> <small style="font-size:9px;color:var(--muted)">v5.2</small></h1>
      <p id="subtitle">Any tree · any place · climate · micro-siting · camera · future projection</p>
      <div class="lang-sel">
        <button class="on" data-lang="en">EN</button>
        <button data-lang="hi">हिं</button>
        <button data-lang="ta">த</button>
      </div>
    </div>
    <div class="form">
      <div class="field">
        <label>1 · Country</label>
        <select id="country"><option value="">— Loading… —</option></select>
      </div>
      <div class="field">
        <label>2 · Place / City / Region</label>
        <input id="place" type="text" placeholder="e.g. Nairobi, Manaus, Tamil Nadu"/>
      </div>
      <div class="field" style="position:relative">
        <label>3 · Species (type any tree)</label>
        <input id="species" type="text" placeholder="e.g. Moringa, Teak, Eucalyptus"/>
        <button class="speak-btn" id="speak-btn" title="Voice input">🎤</button>
        <div class="ac-list" id="ac-list"></div>
        <div class="hintline">Any name — we'll match or infer tolerance.</div>
      </div>
      <button id="go">🌱 Analyze Growing Conditions</button>
      <div class="row3">
        <button class="mini" id="btn-reverse">🔍 What to plant?</button>
        <button class="mini" id="btn-carbon">🌍 Carbon</button>
        <button class="mini" id="btn-batch">📤 Batch</button>
      </div>
      <div class="row2">
        <button class="mini" id="btn-polygon">📐 Draw polygon</button>
        <button class="mini" id="btn-share">🔗 Share</button>
      </div>
    </div>
    <div class="results" id="results">
      <div class="empty">
        <b>ReforestAI v5.2</b>
        Pick a country, type a place, and type any tree.
        Draw a polygon to size up a whole plot. Share via WhatsApp with one click.
      </div>
    </div>
  </aside>
  <main style="position:relative">
    <div id="map"></div>
    <div class="float">
      <button id="btn-sat">🛰 Satellite</button>
      <button id="btn-ter">🗺 Terrain</button>
      <button id="btn-contour">⛰ Contours</button>
      <button id="btn-ndvi">🌿 NDVI</button>
      <button id="btn-community">👥 Community</button>
      <button id="btn-spin">🌐 Spin</button>
    </div>
    <div class="hud">Live · Multi-source · <b id="status">Ready</b></div>
  </main>
</div>

<div class="modal-bg" id="cam-modal">
  <div class="modal">
    <button class="modal-close" onclick="closeCamera()">×</button>
    <h3>📷 Site Inspection Camera</h3>
    <p>Point at the ground. We analyze greenness in real time — nothing is uploaded.</p>
    <video id="cam-video" autoplay playsinline muted></video>
    <canvas id="cam-canvas" class="preview"></canvas>
    <div id="cam-metrics"></div>
    <div class="modal-actions">
      <button onclick="closeCamera()">Cancel</button>
      <button class="primary" onclick="captureAndAnalyze()">Capture &amp; Analyze</button>
    </div>
  </div>
</div>

<div class="modal-bg" id="batch-modal">
  <div class="modal">
    <button class="modal-close" onclick="closeBatch()">×</button>
    <h3>📤 Batch CSV Upload</h3>
    <p>Upload CSV with columns: <code>country, place, species</code> (optional: lat, lon).</p>
    <input type="file" id="batch-file" accept=".csv"/>
    <div id="batch-result" style="margin-top:10px;font-size:11px"></div>
    <div class="modal-actions">
      <button onclick="closeBatch()">Close</button>
      <button class="primary" onclick="runBatch()">Run</button>
    </div>
  </div>
</div>

<script src="https://unpkg.com/topojson-client@3"></script>
<script>
const I18N = {
  en:{subtitle:"Any tree · any place · climate · micro-siting · camera · future projection"},
  hi:{subtitle:"कोई भी पेड़ · कोई भी जगह · जलवायु · सूक्ष्म-स्थल · कैमरा · भविष्य"},
  ta:{subtitle:"எந்த மரமும் · எந்த இடமும் · காலநிலை · நுண்-இடம் · கேமரா · எதிர்காலம்"},
};
let lang='en';
function applyLang(l){
  lang=l;
  const s=document.getElementById('subtitle');
  if(I18N[l]) s.textContent=I18N[l].subtitle;
  document.querySelectorAll('.lang-sel button').forEach(b=>b.classList.toggle('on',b.dataset.lang===l));
}
document.querySelectorAll('.lang-sel button').forEach(b=>b.onclick=()=>applyLang(b.dataset.lang));

const SAT={version:8,sources:{sat:{type:'raster',tileSize:256,attribution:'Esri',tiles:['https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}']}},layers:[{id:'bg',type:'background',paint:{'background-color':'#070c17'}},{id:'sat',type:'raster',source:'sat'}]};
const TER={version:8,sources:{ter:{type:'raster',tileSize:256,attribution:'OpenTopoMap',tiles:['https://a.tile.opentopomap.org/{z}/{x}/{y}.png']}},layers:[{id:'bg',type:'background',paint:{'background-color':'#070c17'}},{id:'ter',type:'raster',source:'ter'}]};

const map=new maplibregl.Map({container:'map',style:SAT,center:[10,20],zoom:1.6,pitch:0,bearing:0,attributionControl:false});
map.addControl(new maplibregl.NavigationControl({visualizePitch:true}),'bottom-right');
map.addControl(new maplibregl.ScaleControl({maxWidth:120,unit:'metric'}),'bottom-left');

let marker=null, gridMarkers=[], lastResult=null;
let contourOn=false, ndviOn=false, communityOn=false;
let drawControl=null;

function clearGrid(){gridMarkers.forEach(m=>m.remove());gridMarkers=[]}
function dropPin(lat,lon){
  if(marker) marker.remove();
  const el=document.createElement('div');el.className='pin';
  marker=new maplibregl.Marker({element:el,anchor:'center'}).setLngLat([lon,lat]).addTo(map);
}
async function flyTo(lat,lon){
  setStatus('Rotating globe…');
  map.flyTo({center:[lon,lat],zoom:1.4,bearing:-60,pitch:0,duration:1400,essential:true});
  await wait(1450);
  setStatus('Descending…');
  map.flyTo({center:[lon,lat],zoom:8.5,bearing:25+Math.random()*35,pitch:55,duration:2600,curve:1.6,essential:true});
  await wait(2650);
  dropPin(lat,lon); setStatus('Site locked');
}
function wait(ms){return new Promise(r=>setTimeout(r,ms))}
function setStatus(t){document.getElementById('status').textContent=t}
function esc(s){return String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}

document.getElementById('btn-sat').onclick=()=>map.setStyle(SAT);
document.getElementById('btn-ter').onclick=()=>map.setStyle(TER);
document.getElementById('btn-spin').onclick=()=>{
  let b=map.getBearing();const s=setInterval(()=>{b+=6;map.setBearing(b);
    if(b>=map.getBearing()+720)clearInterval(s)},40);setTimeout(()=>clearInterval(s),5000);
};
document.getElementById('btn-contour').onclick=(e)=>{
  contourOn=!contourOn;e.currentTarget.classList.toggle('on',contourOn);
  if(contourOn){
    if(!map.getSource('contour-src')){
      map.addSource('contour-src',{type:'raster',tileSize:256,tiles:['https://a.tile.opentopomap.org/{z}/{x}/{y}.png']});
      map.addLayer({id:'contour-layer',type:'raster',source:'contour-src',paint:{'raster-opacity':.55}});
    } else map.setLayoutProperty('contour-layer','visibility','visible');
  } else if(map.getLayer('contour-layer')) map.setLayoutProperty('contour-layer','visibility','none');
};
document.getElementById('btn-ndvi').onclick=(e)=>{
  ndviOn=!ndviOn;e.currentTarget.classList.toggle('on',ndviOn);
  const t=new Date(),y=t.getFullYear(),m=String(t.getMonth()+1).padStart(2,'0'),d=String(t.getDate()).padStart(2,'0');
  if(ndviOn){
    if(!map.getSource('ndvi-src')){
      map.addSource('ndvi-src',{type:'raster',tileSize:256,
        tiles:[`https://gibs.earthdata.nasa.gov/wmts/epsg3857/best/MODIS_Terra_CorrectedReflectance_TrueColor/default/${y}-${m}-${d}/GoogleMapsCompatible_Level9/{z}/{y}/{x}.jpg`]});
      map.addLayer({id:'ndvi-layer',type:'raster',source:'ndvi-src',paint:{'raster-opacity':.6}},'sat');
    } else map.setLayoutProperty('ndvi-layer','visibility','visible');
  } else if(map.getLayer('ndvi-layer')) map.setLayoutProperty('ndvi-layer','visibility','none');
};
document.getElementById('btn-community').onclick=async(e)=>{
  communityOn=!communityOn;e.currentTarget.classList.toggle('on',communityOn);
  if(!communityOn){clearGrid();return;}
  if(!lastResult){setStatus('Analyze a site first');return;}
  try{
    const r=await fetch('/api/community/nearby',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify(lastResult)});
    const {nearby}=await r.json();
    nearby.forEach(p=>{
      const el=document.createElement('div');el.className='pin';
      el.style.filter='hue-rotate(200deg)';
      const m=new maplibregl.Marker({element:el}).setLngLat([p.lon,p.lat]).addTo(map);
      m.setPopup(new maplibregl.Popup().setHTML(`<b>${esc(p.species)}</b><br>${esc(p.status)}`));
      gridMarkers.push(m);
    });
    setStatus(`Community: ${nearby.length} nearby`);
  }catch(err){setStatus('Community failed')}
};

async function loadCountries(){
  const sel=document.getElementById('country');
  try{
    const r=await fetch('https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json');
    const topo=await r.json();
    const geo=topojson.feature(topo,topo.objects.countries);
    const names=geo.features.map(f=>f.properties.name).filter(Boolean).sort();
    sel.innerHTML='<option value="">— Select country —</option>'+
      names.map(n=>`<option value="${esc(n)}">${esc(n)}</option>`).join('');
  }catch(e){sel.innerHTML='<option>Failed</option>'}
}
async function loadSpecies(){
  try{
    const r=await fetch('/api/species');
    const {species}=await r.json();
    window.__localSpecies=species;
    document.getElementById('species').value='Tectona grandis';
  }catch(e){}
}
loadSpecies();loadCountries();

let acTimer=null;
document.getElementById('species').addEventListener('input',e=>{
  clearTimeout(acTimer);
  const q=e.target.value.trim(),box=document.getElementById('ac-list');
  if(q.length<2){box.classList.remove('open');return;}
  acTimer=setTimeout(async()=>{
    const local=(window.__localSpecies||[]).filter(s=>s.toLowerCase().includes(q.toLowerCase())).slice(0,4);
    let remote=[];
    try{
      const r=await fetch('/api/species/autocomplete',{method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({q,limit:6})});
      remote=r.ok?(await r.json()).suggestions||[]:[];
    }catch(err){}
    const items=[
      ...local.map(s=>({l:s,sub:'catalog'})),
      ...remote.map(x=>({l:x.scientificName,sub:`${x.family||''} · ${x.genus||''}`})),
    ].filter((v,i,a)=>a.findIndex(x=>x.l===v.l)===i);
    if(!items.length){box.classList.remove('open');return;}
    // FIX: escape HTML
    box.innerHTML=items.map(it=>`<div class="ac-item" data-v="${esc(it.l)}">${esc(it.l)}<small>${esc(it.sub)}</small></div>`).join('');
    box.classList.add('open');
    box.querySelectorAll('.ac-item').forEach(el=>el.onclick=()=>{
      document.getElementById('species').value=el.dataset.v;box.classList.remove('open');
    });
  },250);
});
document.addEventListener('click',e=>{
  if(!e.target.closest('#species')&&!e.target.closest('#ac-list'))
    document.getElementById('ac-list').classList.remove('open');
});

document.getElementById('speak-btn').onclick=()=>{
  if(!('webkitSpeechRecognition' in window)&&!('SpeechRecognition' in window)){
    alert('Voice input not supported in this browser');return;
  }
  const SR=window.SpeechRecognition||window.webkitSpeechRecognition;
  const rec=new SR();rec.lang='en-US';rec.interimResults=false;
  rec.onresult=e=>{document.getElementById('species').value=e.results[0][0].transcript};
  rec.start();
};

const SC={excellent:'var(--accent)',good:'var(--accent2)',marginal:'var(--warn)',bad:'var(--bad)'};
const BC={excellent:'vb-excellent',good:'vb-good',marginal:'vb-marginal',bad:'vb-bad'};

function renderCard(res){
  const bc=BC[res.verdict_key]||BC.marginal, sc=SC[res.verdict_key]||SC.marginal;
  const c=res.climate||{};
  const climate=`<div class="climate">
    <div class="cl"><div class="k">Temp</div><div class="v">${c.temp_annual_mean_c}°C</div></div>
    <div class="cl"><div class="k">Humidity</div><div class="v">${c.humidity_rel_pct}%</div></div>
    <div class="cl"><div class="k">Rain</div><div class="v">${c.annual_precip_mm}mm</div></div>
    <div class="cl"><div class="k">Heat days</div><div class="v">${c.heatwave_days_yr}</div></div>
    <div class="cl"><div class="k">Elevation</div><div class="v">${c.elevation_m??'—'}m</div></div>
    <div class="cl"><div class="k">ONI</div><div class="v">${c.oni_value??'—'}</div></div>
  </div>`;
  const checks=res.checks.map(x=>`<div class="chk ${x.pass?'pass':'fail'}">
    <div class="ic">${x.pass?'✓':'✕'}</div><div class="nm">${esc(x.factor)}</div>
    <div class="vl">${esc(x.actual)} / ${esc(x.required)}</div></div>`).join('');
  const reasoning=res.reasoning.length?`<div class="section"><h4 class="purple">🧠 Reasoning</h4>
    <ul style="list-style:none;display:flex;flex-direction:column;gap:5px">
    ${res.reasoning.map(r=>`<li style="font-size:11px;line-height:1.5;padding-left:12px;position:relative">▸ ${esc(r)}</li>`).join('')}</ul></div>`:'';
  const alts=res.alternatives&&res.alternatives.length?`<div class="section"><h4 class="blue">💡 Better alternatives</h4>
    <div class="alt-chips">${res.alternatives.map(a=>`<span class="alt" data-s="${esc(a)}">${esc(a)}</span>`).join('')}</div></div>`:'';
  const ni=res.native_invasive||{};
  const niBadge=ni.status&&ni.status!=='unknown'?`<span class="badge ${ni.status==='native'?'native':ni.status==='invasive'?'invasive':''}">${esc(ni.status)}</span>`:'';
  const prot=res.protected||{};
  const protRow=prot.available?`<div class="row"><span class="k">🛡 Protected area</span>
    <span class="v ${prot.inside_protected?'warn':'good'}">${prot.inside_protected?'Inside '+esc(prot.name||''):'Outside'}</span></div>`:'';
  let trendSection='';
  if(res.trend&&res.trend.available){
    const sparks=res.trend.points.map(p=>`<div class="sp" style="height:${Math.max(2,Math.min(32,(p.temp_c-5)*1.6))}px" title="${p.year}: ${p.temp_c}°C"></div>`).join('');
    trendSection=`<div class="section"><h4 class="blue">📈 30-year trend</h4>
      <div class="row"><span class="k">Warming / drying</span>
      <span class="v ${res.trend.warming?'bad':'good'}">${esc(res.trend.summary)}</span></div>
      <div class="trend-spark">${sparks}</div></div>`;
  }
  let futureSection='';
  if((res.future_2050&&res.future_2050.available)||(res.future_2070&&res.future_2070.available)){
    const f50=res.future_2050||{}, f70=res.future_2070||{};
    const rows=[];
    if(f50.available) rows.push(`<div class="row"><span class="k">2050 (SSP2-4.5)</span>
      <span class="v ${f50.warming_c>2?'bad':'warn'}">+${f50.warming_c}°C · ${f50.precip_change_pct>0?'+':''}${f50.precip_change_pct}% rain</span></div>`);
    if(f70.available) rows.push(`<div class="row"><span class="k">2070 (SSP2-4.5)</span>
      <span class="v ${f70.warming_c>3?'bad':'warn'}">+${f70.warming_c}°C · ${f70.precip_change_pct>0?'+':''}${f70.precip_change_pct}% rain</span></div>`);
    if(rows.length) futureSection=`<div class="section"><h4 class="warn">🌡 Future climate</h4>${rows.join('')}</div>`;
  }
  let frostSection='';
  if(res.frost&&res.frost.available){
    const fr=res.frost;
    const cls=fr.seedling_risk==='low'?'good':fr.seedling_risk==='moderate'?'warn':'bad';
    frostSection=`<div class="section"><h4 class="blue">❄ Frost risk (seedlings)</h4>
      <div class="row"><span class="k">Min next 14 days</span><span class="v">${fr.min_next_14d}°C</span></div>
      <div class="row"><span class="k">Frost days</span><span class="v ${cls}">${fr.frost_days_next_14d} (${fr.seedling_risk})</span></div></div>`;
  }
  let irrigationSection='';
  if(res.irrigation&&res.irrigation.available){
    const ir=res.irrigation;
    irrigationSection=`<div class="section"><h4 class="accent">💧 Irrigation plan</h4>
      <div class="row"><span class="k">ET₀ / precip deficit</span><span class="v">${ir.annual_et0_mm} / ${ir.deficit_mm} mm</span></div>
      <div class="row"><span class="k">Per tree / week</span><span class="v">${ir.liters_per_tree_per_week} L</span></div>
      <div class="row"><span class="k">For 100 trees / week</span><span class="v accent">${ir.liters_total_weekly} L</span></div></div>`;
  }
  let soilSection='';
  if(res.soil&&res.soil.ph!=null){
    const s=res.soil,cls=s.ph>=5.5&&s.ph<=7.5?'good':s.ph>=4.5&&s.ph<=8.5?'warn':'bad';
    soilSection=`<div class="section"><h4 class="blue">🧪 Soil</h4>
      <div class="row"><span class="k">pH</span><span class="v ${cls}">${s.ph} (${esc(s.source)})</span></div>
      <div class="row"><span class="k">Drainage</span><span class="v">${esc(s.drainage)}</span></div></div>`;
  }
  let forestSection='';
  if(res.forest&&res.forest.available){
    const f=res.forest;
    forestSection=`<div class="section"><h4 class="accent">🌲 Forest cover</h4>
      <div class="row"><span class="k">Tree cover (2000)</span><span class="v">${f.tree_cover_pct_2000}%</span></div>
      <div class="row"><span class="k">Status</span><span class="v">${esc(f.status)}</span></div></div>`;
  }
  let ndviSection='';
  if(res.ndvi&&res.ndvi.available){
    ndviSection=`<div class="section"><h4 class="accent">🛰 Sentinel-2 NDVI</h4>
      <div class="row"><span class="k">Scene</span><span class="v good">${esc(res.ndvi.ndvi_hint)} · ${esc(res.ndvi.scene_date)}</span></div></div>`;
  }
  let fcSection='';
  if(res.forecast&&res.forecast.available){
    const days=res.forecast.days.map(d=>`<div class="fc">
      <div class="d">${(d.date||'').slice(5)}</div>
      <div class="t">${d.t_max!=null?Math.round(d.t_max)+'°':'—'}</div>
      <div class="p">${d.precip!=null?d.precip.toFixed(1):''}</div></div>`).join('');
    fcSection=`<div class="section"><h4 class="blue">🌦 7-day outlook</h4>
      <div class="row"><span class="k">${esc(res.forecast.advice)}</span><span class="v">${res.forecast.total_rain_mm}mm rain</span></div>
      <div class="forecast-grid">${days}</div></div>`;
  }
  let carbonSection='';
  if(res.carbon_kg_yr){
    const t=(res.carbon_kg_yr*20*100*0.8)/1000;
    carbonSection=`<div class="section"><h4 class="accent">🌍 Carbon (100 trees / 20 yr)</h4>
      <div class="row"><span class="k">Per tree / year</span><span class="v">${res.carbon_kg_yr} kg CO₂</span></div>
      <div class="row"><span class="k">Total</span><span class="v accent">${t.toFixed(1)} t CO₂</span></div></div>`;
  }
  let reverseSection='';
  if(res.reverse_species&&res.reverse_species.length){
    reverseSection=`<div class="section"><h4 class="blue">🔍 What grows well here</h4>
      <div class="alt-chips">${res.reverse_species.slice(0,6).map(r=>`<span class="alt" data-s="${esc(r.species)}">${esc(r.species)} · ${r.score}%</span>`).join('')}</div></div>`;
  }
  const srcLine=`${c.source_year||'—'} · ${esc(c.source||'climate')}`;
  const inferred=res.species_inferred?`<span class="badge inferred">inferred</span>`:'';
  const protBadge=prot.inside_protected?`<span class="badge protected">protected</span>`:'';
  const gh=`https://earth.google.com/web/@${res.lat},${res.lon},0a,800d,35y,0h,45t,0r`;
  const gsv=`https://www.google.com/maps/@?api=1&map_action=pano&viewpoint=${res.lat},${res.lon}`;
  const gmaps=`https://www.google.com/maps/@${res.lat},${res.lon},18z/data=!3m1!1e3`;
  const toolbar=`<div class="toolbar">
    <button class="primary micro-btn">🎯 Micro-site</button>
    <button class="cam-btn">📷 Camera</button>
    <a href="${gh}" target="_blank" rel="noopener">🛰 Earth</a>
    <a href="${gsv}" target="_blank" rel="noopener">🚶 Street</a>
    <a href="${gmaps}" target="_blank" rel="noopener">🗺 Maps</a>
    <button class="kml-btn">📥 KML</button>
    <button class="wa-btn">💬 WhatsApp</button>
  </div>`;
  return `<div class="card" style="border-color:${sc}33"
      data-lat="${res.lat}" data-lon="${res.lon}" data-species="${esc(res.species)}"
      data-result='${esc(JSON.stringify(res))}'>
    <div class="vhead">
      <div>
        <div class="where">${esc(res.country||'Location')}</div>
        <div class="place">${esc(res.place)}</div>
        <div class="coords">${res.lat.toFixed(3)}°, ${res.lon.toFixed(3)}° · ${srcLine}</div>
      </div>
      <div class="vbadge ${bc}">${esc(res.verdict)}</div>
    </div>
    <div class="species-name">🌱 ${esc(res.species)}${inferred}${niBadge}${protBadge}
      <small>${esc(res.biome)}</small></div>
    ${ni.advisory?`<div style="padding:0 16px 8px;font-size:10.5px;color:var(--muted)">${esc(ni.advisory)}</div>`:''}
    <div class="score-bar">
      <div class="score-row"><span>Climate match</span><b>${res.score}% · confidence ${res.confidence}%</b></div>
      <div class="bar"><div style="width:${res.score}%;background:${sc}"></div></div>
    </div>
    ${climate}
    <div class="checks">${checks}</div>
    ${reasoning}${alts}${trendSection}${futureSection}${frostSection}
    ${irrigationSection}${soilSection}${forestSection}${ndviSection}${fcSection}${carbonSection}${reverseSection}
    ${protRow?`<div class="section"><h4 class="blue">🛡 Legal</h4>${protRow}</div>`:''}
    ${toolbar}
  </div>`;
}

document.getElementById('go').onclick=async()=>{
  const country=document.getElementById('country').value;
  const place=document.getElementById('place').value.trim();
  const species=document.getElementById('species').value.trim();
  const btn=document.getElementById('go'),results=document.getElementById('results');
  if(!place){alert('Enter a place');return}
  if(!species){alert('Type the tree/plant species');return}
  btn.disabled=true;btn.textContent='⏳ Locating…';
  clearGrid();
  results.insertAdjacentHTML('afterbegin',`<div class="card" id="load">
    <div style="padding:18px;text-align:center"><span class="spinner"></span>
    Geocoding & fetching all data sources…</div></div>`);
  try{
    const g=await fetch('/api/geocode',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({place,country})});
    if(!g.ok) throw new Error((await g.json()).detail||'Geocode failed');
    const hit=(await g.json()).results[0];
    const lat=hit.latitude,lon=hit.longitude;
    const label=[hit.name,hit.admin1,hit.country].filter(Boolean).join(', ')||hit.name||'Unknown';
    const flightP=flyTo(lat,lon);
    const a=await fetch('/api/analyze',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({lat,lon,species,place:label,country})});
    if(!a.ok) throw new Error((await a.json()).detail||'Analysis failed');
    const res=await a.json();
    await flightP;
    document.getElementById('load')?.remove();
    lastResult=res;
    results.insertAdjacentHTML('afterbegin',renderCard(res));
    btn.disabled=false;btn.textContent='🌱 Analyze Growing Conditions';
    wireCard(results.querySelector('.card[data-lat]'));
  }catch(err){
    document.getElementById('load')?.remove();
    results.insertAdjacentHTML('afterbegin',
      `<div class="card" style="border-color:rgba(255,93,93,.4)">
        <div style="padding:14px;color:var(--bad);font-size:12px">⚠ ${esc(err.message)}</div></div>`);
    btn.disabled=false;btn.textContent='🌱 Analyze Growing Conditions';
  }
};

document.getElementById('species').addEventListener('keydown',e=>{
  if(e.key==='Enter') document.getElementById('go').click();
});

function wireCard(card){
  if(!card) return;
  card.querySelectorAll('.alt').forEach(el=>el.onclick=()=>{
    document.getElementById('species').value=el.dataset.s;
    document.getElementById('go').click();
  });
  const cam=card.querySelector('.cam-btn'); if(cam) cam.onclick=()=>openCamera();
  const kml=card.querySelector('.kml-btn'); if(kml) kml.onclick=()=>downloadExport(card,'kml');
  const wa=card.querySelector('.wa-btn'); if(wa) wa.onclick=()=>shareWhatsApp(card);
  const mic=card.querySelector('.micro-btn'); if(mic) mic.onclick=async(e)=>{
    const b=e.currentTarget;b.disabled=true;b.textContent='⏳ Scanning 25 points…';
    const res=JSON.parse(card.dataset.result);
    try{
      const r=await fetch('/api/microsite',{method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({lat:res.lat,lon:res.lon,species:res.species,radius_m:300,grid:5})});
      const micro=await r.json();
      renderMicro(micro,card);
      b.textContent='✓ Safest spots';
      setStatus('Micro-site heatmap ready');
    }catch(err){b.disabled=false;b.textContent='🎯 Micro-site';alert('Failed: '+err.message)}
  };
}

function renderMicro(micro,parent){
  const top3=micro.top3||[];
  if(!top3.length){
    parent.insertAdjacentHTML('beforeend',`<div class="section"><h4 class="accent">🎯 Micro-site</h4>
      <div style="font-size:11px;color:var(--muted)">No suitable points found.</div></div>`);
    return;
  }
  const rows=top3.map((p,i)=>`<div class="row" style="cursor:pointer" data-lat="${p.lat}" data-lon="${p.lon}">
    <span class="k">#${i+1} · ${p.micro_score}/100 safety</span>
    <span class="v">${p.elevation_m??'—'}m · slope ${p.slope_pct}%</span></div>`).join('');
  parent.insertAdjacentHTML('beforeend',`<div class="section"><h4 class="accent">
    🎯 Safest spots (${micro.grid}×${micro.grid}, ${micro.radius_m}m)</h4>${rows}</div>`);
  parent.querySelectorAll('.row[data-lat]').forEach(el=>el.onclick=()=>{
    const la=parseFloat(el.dataset.lat),lo=parseFloat(el.dataset.lon);
    map.flyTo({center:[lo,la],zoom:16,pitch:55,duration:1500});dropPin(la,lo);
  });
  micro.points.forEach(p=>{
    const el=document.createElement('div');
    el.style.cssText=`width:10px;height:10px;border-radius:50%;border:2px solid #fff;background:${p.micro_score>=85?'#3ddc84':p.micro_score>=70?'#4ea8ff':p.micro_score>=50?'#ffb454':'#ff5d5d'}`;
    const m=new maplibregl.Marker({element:el}).setLngLat([p.lon,p.lat]).addTo(map);
    gridMarkers.push(m);
  });
}

document.getElementById('btn-reverse').onclick=async()=>{
  const country=document.getElementById('country').value;
  const place=document.getElementById('place').value.trim();
  if(!place){alert('Enter a place first');return}
  try{
    const g=await fetch('/api/geocode',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({place,country})});
    const hit=(await g.json()).results[0];
    const r=await fetch('/api/reverse',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({lat:hit.latitude,lon:hit.longitude})});
    const data=await r.json();
    const results=document.getElementById('results');
    const recs=data.recommended.map(s=>`<span class="alt" data-s="${esc(s.species)}">${esc(s.species)} · ${s.score}%</span>`).join('');
    const plan=data.succession_plan;
    const planHTML=`<div class="section"><h4 class="accent">🌱 Succession plan</h4>
      <div class="row"><span class="k">Phase 1 · Pioneers (0-5 yr)</span><span class="v">${plan.phase_1_pioneers_0_5yr.map(x=>esc(x.species)).slice(0,2).join(', ')||'—'}</span></div>
      <div class="row"><span class="k">Phase 2 · Mid-story (5-15 yr)</span><span class="v">${plan.phase_2_midstory_5_15yr.map(x=>esc(x.species)).slice(0,2).join(', ')||'—'}</span></div>
      <div class="row"><span class="k">Phase 3 · Climax (15+ yr)</span><span class="v">${plan.phase_3_climax_15yr_plus.map(x=>esc(x.species)).slice(0,2).join(', ')||'—'}</span></div></div>`;
    results.insertAdjacentHTML('afterbegin',`<div class="card">
      <div class="vhead"><div><div class="where">Reverse engine</div>
      <div class="place">${esc(place)}</div></div></div>
      <div class="section"><h4 class="blue">🔍 Species that thrive here</h4>
      <div class="alt-chips">${recs}</div></div>
      ${planHTML}</div>`);
    const card=results.querySelector('.card');
    card.querySelectorAll('.alt').forEach(el=>el.onclick=()=>{
      document.getElementById('species').value=el.dataset.s;
      document.getElementById('go').click();
    });
  }catch(err){alert('Reverse failed: '+err.message)}
};

document.getElementById('btn-carbon').onclick=async()=>{
  const sp=document.getElementById('species').value.trim();
  if(!sp){alert('Type a species first');return}
  const r=await fetch('/api/carbon',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({species:sp,tree_count:100,years:20})});
  const data=await r.json();
  const results=document.getElementById('results');
  results.insertAdjacentHTML('afterbegin',`<div class="card">
    <div class="vhead"><div><div class="where">Carbon estimate</div>
    <div class="place">${esc(sp)} · 100 trees · 20 yr</div></div></div>
    <div class="section"><h4 class="accent">🌍 CO₂ sequestration</h4>
    <div class="row"><span class="k">Per tree / year</span><span class="v">${data.kg_per_tree_per_year} kg</span></div>
    <div class="row"><span class="k">Total (20 yr)</span><span class="v accent">${data.total_tonnes_co2} t CO₂</span></div>
    <div class="row"><span class="k">Equivalent km driven</span><span class="v">${data.equivalent_km_driven} km</span></div></div></div>`);
};

document.getElementById('btn-batch').onclick=()=>document.getElementById('batch-modal').classList.add('open');
function closeBatch(){document.getElementById('batch-modal').classList.remove('open')}
async function runBatch(){
  const f=document.getElementById('batch-file').files[0];
  if(!f){alert('Choose a CSV');return}
  const form=new FormData();form.append('file',f);
  document.getElementById('batch-result').innerHTML='<span class="spinner"></span>Processing…';
  const r=await fetch('/api/batch/upload',{method:'POST',body:form});
  const data=await r.json();
  document.getElementById('batch-result').innerHTML=
    `<b>${data.count} results</b><br>${data.results.map(x=>x.error?`❌ ${esc(x.error)}`:`${esc(x.place)} · ${esc(x.species)} → ${esc(x.verdict)}`).join('<br>')}`;
}

document.getElementById('btn-share').onclick=()=>{
  if(!lastResult){alert('Analyze first');return}
  const url=`${location.origin}/?lat=${lastResult.lat}&lon=${lastResult.lon}&species=${encodeURIComponent(lastResult.species)}&country=${encodeURIComponent(lastResult.country||'')}`;
  navigator.clipboard.writeText(url).then(()=>setStatus('Share link copied'));
};

document.getElementById('btn-polygon').onclick = async (e) => {
  const btn = e.currentTarget;
  if (!drawControl) {
    const Ctor = (window.MaplibreMeasureControl && window.MaplibreMeasureControl.MaplibreMeasureControl)
              || window.MaplibreMeasureControl;
    if (typeof Ctor !== 'function') {
      alert('Polygon drawing plugin not loaded. Check internet connection.');
      return;
    }
    try {
      drawControl = new Ctor({
        modes: ['polygon', 'rectangle', 'freehand', 'delete-selection'],
        open: true,
        adapterOptions: { coordinatePrecision: 12 },
      });
      map.addControl(drawControl, 'top-left');
      btn.classList.add('on');
      btn.textContent = '📐 Analyze drawn area';
      setStatus('Draw a polygon, then click "Analyze drawn area"');
      return;
    } catch (err) {
      alert('Plugin failed: ' + err.message);
      return;
    }
  }
  let snapshot = [];
  try {
    const instance = (typeof drawControl.getTerraDrawInstance === 'function')
        ? drawControl.getTerraDrawInstance() : null;
    if (instance && typeof instance.getSnapshot === 'function') {
      snapshot = instance.getSnapshot();
    } else if (typeof drawControl.getFeatures === 'function') {
      snapshot = drawControl.getFeatures() || [];
    }
  } catch (err) {
    alert('Could not read polygon: ' + err.message);
    return;
  }
  const polygons = snapshot.filter(f => f?.geometry?.type === 'Polygon');
  if (!polygons.length) { alert('Draw a polygon on the map first.'); return; }
  const ring = polygons[0].geometry.coordinates[0].map(([x, y]) => [
    Math.round(x * 1e9) / 1e9,
    Math.round(y * 1e9) / 1e9,
  ]);
  const species = document.getElementById('species').value.trim() || 'Tectona grandis';
  btn.disabled = true; btn.textContent = '⏳ Analyzing…';
  setStatus('Analyzing polygon…');
  try {
    const r = await fetch('/api/polygon', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ polygon: ring, species, spacing_m: 4, sample_points: 9 }),
    });
    if (!r.ok) throw new Error((await r.json()).detail || 'Polygon analysis failed');
    const data = await r.json();
    const results = document.getElementById('results');
    results.insertAdjacentHTML('afterbegin', `<div class="card">
      <div class="vhead"><div><div class="where">Polygon analysis</div>
      <div class="place">${esc(species)}</div></div>
      <div class="vbadge vb-good">${data.area_ha} ha</div></div>
      <div class="section"><h4 class="accent">📐 Area & planting</h4>
        <div class="row"><span class="k">Total area</span><span class="v">${data.area_ha} ha</span></div>
        <div class="row"><span class="k">Plantable</span><span class="v">${data.plantable_ha} ha (${(data.suitable_ratio*100).toFixed(0)}% suitable)</span></div>
        <div class="row"><span class="k">Trees at ${data.spacing_m}m spacing</span><span class="v accent">~${data.estimated_tree_count.toLocaleString()}</span></div>
        <div class="row"><span class="k">Carbon (20 yr @ 80% survival)</span><span class="v">~${((data.estimated_tree_count*20*0.8*15)/1000).toFixed(1)} t CO₂</span></div>
        <div class="row"><span class="k">Samples analyzed</span><span class="v">${data.samples}</span></div>
      </div></div>`);
    try {
      const instance = drawControl.getTerraDrawInstance?.();
      if (instance && instance.clear) instance.clear();
      else if (drawControl.clear) drawControl.clear();
    } catch (_) {}
    btn.disabled = false; btn.textContent = '📐 Draw polygon';
    btn.classList.remove('on');
    setStatus('Polygon analyzed');
  } catch (err) {
    btn.disabled = false; btn.textContent = '📐 Analyze drawn area';
    alert('Polygon analysis failed: ' + err.message);
  }
};

async function downloadExport(card,type){
  const res=JSON.parse(card.dataset.result);
  const r=await fetch(`/api/export/${type}`,{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify(res)});
  const blob=await r.blob();
  const a=document.createElement('a');a.href=URL.createObjectURL(blob);
  a.download=`reforestai_site.${type}`;a.click();
}

async function shareWhatsApp(card){
  const res = JSON.parse(card.dataset.result);
  try {
    const r = await fetch('/api/export/whatsapp', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(res),
    });
    if (!r.ok) throw new Error('WhatsApp export failed');
    const { wa_url } = await r.json();
    const win = window.open(wa_url, '_blank', 'noopener,noreferrer');
    if (!win) window.location.href = wa_url;
  } catch (err) {
    alert('WhatsApp share failed: ' + err.message);
  }
}

let camStream=null;
async function openCamera(){
  const modal=document.getElementById('cam-modal'),video=document.getElementById('cam-video'),canvas=document.getElementById('cam-canvas');
  video.style.display='block';canvas.style.display='none';
  document.getElementById('cam-metrics').innerHTML='';
  modal.classList.add('open');
  try{
    camStream=await navigator.mediaDevices.getUserMedia({video:{facingMode:{ideal:'environment'},width:{ideal:1280},height:{ideal:720}},audio:false});
    video.srcObject=camStream;
  }catch(e){
    modal.querySelector('p').innerHTML=`<span style="color:var(--bad)">⚠ Camera: ${esc(e.message)}</span><br>Requires localhost or HTTPS.`;
  }
}
function closeCamera(){document.getElementById('cam-modal').classList.remove('open');if(camStream){camStream.getTracks().forEach(t=>t.stop());camStream=null}}
function captureAndAnalyze(){
  const video=document.getElementById('cam-video'),canvas=document.getElementById('cam-canvas');
  const w=video.videoWidth||640,h=video.videoHeight||480;
  canvas.width=w;canvas.height=h;
  const ctx=canvas.getContext('2d');ctx.drawImage(video,0,0,w,h);
  video.style.display='none';canvas.style.display='block';
  const data=ctx.getImageData(0,0,w,h).data;
  let sumExG=0,green=0,bare=0,n=0;
  for(let i=0;i<data.length;i+=16){
    const r=data[i],g=data[i+1],b=data[i+2];
    const exg=2*g-r-b;sumExG+=exg;
    if(exg>25&&g>r&&g>b)green++;
    else if(r>180&&g>180&&b>150&&Math.abs(r-g)<25)bare++;
    n++;
  }
  const meanExG=sumExG/n,gPct=100*green/n,bPct=100*bare/n;
  let v,cls,advice;
  if(gPct>=55&&meanExG>=25){v='HEALTHY';cls='good';advice='Lush vegetation — ideal for planting.'}
  else if(gPct>=25){v='MODERATE';cls='warn';advice='Some vegetation. Consider soil prep.'}
  else if(bPct>=40){v='BARE GROUND';cls='warn';advice='Exposed soil. Good for planting — add compost.'}
  else {v='STRESSED';cls='bad';advice='Little green. Investigate drainage or salinity.'}
  document.getElementById('cam-metrics').innerHTML=`
    <div class="photo-metrics">
      <div class="pm"><div class="k">Vigor</div><div class="v ${cls}">${v}</div></div>
      <div class="pm"><div class="k">Green</div><div class="v">${gPct.toFixed(1)}%</div></div>
      <div class="pm"><div class="k">ExG</div><div class="v">${meanExG.toFixed(1)}</div></div>
    </div>
    <div style="margin-top:10px;padding:9px;background:rgba(0,0,0,.3);border-radius:6px;font-size:11.5px">🔍 ${advice}</div>`;
}

if('serviceWorker' in navigator){
  navigator.serviceWorker.register('/sw.js').catch(()=>{});
}

window.addEventListener('load',()=>{
  const p=new URLSearchParams(location.search);
  if(p.get('place')) document.getElementById('place').value=p.get('place');
  if(p.get('species')) document.getElementById('species').value=p.get('species');
  if(p.get('country')) document.getElementById('country').value=p.get('country');
});
</script>
</body>
</html>
"""

# ==================================================================
# 14. CLI
# ==================================================================
async def cli_run(args):
    svc = ClimateService(ONIService()); engine = VerdictEngine()
    print(f"\n🌳 ReforestAI v5.2 CLI — '{args.species}' at '{args.place}' ({args.country})\n")
    hits = await svc.geocode(args.place, args.country)
    if not hits: print(f"❌ Not found: {args.place}"); await svc.close(); return
    hit = hits[0]; lat, lon = hit["latitude"], hit["longitude"]
    label = ", ".join(filter(None, [hit.get("name"), hit.get("admin1"), hit.get("country")]))
    print(f"📍 {label} ({lat:.4f}, {lon:.4f})\n")
    climate = await svc.climate(lat, lon)
    tol, reason, inferred = infer_tolerance(args.species)
    v = engine.evaluate(args.species, climate, tol, reason, inferred)
    print(f"   Verdict  : {v['verdict']} ({v['score']}% · conf {v['confidence']}%)")
    print(f"   Climate  : {climate['temp_annual_mean_c']}°C · {climate['annual_precip_mm']}mm/yr · {climate['humidity_rel_pct']}% RH")
    print(f"   ONI      : {climate.get('oni_value','—')} · Source: {climate['source']}")
    if inferred: print(f"   ⚠ Inferred: {reason}")
    print("\n   Reasoning:")
    for r in v["reasoning"]: print(f"     • {r}")
    if v["alternatives"]: print("\n   Alternatives:", ", ".join(v["alternatives"]))
    carb = carbon_estimate(args.species, 100, 20)
    print(f"\n   Carbon (100 trees/20yr): {carb['total_tonnes_co2']} t CO₂")
    await svc.close()

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="ReforestAI v5.2")
    ap.add_argument("--cli", action="store_true")
    ap.add_argument("--country", default="India")
    ap.add_argument("--place", default="Tamil Nadu")
    ap.add_argument("--species", default="Tectona grandis")
    a = ap.parse_args()
    if a.cli:
        asyncio.run(cli_run(a))
    else:
        import uvicorn
        print("\n  🌳  ReforestAI v5.2 running at  http://localhost:8000\n")
        uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
