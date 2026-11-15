"""
Scanner de Arbitraje de Mercados en Tiempo Real para Albion Online.
Modo Escucha Total NATS (100% del Tráfico Comercial en Vivo) + Traductor Oficial Completo al Español (ES-ES).
"""

import asyncio
import json
import logging
import os
import re
import urllib.parse
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

from aiohttp import web
import httpx
import nats
from nats.aio.client import Client as NATSClient
import pandas as pd
from dotenv import load_dotenv
from pydantic import BaseModel, Field, field_validator
from telegram import (
    CopyTextButton,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    MenuButtonWebApp,
    Update,
    WebAppInfo,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    PreCheckoutQueryHandler,
    filters,
)

# ==============================================================================
# 1. CONFIGURACIÓN, SERVIDORES Y MAPEO DE CIUDADES
# ==============================================================================
load_dotenv()

logging.basicConfig(
    format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("AlbionArbitrageBot")

SERVERS_CONFIG: Dict[str, Dict[str, str]] = {
    "americas": {
        "name": "🌎 América (Washington)",
        "short_name": "🌎 América",
        "api_url": "https://west.albion-online-data.com/api/v2/stats/prices",
        "nats_url": "nats://public:thenewalbiondata@nats.albion-online-data.com:4222",
    },
    "europe": {
        "name": "🌍 Europa (Ámsterdam)",
        "short_name": "🌍 Europa",
        "api_url": "https://europe.albion-online-data.com/api/v2/stats/prices",
        "nats_url": "nats://public:thenewalbiondata@nats.albion-online-data.com:34222",
    },
    "asia": {
        "name": "🌏 Asia (Singapur)",
        "short_name": "🌏 Asia",
        "api_url": "https://east.albion-online-data.com/api/v2/stats/prices",
        "nats_url": "nats://public:thenewalbiondata@nats.albion-online-data.com:24222",
    },
}

MONITORED_CITIES: List[str] = [
    "Martlock",
    "Lymhurst",
    "Fort Sterling",
    "Thetford",
    "Bridgewatch",
    "Caerleon",
    "Brecilien",
]

# Mapeo de todos los IDs de mercado del protocolo NATS de Albion (incluyendo portales y mercados reales)
NATS_LOCATION_ID_TO_CITY: Dict[int, str] = {
    7: "Thetford",
    301: "Thetford",
    1002: "Lymhurst",
    1301: "Lymhurst",
    2004: "Bridgewatch",
    2301: "Bridgewatch",
    3005: "Caerleon",
    3008: "Martlock",
    3301: "Martlock",
    4002: "Fort Sterling",
    4301: "Fort Sterling",
    5003: "Brecilien",
}

CITY_BADGES: Dict[str, str] = {
    "Martlock": "🏔️ Martlock",
    "Lymhurst": "🌲 Lymhurst",
    "Fort Sterling": "❄️ Fort Sterling",
    "Thetford": "🌿 Thetford",
    "Bridgewatch": "🏜️ Bridgewatch",
    "Caerleon": "☠️ Caerleon",
    "Brecilien": "🌫️ Brecilien",
}

QUALITIES_ES: Dict[int, str] = {
    1: "Normal",
    2: "Bueno",
    3: "Notable",
    4: "Sobresaliente",
    5: "Obra Maestra",
}

# Impuestos de Albion Online (Con cuenta Premium activa)
SETUP_FEE: float = 0.025        # 2.5% Tasa de colocación de orden
TRANSACTION_TAX: float = 0.040  # 4.0% Impuesto de transacción
TOTAL_TAX_RETENTION: float = SETUP_FEE + TRANSACTION_TAX  # 6.5% (0.065)
NET_REVENUE_FACTOR: float = 1.0 - TOTAL_TAX_RETENTION     # 0.935


# ==============================================================================
# 2. TRADUCTOR AUTOMÁTICO AL ESPAÑOL PARA TODOS LOS ÍTEMS DEL JUEGO
# ==============================================================================

class SpanishItemTranslator:
    """
    Traduce automáticamente cualquier ID de ítem de Albion Online al español oficial (ES-ES),
    agregando el ícono de categoría, el Tier (T4-T8) y el nivel de encantamiento (.1 a .4).
    """

    def __init__(self, json_path: str = "items_es.json") -> None:
        self.json_path = Path(json_path)
        self.catalog: Dict[str, str] = {}
        self._load_local_catalog()

    def _load_local_catalog(self) -> None:
        if self.json_path.exists():
            try:
                self.catalog = json.loads(self.json_path.read_text(encoding="utf-8"))
                logger.info(f"Diccionario Español cargado: {len(self.catalog):,} ítems oficiales.")
            except Exception as e:
                logger.warning(f"No se pudo leer {self.json_path}: {e}")

    async def ensure_catalog_loaded(self) -> None:
        """Si items_es.json aún no existe, lo descarga asíncronamente desde ao-bin-dumps."""
        if self.catalog:
            return
        self._load_local_catalog()
        if self.catalog:
            return

        logger.info("Descargando diccionario oficial de ítems en español (ao-bin-dumps)...")
        try:
            async with httpx.AsyncClient(timeout=45.0) as client:
                r = await client.get(
                    "https://raw.githubusercontent.com/ao-data/ao-bin-dumps/master/formatted/items.json"
                )
                r.raise_for_status()
                data = r.json()
                for item in data:
                    uid = item.get("UniqueName")
                    loc = item.get("LocalizedNames") or {}
                    es_name = loc.get("ES-ES") or loc.get("EN-US")
                    if uid and es_name:
                        self.catalog[uid] = es_name
                self.json_path.write_text(
                    json.dumps(self.catalog, ensure_ascii=False), encoding="utf-8"
                )
                logger.info(f"Diccionario Español descargado: {len(self.catalog):,} ítems.")
        except Exception as e:
            logger.warning(f"Fallo al descargar items.json: {e}")

    @staticmethod
    def _pick_category_icon(item_id: str) -> str:
        u = item_id.upper()
        if "_ORE" in u:
            return "⛏️"
        if "_METALBAR" in u:
            return "🔩"
        if "_HIDE" in u:
            return "🦌"
        if "_LEATHER" in u:
            return "🛡️"
        if "_FIBER" in u:
            return "🌾"
        if "_CLOTH" in u:
            return "🧵"
        if "_WOOD" in u:
            return "🪵"
        if "_PLANKS" in u:
            return "🪚"
        if "_ROCK" in u or "_STONEBLOCK" in u:
            return "🪨"
        if "_BAG" in u or "CAPE" in u:
            return "🎒"
        if "_MOUNT" in u or "HORSE" in u or "OX" in u or "MAMMOTH" in u or "DIREBEAR" in u:
            return "🐎"
        if "_MEAL" in u or "_FOOD" in u or "_FISH" in u or "_PIE" in u or "_STEW" in u:
            return "🍖"
        if "_POTION" in u:
            return "🧪"
        if "ARTEFACT" in u or "TOKEN" in u or "SOUL" in u or "RUNE" in u or "RELIC" in u:
            return "⚜️"
        if "_2H_" in u or "_MAIN_" in u or "_OFF_" in u:
            return "⚔️"
        if "_HEAD_" in u or "_ARMOR_" in u or "_SHOES_" in u:
            return "🥋"
        return "📦"

    def translate(self, item_id: str, quality: int = 1) -> str:
        base_id = item_id.split("@")[0]
        enchant = item_id.split("@")[1] if "@" in item_id else None
        if not enchant and "_LEVEL" in base_id:
            parts = base_id.rsplit("_LEVEL", 1)
            if len(parts) == 2 and parts[1].isdigit():
                enchant = parts[1]

        icon = self._pick_category_icon(base_id)
        es_name = self.catalog.get(item_id) or self.catalog.get(base_id) or base_id

        # Detectar el Tier (T1 a T8)
        tier_num = None
        if len(base_id) >= 3 and base_id[0] == "T" and base_id[1].isdigit() and base_id[2] == "_":
            tier_num = base_id[1]

        tier_badge = ""
        if tier_num and enchant:
            tier_badge = f" (T{tier_num}.{enchant} ✨)"
        elif tier_num:
            tier_badge = f" (T{tier_num})"
        elif enchant:
            tier_badge = f" (.{enchant} ✨)"

        quality_badge = ""
        if quality > 1:
            q_name = QUALITIES_ES.get(quality, f"Calidad {quality}")
            quality_badge = f" [{q_name}]"

        return f"{icon} {es_name}{tier_badge}{quality_badge}"


# ==============================================================================
# 3. MODELOS DE VALIDACIÓN DE DATOS (PYDANTIC V2)
# ==============================================================================

class AlbionPriceRecord(BaseModel):
    item_id: str = Field(..., alias="item_id")
    city: str = Field(..., alias="city")
    quality: int = Field(default=1)
    sell_price_min: int = Field(default=0, ge=0)
    sell_price_min_date: datetime
    sell_price_max: int = Field(default=0, ge=0)
    sell_price_max_date: datetime
    buy_price_min: int = Field(default=0, ge=0)
    buy_price_min_date: datetime
    buy_price_max: int = Field(default=0, ge=0)
    buy_price_max_date: datetime

    @field_validator(
        "sell_price_min_date",
        "sell_price_max_date",
        "buy_price_min_date",
        "buy_price_max_date",
        mode="before",
    )
    @classmethod
    def ensure_utc_datetime(cls, value: str | datetime) -> datetime:
        if isinstance(value, str):
            if value.endswith("Z"):
                value = value[:-1] + "+00:00"
            dt = datetime.fromisoformat(value)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)
        raise ValueError(f"Formato de fecha inválido: {value}")


class ScannerConfig(BaseModel):
    server_key: str = Field(default=os.getenv("ALBION_SERVER", "americas"))
    min_roi: float = Field(default=float(os.getenv("DEFAULT_MIN_ROI", "12.0")), ge=0.1)
    min_profit: int = Field(default=int(os.getenv("DEFAULT_MIN_PROFIT", "1000")), ge=1)
    max_age_minutes: int = Field(default=int(os.getenv("MAX_DATA_AGE_MINUTES", "10")), ge=1)
    scan_interval: int = Field(default=int(os.getenv("SCAN_INTERVAL_SECONDS", "60")), ge=10)
    strategy: str = Field(default=os.getenv("ARBITRAGE_STRATEGY", "instant_sell"))
    is_running: bool = Field(default=True)


# ==============================================================================
# 4. GESTOR DE ÍTEMS BASE (WHITELIST)
# ==============================================================================

class ItemWhitelistManager:
    def __init__(self, filepath: str = "items_whitelist.txt") -> None:
        self.filepath = Path(filepath)

    def load_items(self) -> List[str]:
        if not self.filepath.exists():
            self._create_default_whitelist()

        items: List[str] = []
        with self.filepath.open("r", encoding="utf-8") as f:
            for line in f:
                clean = line.strip()
                if clean and not clean.startswith("#"):
                    items.append(clean)
        return items

    def _create_default_whitelist(self) -> None:
        default_items = [
            "T4_ORE", "T5_ORE", "T6_ORE", "T7_ORE", "T8_ORE",
            "T4_METALBAR", "T5_METALBAR", "T6_METALBAR", "T7_METALBAR", "T8_METALBAR",
            "T4_LEATHER", "T5_LEATHER", "T6_LEATHER", "T7_LEATHER", "T8_LEATHER",
            "T4_CLOTH", "T5_CLOTH", "T6_CLOTH", "T7_CLOTH", "T8_CLOTH",
            "T4_PLANKS", "T5_PLANKS", "T6_PLANKS", "T7_PLANKS", "T8_PLANKS",
        ]
        self.filepath.write_text("\n".join(default_items) + "\n", encoding="utf-8")


# ==============================================================================
# 5. MOTOR DE ESCUCHA TOTAL NATS EN TIEMPO REAL (100% DEL TRÁFICO)
# ==============================================================================

class AlbionRealtimeNATSStream:
    """
    Modo Escucha Total: Captura el 100% de las órdenes de mercado e historiales de precios
    que circulan por el servidor NATS en vivo, sin descartar ningún ítem comercializable.
    Además mantiene una cola con los ítems más calientes vistos en los últimos minutos
    para cruzarlos contra las 7 ciudades.
    """

    def __init__(self) -> None:
        self.nc: Optional[NATSClient] = None
        self.current_server_key: str = "americas"
        # Libro de órdenes en vivo: (item_id, city, quality) -> {"sell_min": int, "sell_date": dt, "buy_max": int, "buy_date": dt}
        self.live_book: Dict[Tuple[str, str, int], Dict[str, Any]] = {}
        # Cola de los últimos ítems únicos vistos en vivo por NATS (para enriquecer el cruce entre ciudades)
        self.recent_live_items: Deque[str] = deque(maxlen=160)
        self.recent_live_set: Set[str] = set()
        self.messages_received: int = 0
        self.orders_processed: int = 0
        self.connected_since: Optional[datetime] = None
        self._lock = asyncio.Lock()

    @property
    def is_connected(self) -> bool:
        return self.nc is not None and self.nc.is_connected

    async def connect_to_server(self, server_key: str) -> None:
        async with self._lock:
            if self.nc is not None:
                try:
                    await self.nc.close()
                except Exception:
                    pass
                self.nc = None

            self.current_server_key = server_key
            self.live_book.clear()
            self.recent_live_items.clear()
            self.recent_live_set.clear()
            self.messages_received = 0
            self.orders_processed = 0

            nats_url = SERVERS_CONFIG[server_key]["nats_url"]
            server_name = SERVERS_CONFIG[server_key]["short_name"]

            try:
                self.nc = await nats.connect(
                    servers=[nats_url],
                    connect_timeout=8,
                    reconnect_time_wait=3,
                    max_reconnect_attempts=-1,
                )
                # Suscripción total a órdenes deduplicadas e ingesta en bruto
                await self.nc.subscribe("marketorders.deduped", cb=self._on_market_order)
                await self.nc.subscribe("marketorders.ingest", cb=self._on_market_order)
                self.connected_since = datetime.now(timezone.utc)
                logger.info(f"⚡ Modo Escucha Total NATS activo en {server_name} ({nats_url})")
            except Exception as e:
                logger.warning(f"No se pudo conectar a NATS ({server_name}): {e}.")

    def _record_hot_item(self, item_id: str) -> None:
        if item_id not in self.recent_live_set:
            if len(self.recent_live_items) == self.recent_live_items.maxlen:
                oldest = self.recent_live_items.popleft()
                self.recent_live_set.discard(oldest)
            self.recent_live_items.append(item_id)
            self.recent_live_set.add(item_id)

    async def _on_market_order(self, msg: Any) -> None:
        """Procesa cada orden de cualquier ítem del juego en el instante en que un jugador la ve."""
        try:
            self.messages_received += 1
            data = json.loads(msg.data.decode("utf-8"))

            if isinstance(data, dict) and "Orders" in data:
                orders = data["Orders"]
            elif isinstance(data, list):
                orders = data
            else:
                orders = [data]

            now = datetime.now(timezone.utc)

            for order in orders:
                if not isinstance(order, dict):
                    continue

                loc_raw = order.get("LocationId")
                try:
                    loc_id = int(loc_raw)
                except (TypeError, ValueError):
                    continue

                city = NATS_LOCATION_ID_TO_CITY.get(loc_id)
                if not city:
                    continue

                item_id = str(order.get("ItemTypeId") or order.get("ItemGroupTypeId") or "").strip()
                if not item_id or "NON_TRADABLE" in item_id:
                    continue

                quality = int(order.get("QualityLevel", 1))
                if quality < 1 or quality > 5:
                    quality = 1

                raw_price = int(order.get("UnitPriceSilver", 0))
                if raw_price <= 0:
                    continue
                # Normalización automática de precisión interna de Albion (x10000) si aplica
                price = raw_price // 10000 if raw_price > 500_000_000 else raw_price
                if price <= 0:
                    continue

                auction_type = str(order.get("AuctionType", "")).lower()
                key = (item_id, city, quality)
                entry = self.live_book.get(key)
                if entry is None:
                    entry = {
                        "sell_min": 0,
                        "sell_date": datetime(2000, 1, 1, tzinfo=timezone.utc),
                        "buy_max": 0,
                        "buy_date": datetime(2000, 1, 1, tzinfo=timezone.utc),
                    }
                    self.live_book[key] = entry

                if auction_type == "offer":
                    age_sec = (now - entry["sell_date"]).total_seconds()
                    if entry["sell_min"] == 0 or age_sec > 180 or price <= entry["sell_min"]:
                        entry["sell_min"] = price
                        entry["sell_date"] = now
                        self.orders_processed += 1
                        self._record_hot_item(item_id)
                elif auction_type == "request":
                    age_sec = (now - entry["buy_date"]).total_seconds()
                    if entry["buy_max"] == 0 or age_sec > 180 or price >= entry["buy_max"]:
                        entry["buy_max"] = price
                        entry["buy_date"] = now
                        self.orders_processed += 1
                        self._record_hot_item(item_id)

        except Exception as e:
            logger.debug(f"Error en paquete NATS: {e}")

    def get_dynamic_items_to_query(self, base_items: List[str], max_total: int = 160) -> List[str]:
        """
        Combina los ítems recién detectados en vivo por NATS (0-60s de antigüedad)
        con la lista base de transporte para cruzar sus precios contra las 7 ciudades.
        """
        hot_items = list(reversed(self.recent_live_items))
        combined: List[str] = []
        seen: Set[str] = set()

        for item in hot_items + base_items:
            if item not in seen:
                seen.add(item)
                combined.append(item)
                if len(combined) >= max_total:
                    break
        return combined

    def merge_with_rest_records(self, rest_records: List[AlbionPriceRecord]) -> List[AlbionPriceRecord]:
        """Fusiona el libro de órdenes NATS en tiempo real con la instantánea de las 7 ciudades."""
        by_key: Dict[Tuple[str, str, int], AlbionPriceRecord] = {
            (r.item_id, r.city, r.quality): r for r in rest_records
        }

        for (item_id, city, quality), live in list(self.live_book.items()):
            rec = by_key.get((item_id, city, quality))
            if rec is None:
                rec = AlbionPriceRecord(
                    item_id=item_id,
                    city=city,
                    quality=quality,
                    sell_price_min=live["sell_min"],
                    sell_price_min_date=live["sell_date"],
                    sell_price_max=live["sell_min"],
                    sell_price_max_date=live["sell_date"],
                    buy_price_min=live["buy_max"],
                    buy_price_min_date=live["buy_date"],
                    buy_price_max=live["buy_max"],
                    buy_price_max_date=live["buy_date"],
                )
                by_key[(item_id, city, quality)] = rec
            else:
                if live["sell_min"] > 0 and live["sell_date"] > rec.sell_price_min_date:
                    rec.sell_price_min = live["sell_min"]
                    rec.sell_price_min_date = live["sell_date"]
                if live["buy_max"] > 0 and live["buy_date"] > rec.buy_price_max_date:
                    rec.buy_price_max = live["buy_max"]
                    rec.buy_price_max_date = live["buy_date"]

        return list(by_key.values())

    async def close(self) -> None:
        if self.nc is not None:
            try:
                await self.nc.close()
            except Exception:
                pass


# ==============================================================================
# 6. CLIENTE HTTP ASÍNCRONO MULTI-SERVIDOR CON CONSULTA PARALELA
# ==============================================================================

class AlbionAPIClient:
    def __init__(self, max_retries: int = 3, chunk_size: int = 45) -> None:
        self.max_retries = max_retries
        self.chunk_size = chunk_size
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(12.0, connect=5.0),
            headers={"Accept": "application/json", "User-Agent": "AlbionArbitrageScanner/3.0"},
        )

    async def close(self) -> None:
        await self.client.aclose()

    async def _fetch_chunk_with_backoff(
        self, base_url: str, items_chunk: List[str], cities: List[str]
    ) -> List[AlbionPriceRecord]:
        items_str = ",".join(items_chunk)
        cities_str = ",".join(cities)
        url = f"{base_url.rstrip('/')}/{items_str}"
        params = {"locations": cities_str}

        base_delay = 1.2
        for attempt in range(1, self.max_retries + 1):
            try:
                response = await self.client.get(url, params=params)
                response.raise_for_status()
                raw_data = response.json()

                valid_records: List[AlbionPriceRecord] = []
                for entry in raw_data:
                    try:
                        valid_records.append(AlbionPriceRecord.model_validate(entry))
                    except Exception:
                        pass
                return valid_records

            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                if status == 429 or status >= 500:
                    await asyncio.sleep(base_delay * (2 ** (attempt - 1)))
                else:
                    break
            except (httpx.RequestError, httpx.TimeoutException):
                await asyncio.sleep(base_delay * (2 ** (attempt - 1)))

        return []

    async def fetch_all_prices(
        self, server_key: str, items: List[str], cities: List[str]
    ) -> List[AlbionPriceRecord]:
        base_url = SERVERS_CONFIG[server_key]["api_url"]
        chunks = [items[i : i + self.chunk_size] for i in range(0, len(items), self.chunk_size)]

        # Ejecutar lotes en paralelo controlado (máx 4 peticiones concurrentes) para respuesta <2 seg
        sem = asyncio.Semaphore(4)

        async def _bounded_fetch(chunk: List[str]) -> List[AlbionPriceRecord]:
            async with sem:
                return await self._fetch_chunk_with_backoff(base_url, chunk, cities)

        results = await asyncio.gather(*[_bounded_fetch(c) for c in chunks])
        all_records: List[AlbionPriceRecord] = []
        for r in results:
            all_records.extend(r)
        return all_records


# ==============================================================================
# 7. MOTOR MATEMÁTICO Y ANALÍTICO DE ARBITRAJE (PANDAS VECTORIZADO)
# ==============================================================================

class ArbitrageEngine:
    @staticmethod
    def analyze_opportunities(
        records: List[AlbionPriceRecord],
        config: ScannerConfig,
    ) -> pd.DataFrame:
        if not records:
            return pd.DataFrame()

        now_utc = datetime.now(timezone.utc)
        df = pd.DataFrame([r.model_dump() for r in records])

        df["sell_age_min"] = (now_utc - df["sell_price_min_date"]).dt.total_seconds() / 60.0
        df["buy_age_min"] = (now_utc - df["buy_price_max_date"]).dt.total_seconds() / 60.0

        origin_mask = (
            (df["sell_price_min"] > 0)
            & (df["sell_age_min"] <= config.max_age_minutes)
            & (df["sell_age_min"] >= 0)
        )
        df_origin = df.loc[
            origin_mask,
            ["item_id", "city", "quality", "sell_price_min", "sell_age_min"],
        ].rename(
            columns={
                "city": "origin_city",
                "sell_price_min": "buy_cost",
                "sell_age_min": "origin_age_min",
            }
        )

        if config.strategy == "sell_order":
            dest_mask = (
                (df["sell_price_min"] > 0)
                & (df["sell_age_min"] <= config.max_age_minutes)
                & (df["sell_age_min"] >= 0)
            )
            df_dest = df.loc[
                dest_mask,
                ["item_id", "city", "quality", "sell_price_min", "sell_age_min"],
            ].rename(
                columns={
                    "city": "dest_city",
                    "sell_price_min": "gross_sell_price",
                    "sell_age_min": "dest_age_min",
                }
            )
        else:
            dest_mask = (
                (df["buy_price_max"] > 0)
                & (df["buy_age_min"] <= config.max_age_minutes)
                & (df["buy_age_min"] >= 0)
            )
            df_dest = df.loc[
                dest_mask,
                ["item_id", "city", "quality", "buy_price_max", "buy_age_min"],
            ].rename(
                columns={
                    "city": "dest_city",
                    "buy_price_max": "gross_sell_price",
                    "buy_age_min": "dest_age_min",
                }
            )

        if df_origin.empty or df_dest.empty:
            return pd.DataFrame()

        merged = pd.merge(df_origin, df_dest, on=["item_id", "quality"], how="inner")
        merged = merged[merged["origin_city"] != merged["dest_city"]].copy()

        if merged.empty:
            return pd.DataFrame()

        merged["net_revenue"] = merged["gross_sell_price"] * NET_REVENUE_FACTOR
        merged["net_profit"] = (merged["net_revenue"] - merged["buy_cost"]).round().astype(int)
        merged["roi_pct"] = ((merged["net_profit"] / merged["buy_cost"]) * 100.0).round(2)

        # Descartar anomalías/manipulaciones absurdas de órdenes fantasma (>500% ROI en items sin liquidez)
        filtered = merged[
            (merged["net_profit"] >= config.min_profit)
            & (merged["roi_pct"] >= config.min_roi)
            & (merged["roi_pct"] <= 400.0)
        ].copy()

        if filtered.empty:
            return pd.DataFrame()

        filtered["max_route_age"] = filtered[["origin_age_min", "dest_age_min"]].max(axis=1)
        filtered.sort_values(
            by=["max_route_age", "roi_pct", "net_profit"],
            ascending=[True, False, False],
            inplace=True,
        )
        filtered.reset_index(drop=True, inplace=True)
        return filtered

    @staticmethod
    def analyze_both_strategies(
        records: List[AlbionPriceRecord],
        config: ScannerConfig,
    ) -> pd.DataFrame:
        """
        Analiza tanto Venta Instantánea (contra Orden de Compra) como Orden de Venta (contra Sell Min)
        y garantiza devolver siempre las rutas más frescas y rentables sin quedarse vacío.
        """
        cfg_instant = ScannerConfig(
            server_key=config.server_key,
            min_roi=config.min_roi,
            min_profit=config.min_profit,
            max_age_minutes=config.max_age_minutes,
            strategy="instant_sell",
        )
        df_instant = ArbitrageEngine.analyze_opportunities(records, cfg_instant)
        if not df_instant.empty:
            df_instant["sale_mode"] = "⚡ Venta Directa"

        cfg_order = ScannerConfig(
            server_key=config.server_key,
            min_roi=config.min_roi,
            min_profit=config.min_profit,
            max_age_minutes=config.max_age_minutes,
            strategy="sell_order",
        )
        df_order = ArbitrageEngine.analyze_opportunities(records, cfg_order)
        if not df_order.empty:
            df_order["sale_mode"] = "📋 Orden de Venta"

        frames = [f for f in (df_instant, df_order) if not f.empty]
        if not frames:
            return pd.DataFrame()

        combined = pd.concat(frames, ignore_index=True)
        combined.sort_values(
            by=["max_route_age", "roi_pct", "net_profit"],
            ascending=[True, False, False],
            inplace=True,
        )
        combined.drop_duplicates(
            subset=["item_id", "quality", "origin_city", "dest_city"],
            keep="first",
            inplace=True,
        )
        combined.reset_index(drop=True, inplace=True)
        return combined


# ==============================================================================
# 8. DISEÑO VISUAL DE INTERFAZ Y TARJETAS (HTML RICO DE TELEGRAM)
# ==============================================================================

def format_city(city: str) -> str:
    return CITY_BADGES.get(city, f"🏰 {city}")


def format_age_badge(age_minutes: float) -> str:
    seconds = max(1, int(round(age_minutes * 60)))
    if seconds < 60:
        return f"⚡ En vivo hace {seconds} seg"
    mins = int(round(age_minutes))
    if mins <= 2:
        return f"🟢 hace {mins} min"
    return f"hace {mins} min"


def format_opportunity_card(
    row: pd.Series, translator: SpanishItemTranslator, rank: Optional[int] = None
) -> str:
    item_code = str(row["item_id"])
    quality = int(row.get("quality", 1))
    spanish_name = translator.translate(item_code, quality)
    origin_city = format_city(str(row["origin_city"]))
    dest_city = format_city(str(row["dest_city"]))
    buy_cost = int(row["buy_cost"])
    sell_price = int(row["gross_sell_price"])
    net_profit = int(row["net_profit"])
    roi_pct = float(row["roi_pct"])
    origin_age_badge = format_age_badge(float(row["origin_age_min"]))
    dest_age_badge = format_age_badge(float(row["dest_age_min"]))
    sale_mode = str(row.get("sale_mode", "⚡ Venta Directa"))

    medals = {1: "🥇", 2: "🥈", 3: "🥉", 4: "4️⃣", 5: "5️⃣"}
    header_badge = f"{medals.get(rank, '🚨')} " if rank else "🚨 <b>¡OPORTUNIDAD EN TIEMPO REAL!</b>\n\n"

    return (
        f"{header_badge}<b>{spanish_name}</b>\n"
        f"<blockquote>"
        f"🛒 <b>Comprar en:</b> {origin_city}\n"
        f"   └ 💵 Precio: <b>{buy_cost:,} silver</b> <i>({origin_age_badge})</i>\n\n"
        f"💰 <b>Vender en:</b> {dest_city} <i>({sale_mode})</i>\n"
        f"   └ 💵 Precio: <b>{sell_price:,} silver</b> <i>({dest_age_badge})</i>\n\n"
        f"📈 <b>Ganancia Neta:</b> <b>+{net_profit:,} silver</b> / unidad\n"
        f"🔥 <b>Rentabilidad (ROI):</b> <b>{roi_pct:.1f}%</b> <i>(Impuestos 6.5% desc.)</i>"
        f"</blockquote>"
    )


def build_server_selection_keyboard(active_server: str) -> InlineKeyboardMarkup:
    buttons = []
    for key, info in SERVERS_CONFIG.items():
        check = "✅ " if key == active_server else ""
        buttons.append(
            [InlineKeyboardButton(f"{check}{info['name']}", callback_data=f"set_server:{key}")]
        )
    buttons.append(
        [InlineKeyboardButton("🎁 Ganar VIP Gratis (Invitar Amigos)", callback_data="open_referrals")]
    )
    return InlineKeyboardMarkup(buttons)


# ==============================================================================
# 8.5 MOTOR DE PUBLICIDAD ADMINISTRABLE Y MONETIZACIÓN (MONETAG / DIRECT LINK)
# ==============================================================================

class AdManager:
    """
    Gestiona tanto la Mini App Web con el SDK de Monetag (Telegram Mini App SDK)
    como el bloque patrocinado y estadísticas de aperturas/impresiones.
    """

    def __init__(self, config_path: str = "ads_config.json") -> None:
        self.config_path = Path(config_path)
        self.data: Dict[str, Any] = {
            "enabled": True,
            "monetag_zone_id": "",
            "ad_text": "🔥 <b>PATROCINADO:</b> ¿Quieres multiplicar tu Silver y jugar con 0 lag en Albion Online? Descubre la herramienta recomendada para mercaderes:",
            "button_text": "🎁 Ver Oferta / Herramienta VIP",
            "button_url": "https://albiononline.com",
            "impressions": 0,
            "webapp_opens": 0,
            "users_seen": [],
        }
        self._load()

    def _load(self) -> None:
        if self.config_path.exists():
            try:
                loaded = json.loads(self.config_path.read_text(encoding="utf-8"))
                self.data.update(loaded)
            except Exception as e:
                logger.warning(f"Error leyendo {self.config_path}: {e}")
        else:
            self._save()

    def _save(self) -> None:
        try:
            self.config_path.write_text(
                json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        except Exception as e:
            logger.warning(f"Error guardando {self.config_path}: {e}")

    def record_impression(self, user_id: Optional[int]) -> None:
        if not self.data.get("enabled", True):
            return
        self.data["impressions"] = int(self.data.get("impressions", 0)) + 1
        if user_id:
            seen = self.data.setdefault("users_seen", [])
            if user_id not in seen:
                seen.append(user_id)
        self._save()

    def record_webapp_open(self) -> None:
        self.data["webapp_opens"] = int(self.data.get("webapp_opens", 0)) + 1
        self._save()

    def set_monetag_zone(self, raw_input: str) -> str:
        """Extrae automáticamente el ID de zona numérico o guarda el script de Monetag."""
        match = re.search(r"data-zone=['\"]?(\d+)['\"]?", raw_input)
        if match:
            zone_id = match.group(1)
        else:
            digits = re.findall(r"\d{5,12}", raw_input)
            zone_id = digits[0] if digits else raw_input.strip()
        self.data["monetag_zone_id"] = zone_id
        self._save()
        return zone_id

    def build_ad_footer(self) -> str:
        if not self.data.get("enabled", True):
            return ""
        ad_text = self.data.get("ad_text", "").strip()
        if not ad_text:
            return ""
        return (
            "\n\n━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📢 {ad_text}"
        )

    def build_scan_keyboard(self, webapp_url: Optional[str]) -> Optional[InlineKeyboardMarkup]:
        rows: List[List[InlineKeyboardButton]] = []
        if webapp_url and webapp_url.startswith("https://"):
            rows.append(
                [
                    InlineKeyboardButton(
                        "🚀 Abrir Radar Interactivo en Vivo (Mini App)",
                        web_app=WebAppInfo(url=webapp_url),
                    )
                ]
            )
        if self.data.get("enabled", True):
            btn_text = str(self.data.get("button_text", "")).strip()
            btn_url = str(self.data.get("button_url", "")).strip()
            if btn_text and btn_url.startswith(("http://", "https://")):
                rows.append([InlineKeyboardButton(btn_text, url=btn_url)])
        return InlineKeyboardMarkup(rows) if rows else None

    def update_ad(self, ad_text: str, button_text: str, button_url: str) -> None:
        self.data["enabled"] = True
        self.data["ad_text"] = ad_text.strip()
        self.data["button_text"] = button_text.strip()
        self.data["button_url"] = button_url.strip()
        self._save()

    def set_enabled(self, enabled: bool) -> None:
        self.data["enabled"] = enabled
        self._save()


# ==============================================================================
# 8.6 GESTOR DE MEMBRESÍAS VIP CON TELEGRAM STARS (XTR) Y SIN PUBLICIDAD
# ==============================================================================

class VipManager:
    """
    Gestiona los miembros VIP que pagaron con Telegram Stars (o activados manualmente),
    así como el Sistema Viral de Referidos (Invita a 3 amigos = +7 días VIP sin publicidad).
    """

    def __init__(self, filepath: str = "vip_users.json") -> None:
        self.filepath = Path(filepath)
        self.data: Dict[str, Any] = {
            "vip_price_stars": 100,
            "vip_days": 30,
            "referral_target": 3,
            "referral_reward_days": 7,
            "vip_members": {},  # str(user_id) -> ISO datetime string de expiración
            "known_users": [],  # lista de str(user_id) que ya iniciaron el bot
            "referred_by": {},  # str(new_user_id) -> str(referrer_id)
            "referrals_count": {},  # str(referrer_id) -> int (cantidad de amigos invitados)
        }
        self._load()

    def _load(self) -> None:
        if self.filepath.exists():
            try:
                loaded = json.loads(self.filepath.read_text(encoding="utf-8"))
                self.data.update(loaded)
            except Exception as e:
                logger.warning(f"Error leyendo {self.filepath}: {e}")
        else:
            self._save()

    def _save(self) -> None:
        try:
            self.filepath.write_text(
                json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        except Exception as e:
            logger.warning(f"Error guardando {self.filepath}: {e}")

    @property
    def price_stars(self) -> int:
        return int(self.data.get("vip_price_stars", 100))

    @property
    def vip_days(self) -> int:
        return int(self.data.get("vip_days", 30))

    @property
    def referral_target(self) -> int:
        return max(1, int(self.data.get("referral_target", 3)))

    @property
    def referral_reward_days(self) -> int:
        return max(1, int(self.data.get("referral_reward_days", 7)))

    def set_price_stars(self, stars: int) -> None:
        self.data["vip_price_stars"] = max(1, int(stars))
        self._save()

    def is_vip(self, user_id: Optional[int | str]) -> bool:
        if not user_id:
            return False
        uid = str(user_id)
        exp_str = self.data.get("vip_members", {}).get(uid)
        if not exp_str:
            return False
        try:
            exp_dt = datetime.fromisoformat(exp_str)
            if exp_dt.tzinfo is None:
                exp_dt = exp_dt.replace(tzinfo=timezone.utc)
            return exp_dt > datetime.now(timezone.utc)
        except Exception:
            return False

    def days_left(self, user_id: Optional[int | str]) -> int:
        if not user_id:
            return 0
        uid = str(user_id)
        exp_str = self.data.get("vip_members", {}).get(uid)
        if not exp_str:
            return 0
        try:
            exp_dt = datetime.fromisoformat(exp_str)
            if exp_dt.tzinfo is None:
                exp_dt = exp_dt.replace(tzinfo=timezone.utc)
            delta = exp_dt - datetime.now(timezone.utc)
            return max(0, int(delta.total_seconds() // 86400) + 1) if delta.total_seconds() > 0 else 0
        except Exception:
            return 0

    def activate_vip(self, user_id: int | str, days: Optional[int] = None) -> datetime:
        uid = str(user_id)
        add_days = days if days is not None else self.vip_days
        now = datetime.now(timezone.utc)
        members = self.data.setdefault("vip_members", {})
        current_exp_str = members.get(uid)
        base_dt = now
        if current_exp_str:
            try:
                cur_dt = datetime.fromisoformat(current_exp_str)
                if cur_dt.tzinfo is None:
                    cur_dt = cur_dt.replace(tzinfo=timezone.utc)
                if cur_dt > now:
                    base_dt = cur_dt
            except Exception:
                pass
        new_exp = base_dt + timedelta(days=add_days)
        members[uid] = new_exp.isoformat()
        self._save()
        return new_exp

    def get_referral_stats(self, user_id: Optional[int | str]) -> Dict[str, Any]:
        uid = str(user_id) if user_id else "0"
        counts = self.data.setdefault("referrals_count", {})
        total_invited = int(counts.get(uid, 0))
        target = self.referral_target
        reward_days = self.referral_reward_days
        cycle_progress = total_invited % target
        remaining = target - cycle_progress
        return {
            "total_invited": total_invited,
            "cycle_progress": cycle_progress,
            "remaining": remaining,
            "target": target,
            "reward_days": reward_days,
        }

    def register_user_and_process_referral(
        self, new_user_id: int | str, referrer_id: Optional[str] = None
    ) -> Tuple[bool, bool, int, int, Optional[datetime]]:
        """
        Registra al usuario y si viene por un link de referido válido (?start=ref_XXXX)
        siendo usuario nuevo, suma +1 al invitador y otorga VIP automático al llegar a la meta.
        Retorna: (es_referido_valido, desbloqueo_premio, progreso_ciclo, total_invitados, fecha_exp_vip)
        """
        uid = str(new_user_id).strip()
        ref_uid = str(referrer_id).strip() if referrer_id else ""
        known = self.data.setdefault("known_users", [])
        referred_by = self.data.setdefault("referred_by", {})
        counts = self.data.setdefault("referrals_count", {})

        is_first_time = uid not in known
        if is_first_time:
            known.append(uid)

        # Validar que sea primera vez, que tenga invitador válido y que no se auto-invite
        if (
            is_first_time
            and ref_uid
            and ref_uid.isdigit()
            and ref_uid != uid
            and uid not in referred_by
        ):
            referred_by[uid] = ref_uid
            new_total = int(counts.get(ref_uid, 0)) + 1
            counts[ref_uid] = new_total
            target = self.referral_target
            cycle_progress = new_total % target
            if cycle_progress == 0:
                exp_dt = self.activate_vip(ref_uid, self.referral_reward_days)
                self._save()
                return True, True, target, new_total, exp_dt
            self._save()
            return True, False, cycle_progress, new_total, None

        if is_first_time:
            self._save()
        return False, False, 0, int(counts.get(ref_uid, 0)) if ref_uid else 0, None


class AlbionTelegramBot:
    def __init__(
        self,
        token: str,
        chat_id: str,
        api_client: AlbionAPIClient,
        nats_stream: AlbionRealtimeNATSStream,
        whitelist_manager: ItemWhitelistManager,
        translator: SpanishItemTranslator,
        ad_manager: AdManager,
        vip_manager: VipManager,
        config: ScannerConfig,
    ) -> None:
        self.token = token
        self.chat_id = chat_id
        self.api_client = api_client
        self.nats_stream = nats_stream
        self.whitelist_manager = whitelist_manager
        self.translator = translator
        self.ad_manager = ad_manager
        self.vip_manager = vip_manager
        self.config = config
        self.app_ref: Optional[Application] = None
        self.background_task: Optional[asyncio.Task] = None
        self.web_runner: Optional[web.AppRunner] = None
        self.tunnel_proc: Optional[asyncio.subprocess.Process] = None
        self.webapp_url: Optional[str] = None
        self.notified_cache: Dict[Tuple[str, str, str, str], datetime] = {}

    async def _handle_webapp_index(self, request: web.Request) -> web.Response:
        """Sirve la Mini App HTML inyectando dinámicamente el SDK de Monetag configurado."""
        self.ad_manager.record_webapp_open()
        html_path = Path("webapp.html")
        if not html_path.exists():
            return web.Response(text="Mini App no encontrada", status=404)

        raw_html = html_path.read_text(encoding="utf-8")
        zone_id = str(self.ad_manager.data.get("monetag_zone_id", "")).strip()
        if zone_id:
            script_tag = (
                f"<script src='//libtl.com/sdk.js' data-zone='{zone_id}' "
                f"data-sdk='show_{zone_id}'></script>"
            )
            raw_html = raw_html.replace("__MONETAG_SCRIPT_TAG__", script_tag)
            raw_html = raw_html.replace("__MONETAG_ZONE_ID__", zone_id)
        else:
            raw_html = raw_html.replace("__MONETAG_SCRIPT_TAG__", "")
            raw_html = raw_html.replace("__MONETAG_ZONE_ID__", "NONE")

        return web.Response(text=raw_html, content_type="text/html", charset="utf-8")

    async def _handle_webapp_api_scan(self, request: web.Request) -> web.Response:
        """Endpoint JSON consumido por la Mini App para mostrar el Top 10 en vivo."""
        base_items = self.whitelist_manager.load_items()
        dynamic_items = self.nats_stream.get_dynamic_items_to_query(base_items, max_total=160)
        rest_records = await self.api_client.fetch_all_prices(
            self.config.server_key, dynamic_items, MONITORED_CITIES
        )
        records = self.nats_stream.merge_with_rest_records(rest_records)
        df_opps = ArbitrageEngine.analyze_both_strategies(records, self.config)

        if df_opps.empty:
            for fallback_age in (30, 60, 120, 240):
                relaxed_cfg = ScannerConfig(
                    server_key=self.config.server_key,
                    min_roi=1.0,
                    min_profit=5,
                    max_age_minutes=fallback_age,
                    strategy=self.config.strategy,
                )
                df_opps = ArbitrageEngine.analyze_both_strategies(records, relaxed_cfg)
                if len(df_opps) >= 5:
                    break

        opps_list = []
        if not df_opps.empty:
            for _, row in df_opps.head(10).iterrows():
                opps_list.append(
                    {
                        "spanish_name": self.translator.translate(
                            str(row["item_id"]), int(row.get("quality", 1))
                        ),
                        "origin_city": format_city(str(row["origin_city"])),
                        "dest_city": format_city(str(row["dest_city"])),
                        "buy_cost": int(row["buy_cost"]),
                        "sell_price": int(row["gross_sell_price"]),
                        "net_profit": int(row["net_profit"]),
                        "roi_pct": round(float(row["roi_pct"]), 1),
                        "origin_age": format_age_badge(float(row["origin_age_min"])),
                        "dest_age": format_age_badge(float(row["dest_age_min"])),
                        "sale_mode": str(row.get("sale_mode", "⚡ Venta Directa")),
                    }
                )

        user_id_param = request.query.get("user_id", "").strip()
        user_is_vip = self.vip_manager.is_vip(user_id_param)
        user_days_left = self.vip_manager.days_left(user_id_param)
        ref_stats = self.vip_manager.get_referral_stats(user_id_param)

        payload = {
            "server_name": SERVERS_CONFIG[self.config.server_key]["short_name"],
            "nats_orders": self.nats_stream.orders_processed,
            "hot_items": len(self.nats_stream.recent_live_items),
            "opportunities": opps_list,
            "vip": {
                "is_vip": user_is_vip,
                "days_left": user_days_left,
                "price_stars": self.vip_manager.price_stars,
                "referrals": ref_stats,
            },
            "ad": {
                "enabled": bool(self.ad_manager.data.get("enabled", True)) and not user_is_vip,
                "ad_text": re.sub(r"<[^>]+>", "", str(self.ad_manager.data.get("ad_text", ""))),
                "button_text": str(self.ad_manager.data.get("button_text", "")),
                "button_url": str(self.ad_manager.data.get("button_url", "")),
            },
        }
        return web.json_response(payload)

    async def _handle_webapp_create_vip_invoice(self, request: web.Request) -> web.Response:
        """Crea un enlace de factura oficial de Telegram Stars (XTR) para abrir dentro de la Mini App."""
        user_id_param = request.query.get("user_id", "").strip() or "0"
        stars = self.vip_manager.price_stars
        days = self.vip_manager.vip_days
        try:
            if not self.app_ref:
                raise RuntimeError("Application no inicializada")
            invoice_link = await self.app_ref.bot.create_invoice_link(
                title=f"👑 Membresía Mercader VIP ({days} Días)",
                description=f"Navega {days} días en el Radar y Mini App con 0% publicidad de Monetag y acceso instantáneo.",
                payload=f"vip_stars_{user_id_param}_{days}",
                provider_token="",  # Vacío para Telegram Stars (XTR)
                currency="XTR",
                prices=[LabeledPrice(label=f"Membresía VIP ({days} Días)", amount=stars)],
            )
            return web.json_response({"ok": True, "invoice_link": invoice_link, "price_stars": stars})
        except Exception as e:
            logger.error(f"Error creando invoice link de Stars: {e}")
            return web.json_response({"ok": False, "error": str(e)}, status=500)

    async def _start_miniapp_server_and_tunnel(self, app: Application) -> None:
        """
        Inicia el servidor aiohttp de la Mini App:
        - En la nube (Render.com): escucha en 0.0.0.0:$PORT y usa automáticamente RENDER_EXTERNAL_URL (https:// fijo).
        - En PC local: escucha en 127.0.0.1:8089 y abre el túnel HTTPS con cloudflared.exe.
        """
        self.app_ref = app
        render_url = os.getenv("RENDER_EXTERNAL_URL", "").strip()
        is_cloud = bool(os.getenv("RENDER") or render_url or os.getenv("PORT"))
        port = int(os.getenv("PORT") or os.getenv("WEBAPP_PORT", "8089"))
        bind_host = "0.0.0.0" if is_cloud else "127.0.0.1"

        web_app = web.Application()
        web_app.router.add_get("/", self._handle_webapp_index)
        web_app.router.add_get("/health", lambda req: web.Response(text="OK", status=200))
        web_app.router.add_get("/api/scan", self._handle_webapp_api_scan)
        web_app.router.add_get("/api/create_vip_invoice", self._handle_webapp_create_vip_invoice)

        self.web_runner = web.AppRunner(web_app)
        await self.web_runner.setup()
        site = web.TCPSite(self.web_runner, bind_host, port)
        await site.start()
        logger.info(f"Servidor de Mini App escuchando en http://{bind_host}:{port}")

        # Si estamos en Render.com u otro PaaS con URL HTTPS pública fija:
        public_cloud_url = render_url or os.getenv("PUBLIC_WEBAPP_URL", "").strip()
        if public_cloud_url and public_cloud_url.startswith("https://"):
            self.webapp_url = public_cloud_url.rstrip("/")
            logger.info(f"☁️ Modo Nube (Render) detectado. Mini App URL fija: {self.webapp_url}")
            try:
                await app.bot.set_chat_menu_button(
                    menu_button=MenuButtonWebApp(
                        text="🦅 Escáner",
                        web_app=WebAppInfo(url=self.webapp_url),
                    )
                )
            except Exception as menu_err:
                logger.debug(f"No se pudo fijar MenuButtonWebApp en la nube: {menu_err}")
            return

        if is_cloud:
            logger.info("☁️ Ejecutando en contenedor cloud (sin cloudflared local).")
            return

        cf_bin = Path("cloudflared.exe")
        if not cf_bin.exists():
            logger.warning("cloudflared.exe no encontrado; usando solo modo chat.")
            return

        try:
            self.tunnel_proc = await asyncio.create_subprocess_exec(
                str(cf_bin.resolve()),
                "tunnel",
                "--url",
                f"http://127.0.0.1:{port}",
                "--no-autoupdate",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            async def _watch_tunnel_output() -> None:
                if not self.tunnel_proc or not self.tunnel_proc.stderr:
                    return
                url_pattern = re.compile(r"https://[a-zA-Z0-9-]+\.trycloudflare\.com")
                while True:
                    line_bytes = await self.tunnel_proc.stderr.readline()
                    if not line_bytes:
                        break
                    line = line_bytes.decode("utf-8", errors="ignore")
                    match = url_pattern.search(line)
                    if match and not self.webapp_url:
                        self.webapp_url = match.group(0)
                        self._save_env_setting("WEBAPP_PUBLIC_URL", self.webapp_url)
                        logger.info(f"🚀 Mini App HTTPS URL activa: {self.webapp_url}")
                        try:
                            await app.bot.set_chat_menu_button(
                                menu_button=MenuButtonWebApp(
                                    text="🦅 Escáner",
                                    web_app=WebAppInfo(url=self.webapp_url),
                                )
                            )
                        except Exception as menu_err:
                            logger.debug(f"No se pudo fijar MenuButtonWebApp: {menu_err}")

            asyncio.create_task(_watch_tunnel_output())
        except Exception as e:
            logger.warning(f"No se pudo iniciar el túnel HTTPS de Mini App: {e}")

    def _save_env_setting(self, key: str, value: str) -> None:
        env_path = Path(".env")
        if not env_path.exists():
            return
        lines = env_path.read_text(encoding="utf-8").splitlines()
        found = False
        new_lines = []
        for line in lines:
            if line.startswith(f"{key}="):
                new_lines.append(f"{key}={value}")
                found = True
            else:
                new_lines.append(line)
        if not found:
            new_lines.append(f"{key}={value}")
        env_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")

    def _register_chat_id_if_needed(self, update: Update) -> None:
        if not update.effective_chat:
            return
        detected_id = str(update.effective_chat.id)
        if not self.chat_id or self.chat_id == "tu_chat_id_aqui":
            self.chat_id = detected_id
            self._save_env_setting("TELEGRAM_CHAT_ID", self.chat_id)

    def _build_start_dashboard_text(self) -> str:
        server_info = SERVERS_CONFIG[self.config.server_key]
        nats_status = (
            f"⚡ <b>Escucha Total Activa</b> <i>({self.nats_stream.orders_processed:,} órdenes en vivo)</i>"
            if self.nats_stream.is_connected
            else "🔄 Reconectando NATS..."
        )
        strategy_desc = (
            "⚡ Venta Instantánea (Orden de Compra)"
            if self.config.strategy == "instant_sell"
            else "⏳ Orden de Venta en Mercado"
        )

        return (
            "🦅 <b>MERCADER DE ALBION — ESCUCHA TOTAL NATS</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🌐 <b>Servidor Activo:</b> <b>{server_info['name']}</b>\n"
            f"📡 <b>Motor NATS:</b> {nats_status}\n"
            f"🗣️ <b>Idioma:</b> <b>100% Español Oficial ({len(self.translator.catalog):,} ítems)</b>\n"
            f"🏰 <b>Ciudades:</b> <b>7 Ciudades Reales + Caerleon/Brecilien</b>\n"
            f"🎯 <b>Modo:</b> {strategy_desc}\n\n"
            "🎛️ <b>TUS FILTROS ACTUALES</b>\n"
            "<blockquote>"
            f"• 📈 <b>ROI Mínimo:</b> <b>{self.config.min_roi:.1f}%</b>\n"
            f"• 💰 <b>Ganancia Mínima:</b> <b>+{self.config.min_profit:,} silver</b> / u.\n"
            f"• ⏱️ <b>Frescura Máxima:</b> <b>{self.config.max_age_minutes} min</b>"
            "</blockquote>\n\n"
            "🧭 <b>COMANDOS RÁPIDOS</b>\n"
            "• 👉 /scan — <b>Escanear oportunidades en vivo ahora mismo</b>\n"
            "• 👉 /referidos — <b>🎁 Ganar VIP Gratis invitando amigos</b>\n"
            "• 👉 /vip — <b>👑 Membresía VIP sin publicidad</b>\n"
            "• 👉 /config — <b>Ajustar filtros</b> <i>(Ej: /config 10 500)</i>\n\n"
            "👇 <b>Selecciona tu servidor de Albion Online:</b>"
        )

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_message:
            return
        self._register_chat_id_if_needed(update)

        user_id = update.effective_user.id if update.effective_user else (update.effective_chat.id if update.effective_chat else 0)
        referrer_id: Optional[str] = None
        if context.args and len(context.args) > 0:
            arg0 = context.args[0].strip()
            if arg0.startswith("ref_"):
                referrer_id = arg0.replace("ref_", "", 1).strip()

        if user_id:
            is_valid_ref, reward_unlocked, cycle_progress, total_invited, exp_dt = (
                self.vip_manager.register_user_and_process_referral(user_id, referrer_id)
            )
            if is_valid_ref and referrer_id:
                target = self.vip_manager.referral_target
                reward_days = self.vip_manager.referral_reward_days
                try:
                    if reward_unlocked and exp_dt:
                        await context.bot.send_message(
                            chat_id=int(referrer_id),
                            text=(
                                "🎉👑 <b>¡META DE REFERIDOS COMPLETADA!</b> 👑🎉\n"
                                "━━━━━━━━━━━━━━━━━━━━━━\n"
                                f"• 👥 Un nuevo mercader entró con tu enlace (<b>{target}/{target} invitados</b>).\n"
                                f"• 🎁 <b>Premio activado:</b> <b>+{reward_days} Días de VIP Sin Publicidad</b>\n"
                                f"• 📅 <b>VIP válido hasta:</b> <b>{exp_dt.strftime('%d/%m/%Y')} UTC</b>\n\n"
                                "🚀 ¡Sigue compartiendo tu link con /referidos para acumular más días VIP gratis!"
                            ),
                            parse_mode=ParseMode.HTML,
                        )
                    else:
                        rem = target - cycle_progress
                        await context.bot.send_message(
                            chat_id=int(referrer_id),
                            text=(
                                "🎁 <b>¡NUEVO REFERIDO REGISTRADO!</b>\n"
                                "━━━━━━━━━━━━━━━━━━━━━━\n"
                                f"• 👤 Un amigo acaba de iniciar el bot con tu enlace.\n"
                                f"• 📊 <b>Progreso actual:</b> <b>{cycle_progress}/{target} amigos</b>\n"
                                f"• ⏳ Te faltan solo <b>{rem} amigo(s)</b> para desbloquear <b>+{reward_days} Días de VIP Sin Publicidad</b>.\n\n"
                                "👉 Revisa tu panel con /referidos."
                            ),
                            parse_mode=ParseMode.HTML,
                        )
                except Exception as notify_err:
                    logger.debug(f"No se pudo notificar al referente {referrer_id}: {notify_err}")

        await update.effective_message.reply_text(
            self._build_start_dashboard_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=build_server_selection_keyboard(self.config.server_key),
        )

    async def on_server_selected(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        if not query or not query.data:
            return

        await query.answer("Cambiando servidor y conectando Escucha Total NATS...")
        _, selected_key = query.data.split(":", 1)
        if selected_key not in SERVERS_CONFIG:
            return

        self.config.server_key = selected_key
        self._save_env_setting("ALBION_SERVER", selected_key)

        await self.nats_stream.connect_to_server(selected_key)
        self.notified_cache.clear()

        server_name = SERVERS_CONFIG[selected_key]["name"]
        await query.edit_message_text(
            self._build_start_dashboard_text()
            + f"\n\n✅ <b>Servidor cambiado a {server_name}.</b> Toca /scan para buscar rutas.",
            parse_mode=ParseMode.HTML,
            reply_markup=build_server_selection_keyboard(self.config.server_key),
        )

    async def cmd_scan(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_message:
            return
        self._register_chat_id_if_needed(update)

        server_info = SERVERS_CONFIG[self.config.server_key]
        wait_msg = await update.effective_message.reply_text(
            f"⚡ <b>Analizando tráfico en vivo de {server_info['short_name']}...</b>",
            parse_mode=ParseMode.HTML,
        )

        base_items = self.whitelist_manager.load_items()
        dynamic_items = self.nats_stream.get_dynamic_items_to_query(base_items, max_total=160)

        rest_records = await self.api_client.fetch_all_prices(
            self.config.server_key, dynamic_items, MONITORED_CITIES
        )
        records = self.nats_stream.merge_with_rest_records(rest_records)
        df_opps = ArbitrageEngine.analyze_both_strategies(records, self.config)

        nats_badge = (
            f"⚡ <b>Escucha Total NATS:</b> <b>{self.nats_stream.orders_processed:,} órdenes en vivo</b> "
            f"| <b>{len(self.nats_stream.recent_live_items)} ítems calientes</b> detectados al instante\n"
        )

        if df_opps.empty:
            # Búsqueda progresiva automática (30 min -> 60 min -> 120 min) para NUNCA dejar al usuario sin resultados
            df_relaxed = pd.DataFrame()
            for fallback_age in (30, 60, 120, 240):
                relaxed_cfg = ScannerConfig(
                    server_key=self.config.server_key,
                    min_roi=1.0,
                    min_profit=5,
                    max_age_minutes=fallback_age,
                    strategy=self.config.strategy,
                )
                df_relaxed = ArbitrageEngine.analyze_both_strategies(records, relaxed_cfg)
                if len(df_relaxed) >= 3:
                    break

            if not df_relaxed.empty:
                top_relaxed = df_relaxed.head(5)
                cards = [
                    format_opportunity_card(row, self.translator, rank=idx + 1)
                    for idx, (_, row) in enumerate(top_relaxed.iterrows())
                ]
                cards_block = "\n\n".join(cards)
                user_id = update.effective_user.id if update.effective_user else None
                user_is_vip = self.vip_manager.is_vip(user_id)
                if not user_is_vip:
                    self.ad_manager.record_impression(user_id)
                ad_footer = "" if user_is_vip else self.ad_manager.build_ad_footer()
                ad_kb = self.ad_manager.build_scan_keyboard(self.webapp_url)

                await wait_msg.edit_text(
                    f"🔍 <b>RADAR EN TIEMPO REAL — {server_info['short_name'].upper()}</b>\n"
                    "━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"{nats_badge}\n"
                    f"ℹ️ <i>Mostrando las <b>{len(cards)} rutas más frescas y rentables ahora mismo</b> "
                    f"(ordenadas por actualización más reciente):</i>\n\n"
                    f"{cards_block}\n\n"
                    "💡 <b>Tip:</b> Toca el botón <b>🚀 Abrir Radar Interactivo (Mini App)</b> abajo para abrir la ventana web en vivo."
                    f"{ad_footer}",
                    parse_mode=ParseMode.HTML,
                    reply_markup=ad_kb,
                    disable_web_page_preview=True,
                )
            else:
                await wait_msg.edit_text(
                    f"⚠️ <b>Sin datos en {server_info['short_name']}</b>\n"
                    "━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"{nats_badge}\n"
                    "🔄 <i>Intenta nuevamente con</i> /scan.",
                    parse_mode=ParseMode.HTML,
                )
            return

        top_5 = df_opps.head(5)
        cards = [
            format_opportunity_card(row, self.translator, rank=idx + 1)
            for idx, (_, row) in enumerate(top_5.iterrows())
        ]
        user_id = update.effective_user.id if update.effective_user else None
        user_is_vip = self.vip_manager.is_vip(user_id)
        if not user_is_vip:
            self.ad_manager.record_impression(user_id)
        ad_footer = "" if user_is_vip else self.ad_manager.build_ad_footer()
        ad_kb = self.ad_manager.build_scan_keyboard(self.webapp_url)

        header = (
            f"🏆 <b>TOP {len(cards)} OPORTUNIDADES EN VIVO — {server_info['short_name'].upper()}</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{nats_badge}\n"
        )
        footer = (
            "\n\n🦣 <i>Toca el botón de abajo para abrir el Radar Interactivo (Mini App).</i>"
            f"{ad_footer}"
        )
        full_response = header + "\n\n".join(cards) + footer

        await wait_msg.edit_text(
            full_response,
            parse_mode=ParseMode.HTML,
            reply_markup=ad_kb,
            disable_web_page_preview=True,
        )

    async def cmd_config(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_message:
            return
        self._register_chat_id_if_needed(update)

        args = context.args or []
        if len(args) != 2:
            await update.effective_message.reply_text(
                "🎛️ <b>AJUSTE DE FILTROS EN CALIENTE</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "Escribe el comando seguido del <b>ROI (%)</b> y la <b>Ganancia por unidad</b>:\n\n"
                "<blockquote>"
                "• <b>Para Recursos y Lotes:</b>\n"
                "  👉 <b>/config 5 20</b> <i>(5% ROI y +20s por unidad)</i>\n\n"
                "• <b>Para Equipamiento, Bolsas y Capas:</b>\n"
                "  👉 <b>/config 12 1500</b> <i>(12% ROI y +1,500s por unidad)</i>"
                "</blockquote>",
                parse_mode=ParseMode.HTML,
            )
            return

        try:
            new_roi = float(args[0].replace("%", "").replace(",", "."))
            new_profit = int(float(args[1].replace(",", "")))
            if new_roi <= 0 or new_profit <= 0:
                raise ValueError("Los valores deben ser mayores que cero.")

            self.config.min_roi = new_roi
            self.config.min_profit = new_profit

            await update.effective_message.reply_text(
                "✅ <b>¡FILTROS ACTUALIZADOS CON ÉXITO!</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "<blockquote>"
                f"🌐 <b>Servidor:</b> <b>{SERVERS_CONFIG[self.config.server_key]['name']}</b>\n"
                f"📈 <b>Nuevo ROI Neto Mínimo:</b> <b>{self.config.min_roi:.1f}%</b>\n"
                f"💰 <b>Nueva Ganancia Mínima:</b> <b>+{self.config.min_profit:,} silver</b> / unidad"
                "</blockquote>\n\n"
                "👉 Toca /scan para ver los resultados en vivo.",
                parse_mode=ParseMode.HTML,
            )
        except ValueError:
            await update.effective_message.reply_text(
                "❌ <b>Formato inválido</b>\n"
                "Ingresa dos números mayores a 0. Ejemplo:\n"
                "👉 <b>/config 8 200</b>",
                parse_mode=ParseMode.HTML,
            )

    async def cmd_anuncio(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """
        Comando de Administrador (/anuncio):
        - Sin argumentos: muestra estadísticas de impresiones, usuarios únicos y el anuncio actual.
        - /anuncio off : desactiva la publicidad.
        - /anuncio on  : activa la publicidad.
        - /anuncio Texto del anuncio | Texto del Botón | https://link-de-monetag.com
        """
        if not update.effective_message:
            return
        self._register_chat_id_if_needed(update)

        # Verificar que solo el dueño del bot pueda editar los anuncios
        caller_id = str(update.effective_chat.id) if update.effective_chat else ""
        if self.chat_id and caller_id != self.chat_id:
            await update.effective_message.reply_text("⛔ Solo el administrador del bot puede gestionar anuncios.")
            return

        raw_text = update.effective_message.text or ""
        payload = raw_text.partition(" ")[2].strip()

        if not payload:
            status = "🟢 Activo" if self.ad_manager.data.get("enabled", True) else "🔴 Pausado"
            impressions = int(self.ad_manager.data.get("impressions", 0))
            webapp_opens = int(self.ad_manager.data.get("webapp_opens", 0))
            unique_users = len(self.ad_manager.data.get("users_seen", []))
            zone_id = self.ad_manager.data.get("monetag_zone_id") or "Sin configurar aún"
            await update.effective_message.reply_text(
                "📢 <b>PANEL DE MONETIZACIÓN (MINI APP SDK + DIRECT LINK)</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                f"• <b>Estado:</b> {status}\n"
                f"• <b> Zona Monetag SDK (data-zone):</b> <code>{zone_id}</code>\n"
                f"• <b>Aperturas de Mini App Web:</b> <b>{webapp_opens:,} aperturas</b>\n"
                f"• <b>Impresiones en /scan:</b> <b>{impressions:,} vistas</b>\n"
                f"• <b>Usuarios Únicos:</b> <b>{unique_users:,} mercaderes</b>\n"
                f"• <b>URL Mini App Activa:</b> {self.webapp_url or 'Iniciando túnel...'}\n\n"
                "⚙️ <b>Comandos de Monetag:</b>\n"
                "• Configurar tu SDK de Mini App:\n"
                "  <code>/monetag TU_CODIGO_O_ZONA</code>\n"
                "• Cambiar Banner/Direct Link:\n"
                "  <code>/anuncio Texto | Botón | https://tu-link.com</code>\n"
                "• Pausar/Activar: <code>/anuncio off</code> / <code>/anuncio on</code>",
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            return

        if payload.lower() == "off":
            self.ad_manager.set_enabled(False)
            await update.effective_message.reply_text("🔴 <b>Publicidad desactivada en /scan.</b>", parse_mode=ParseMode.HTML)
            return

        if payload.lower() == "on":
            self.ad_manager.set_enabled(True)
            await update.effective_message.reply_text("🟢 <b>Publicidad activada en /scan.</b>", parse_mode=ParseMode.HTML)
            return

        parts = [p.strip() for p in payload.split("|")]
        if len(parts) != 3 or not parts[2].startswith(("http://", "https://")):
            await update.effective_message.reply_text(
                "⚠️ <b>Formato incorrecto.</b> Separa con <code>|</code> las 3 partes:\n\n"
                "<code>/anuncio Texto del anuncio | Texto del Botón | https://tu-link.com</code>",
                parse_mode=ParseMode.HTML,
            )
            return

        self.ad_manager.update_ad(parts[0], parts[1], parts[2])
        await update.effective_message.reply_text(
            "✅ <b>¡Anuncio actualizado en caliente!</b>\n"
            "Haz una prueba con /scan para ver cómo luce debajo de los resultados.",
            parse_mode=ParseMode.HTML,
        )

    async def cmd_monetag(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Permite pegar directamente el script <script ... data-zone='XXXX'> o el ID de zona de Monetag."""
        if not update.effective_message:
            return
        self._register_chat_id_if_needed(update)

        caller_id = str(update.effective_chat.id) if update.effective_chat else ""
        if self.chat_id and caller_id != self.chat_id:
            await update.effective_message.reply_text("⛔ Solo el administrador puede configurar Monetag.")
            return

        raw_text = update.effective_message.text or ""
        payload = raw_text.partition(" ")[2].strip()
        if not payload:
            zone_id = self.ad_manager.data.get("monetag_zone_id") or "Sin configurar"
            await update.effective_message.reply_text(
                "💎 <b>CONFIGURACIÓN DE MONETAG MINI APP SDK</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                f"• <b>Zona actual:</b> <code>{zone_id}</code>\n\n"
                "Pega el código <code>&lt;script&gt;</code> completo que te dio Monetag o el número de <code>data-zone</code> así:\n"
                "👉 <code>/monetag 1234567</code>\n"
                "o\n"
                "👉 <code>/monetag &lt;script src='//libtl.com/sdk.js' data-zone='1234567' data-sdk='show_1234567'&gt;&lt;/script&gt;</code>",
                parse_mode=ParseMode.HTML,
            )
            return

        zone_id = self.ad_manager.set_monetag_zone(payload)
        await update.effective_message.reply_text(
            "✅ <b>¡SDK de Monetag Mini App activado!</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            f"• <b>data-zone detectado:</b> <code>{zone_id}</code>\n"
            f"• <b>Script inyectado:</b> <code>show_{zone_id}()</code>\n\n"
            "🚀 Cada vez que alguien abra la ventana web desde /scan o desde el botón <b>🦅 Escáner</b> abajo a la izquierda, se ejecutará tu SDK de Monetag.",
            parse_mode=ParseMode.HTML,
        )

    async def cmd_vip(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Envía el panel de Membresía VIP y la factura oficial de Telegram Stars (XTR)."""
        if not update.effective_message or not update.effective_chat:
            return
        self._register_chat_id_if_needed(update)

        user_id = update.effective_user.id if update.effective_user else update.effective_chat.id
        stars = self.vip_manager.price_stars
        days = self.vip_manager.vip_days

        if self.vip_manager.is_vip(user_id):
            left = self.vip_manager.days_left(user_id)
            await update.effective_message.reply_text(
                f"👑 <b>¡ERES MIEMBRO MERCADER VIP ACTIVO!</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                f"• ⏱️ <b>Tiempo restante:</b> <b>{left} días</b>\n"
                "• 🚫 <b>Publicidad de Monetag:</b> <b>Desactivada (0% anuncios)</b>\n\n"
                "Si deseas extender tu membresía otros 30 días, puedes abonar la factura aquí abajo:",
                parse_mode=ParseMode.HTML,
            )

        await context.bot.send_invoice(
            chat_id=update.effective_chat.id,
            title=f"👑 Membresía Mercader VIP ({days} Días)",
            description=(
                f"Elimina el 100% de la publicidad en la Mini App y en /scan durante {days} días, "
                "con actualización directa del Top 10 NATS sin esperas."
            ),
            payload=f"vip_stars_{user_id}_{days}",
            provider_token="",  # Vacío para Telegram Stars (XTR)
            currency="XTR",
            prices=[LabeledPrice(label=f"Membresía VIP ({days} Días)", amount=stars)],
        )

    async def precheckout_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Aprueba automáticamente la orden de pago de Telegram Stars antes del cobro."""
        query = update.pre_checkout_query
        if query:
            await query.answer(ok=True)

    async def successful_payment_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Se dispara en el instante en que el usuario completa el pago con Telegram Stars."""
        if not update.effective_message or not update.effective_message.successful_payment:
            return
        payment = update.effective_message.successful_payment
        user_id = update.effective_user.id if update.effective_user else 0

        # Extraer user_id del payload por si compró desde la Mini App
        if payment.invoice_payload.startswith("vip_stars_"):
            parts = payment.invoice_payload.split("_")
            if len(parts) >= 3 and parts[2].isdigit() and int(parts[2]) > 0:
                user_id = int(parts[2])

        exp_dt = self.vip_manager.activate_vip(user_id, self.vip_manager.vip_days)
        await update.effective_message.reply_text(
            "🎉👑 <b>¡PAGO RECIBIDO! YA ERES MIEMBRO MERCADER VIP</b> 👑🎉\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            f"• ⭐ <b>Estrellas abonadas:</b> <b>{payment.total_amount} XTR</b>\n"
            f"• 🚫 <b>Publicidad:</b> <b>100% Eliminada en Mini App y Chat</b>\n"
            f"• 📅 <b>Válido hasta:</b> <b>{exp_dt.strftime('%d/%m/%Y')} UTC</b>\n\n"
            "🚀 Abre ahora el <b>🦅 Escáner (Mini App)</b> o usa /scan para disfrutar tu cuenta VIP sin anuncios.",
            parse_mode=ParseMode.HTML,
        )

    async def cmd_preciovip(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Permite al dueño del bot cambiar cuántas Estrellas cuesta el VIP (ej: /preciovip 150)."""
        if not update.effective_message:
            return
        caller_id = str(update.effective_chat.id) if update.effective_chat else ""
        if self.chat_id and caller_id != self.chat_id:
            await update.effective_message.reply_text("⛔ Solo el administrador puede cambiar el precio VIP.")
            return

        if not context.args or not context.args[0].isdigit():
            await update.effective_message.reply_text(
                f"⭐ <b>Precio actual del VIP (30 días):</b> <b>{self.vip_manager.price_stars} Estrellas</b>\n\n"
                "Para cambiarlo envía:\n"
                "👉 <code>/preciovip [cantidad_de_estrellas]</code>\n"
                "Ejemplo: <code>/preciovip 150</code> (o <code>/preciovip 1</code> para probar con 1 estrella).",
                parse_mode=ParseMode.HTML,
            )
            return

        new_stars = int(context.args[0])
        self.vip_manager.set_price_stars(new_stars)
        await update.effective_message.reply_text(
            f"✅ <b>¡Precio VIP actualizado a {self.vip_manager.price_stars} ⭐ Estrellas!</b>\n"
            "Ya se actualizó tanto en /vip como dentro del botón <b>👑 PREMIUM / VIP</b> de la Mini App.",
            parse_mode=ParseMode.HTML,
        )

    async def cmd_darvip(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Permite al administrador activar VIP manualmente a cualquier usuario: /darvip [user_id] [dias]."""
        if not update.effective_message:
            return
        caller_id = str(update.effective_chat.id) if update.effective_chat else ""
        if self.chat_id and caller_id != self.chat_id:
            await update.effective_message.reply_text("⛔ Solo el administrador puede otorgar VIP.")
            return

        if not context.args:
            await update.effective_message.reply_text(
                "👑 <b>OTORGAR VIP MANUALMENTE</b>\n"
                "Uso: <code>/darvip [ID_de_usuario] [días]</code>\n"
                f"• Ejemplo para darte VIP a ti mismo por 30 días:\n"
                f"  <code>/darvip {caller_id} 30</code>",
                parse_mode=ParseMode.HTML,
            )
            return

        target_id = context.args[0]
        days = int(context.args[1]) if len(context.args) > 1 and context.args[1].isdigit() else 30
        exp_dt = self.vip_manager.activate_vip(target_id, days)
        await update.effective_message.reply_text(
            f"✅ <b>VIP activado para el usuario <code>{target_id}</code> por {days} días</b> "
            f"(hasta el {exp_dt.strftime('%d/%m/%Y')}).",
            parse_mode=ParseMode.HTML,
        )

    def _build_referrals_message_and_keyboard(
        self, user_id: int | str, bot_username: str
    ) -> Tuple[str, InlineKeyboardMarkup]:
        stats = self.vip_manager.get_referral_stats(user_id)
        target = stats["target"]
        cycle_progress = stats["cycle_progress"]
        remaining = stats["remaining"]
        total_invited = stats["total_invited"]
        reward_days = stats["reward_days"]

        username_clean = (bot_username or "Mercaderalbion_bot").lstrip("@")
        ref_link = f"https://t.me/{username_clean}?start=ref_{user_id}"

        filled_dots = "🟢" * cycle_progress
        empty_dots = "⚪" * remaining
        progress_bar = f"{filled_dots}{empty_dots} ({cycle_progress}/{target})"

        share_text = (
            "🦅 ¡Mira este Bot de Arbitraje en Tiempo Real para Albion Online! "
            "Escanea las 7 ciudades + Caerleon en vivo y te dice qué ítems transportar para ganar millones de Silver gratis:"
        )
        share_url = (
            f"https://t.me/share/url?url={urllib.parse.quote(ref_link)}"
            f"&text={urllib.parse.quote(share_text)}"
        )

        msg_text = (
            "🎁 <b>SISTEMA DE REFERIDOS — GANA VIP GRATIS</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Invita a tus amigos o compañeros de gremio de Albion Online y recibe "
            f"<b>+{reward_days} Días de Membresía VIP (100% Sin Publicidad)</b> automáticamente por cada "
            f"<b>{target} amigos</b> que inicien el bot con tu enlace.\n\n"
            "📊 <b>TU PROGRESO ACTUAL:</b>\n"
            "<blockquote>"
            f"• 🎯 <b>Meta actual:</b> {progress_bar}\n"
            f"• 👥 <b>Total de amigos invitados:</b> <b>{total_invited} mercaderes</b>\n"
            f"• ⏳ <b>Te faltan:</b> <b>{remaining} amigo(s)</b> para recibir <b>+{reward_days} Días VIP Gratis</b>"
            "</blockquote>\n\n"
            "🔗 <b>Tu enlace personal de invitación (toca para copiar):</b>\n"
            f"<code>{ref_link}</code>\n\n"
            "👇 <i>Copia tu link con el botón de abajo para pegarlo en Discord, WhatsApp o Facebook, o envíalo directo por Telegram:</i>"
        )

        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "📋 Copiar Link de Referido",
                        copy_text=CopyTextButton(text=ref_link),
                    )
                ],
                [InlineKeyboardButton("📲 Enviar a un Contacto / Grupo de Telegram", url=share_url)],
            ]
        )
        return msg_text, kb

    async def cmd_referidos(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Muestra el panel viral de referidos con link único y botón de compartir en 1 clic."""
        if not update.effective_message:
            return
        self._register_chat_id_if_needed(update)
        user_id = update.effective_user.id if update.effective_user else (update.effective_chat.id if update.effective_chat else 0)
        bot_username = context.bot.username or "Mercaderalbion_bot"
        msg_text, kb = self._build_referrals_message_and_keyboard(user_id, bot_username)
        await update.effective_message.reply_text(
            msg_text,
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
            disable_web_page_preview=True,
        )

    async def on_open_referrals(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        if not query or not query.message:
            return
        await query.answer("Abriendo tu panel de referidos...")
        user_id = query.from_user.id if query.from_user else 0
        bot_username = context.bot.username or "Mercaderalbion_bot"
        msg_text, kb = self._build_referrals_message_and_keyboard(user_id, bot_username)
        await query.message.reply_text(
            msg_text,
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
            disable_web_page_preview=True,
        )

    async def background_scanner_loop(self, app: Application) -> None:
        logger.info(f"Iniciando bucle de Escucha Total NATS cada {self.config.scan_interval}s...")
        while True:
            try:
                if self.config.is_running and self.chat_id and self.chat_id != "tu_chat_id_aqui":
                    base_items = self.whitelist_manager.load_items()
                    dynamic_items = self.nats_stream.get_dynamic_items_to_query(base_items, max_total=160)
                    rest_records = await self.api_client.fetch_all_prices(
                        self.config.server_key, dynamic_items, MONITORED_CITIES
                    )
                    records = self.nats_stream.merge_with_rest_records(rest_records)
                    df_opps = ArbitrageEngine.analyze_opportunities(records, self.config)

                    if not df_opps.empty:
                        now = datetime.now(timezone.utc)
                        alerts_sent = 0

                        for _, row in df_opps.iterrows():
                            route_key = (
                                self.config.server_key,
                                str(row["item_id"]),
                                str(row["origin_city"]),
                                str(row["dest_city"]),
                            )
                            last_sent = self.notified_cache.get(route_key)
                            if last_sent and (now - last_sent).total_seconds() < 600:
                                continue

                            alert_text = format_opportunity_card(row, self.translator)
                            await app.bot.send_message(
                                chat_id=self.chat_id,
                                text=alert_text,
                                parse_mode=ParseMode.HTML,
                            )
                            self.notified_cache[route_key] = now
                            alerts_sent += 1
                            await asyncio.sleep(0.5)
                            if alerts_sent >= 5:
                                break

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Excepción controlada en bucle de escaneo: {e}", exc_info=True)

            await asyncio.sleep(self.config.scan_interval)

    async def post_init(self, app: Application) -> None:
        await self.translator.ensure_catalog_loaded()
        await self.nats_stream.connect_to_server(self.config.server_key)
        await self._start_miniapp_server_and_tunnel(app)
        self.background_task = asyncio.create_task(self.background_scanner_loop(app))

    async def post_shutdown(self, app: Application) -> None:
        if self.background_task and not self.background_task.done():
            self.background_task.cancel()
            try:
                await self.background_task
            except asyncio.CancelledError:
                pass
        if self.tunnel_proc and self.tunnel_proc.returncode is None:
            self.tunnel_proc.terminate()
        if self.web_runner:
            await self.web_runner.cleanup()
        await self.nats_stream.close()
        await self.api_client.close()
        logger.info("Recursos Mini App, NATS y HTTP cerrados correctamente.")


# ==============================================================================
# 9. PUNTO DE ENTRADA (ENTRYPOINT)
# ==============================================================================

def main() -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()

    if not token or token == "tu_token_de_botfather_aqui":
        logger.error("Falta configurar TELEGRAM_BOT_TOKEN real en tu archivo .env.")
        return

    config = ScannerConfig()
    if config.server_key not in SERVERS_CONFIG:
        config.server_key = "americas"

    whitelist_manager = ItemWhitelistManager("items_whitelist.txt")
    api_client = AlbionAPIClient()
    nats_stream = AlbionRealtimeNATSStream()
    translator = SpanishItemTranslator("items_es.json")
    ad_manager = AdManager("ads_config.json")
    vip_manager = VipManager("vip_users.json")

    bot_controller = AlbionTelegramBot(
        token=token,
        chat_id=chat_id,
        api_client=api_client,
        nats_stream=nats_stream,
        whitelist_manager=whitelist_manager,
        translator=translator,
        ad_manager=ad_manager,
        vip_manager=vip_manager,
        config=config,
    )

    application = (
        ApplicationBuilder()
        .token(token)
        .connect_timeout(30.0)
        .read_timeout(30.0)
        .write_timeout(30.0)
        .post_init(bot_controller.post_init)
        .post_shutdown(bot_controller.post_shutdown)
        .build()
    )

    application.add_handler(CommandHandler("start", bot_controller.cmd_start))
    application.add_handler(CommandHandler("scan", bot_controller.cmd_scan))
    application.add_handler(CommandHandler("config", bot_controller.cmd_config))
    application.add_handler(CommandHandler("vip", bot_controller.cmd_vip))
    application.add_handler(CommandHandler("referidos", bot_controller.cmd_referidos))
    application.add_handler(CommandHandler("invitar", bot_controller.cmd_referidos))
    application.add_handler(CommandHandler("preciovip", bot_controller.cmd_preciovip))
    application.add_handler(CommandHandler("darvip", bot_controller.cmd_darvip))
    application.add_handler(CommandHandler("anuncio", bot_controller.cmd_anuncio))
    application.add_handler(CommandHandler("monetag", bot_controller.cmd_monetag))
    application.add_handler(PreCheckoutQueryHandler(bot_controller.precheckout_callback))
    application.add_handler(
        MessageHandler(filters.SUCCESSFUL_PAYMENT, bot_controller.successful_payment_callback)
    )
    application.add_handler(
        CallbackQueryHandler(bot_controller.on_server_selected, pattern=r"^set_server:")
    )
    application.add_handler(
        CallbackQueryHandler(bot_controller.on_open_referrals, pattern=r"^open_referrals$")
    )

    logger.info("Bot Modo Escucha Total NATS + Traductor Español iniciado...")
    application.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
