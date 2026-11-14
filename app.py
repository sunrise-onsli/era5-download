"""Local picker for Earth Data Hub ERA5 stores.

The API key stays in process memory. The link list is the one file that
receives it, so a person's own download tool can authenticate. Logs, the
page, and HTTP responses do not include the key or the lines.
Opening a dataset reads coordinates and chunk layout. Writing the list
reads chunk addresses only, not array values.
"""

from __future__ import annotations

import argparse
import gc
import json
import logging
import math
import os
import re
import secrets
import sys
import threading
import traceback
import urllib.error
import urllib.request
import warnings
import webbrowser
from dataclasses import dataclass
from datetime import date, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from itertools import product
from pathlib import Path
from urllib.parse import quote, urlparse

import aiohttp
import numpy as np
import pandas as pd
import xarray as xr

os.environ.setdefault("OMP_NUM_THREADS", "4")

warnings.filterwarnings(
    "ignore",
    message="Numcodecs codecs are not in the Zarr version 3",
)
warnings.filterwarnings("ignore", message="BasicAuth is deprecated")
warnings.filterwarnings("ignore", message="Unclosed client session")
warnings.filterwarnings("ignore", message="Unclosed connector")

def _bundle_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(getattr(sys, "_MEIPASS"))
    return Path(__file__).resolve().parent


def _output_paths() -> tuple[Path, Path | None]:
    paper = Path(r"D:\wildfire_asia")
    if paper.is_dir():
        return (
            paper / "data" / "era5" / "edh_picker",
            paper / "data" / "era5" / "q_u_v_925_daily",
        )
    return Path.home() / "era5-download", None


DEFAULT_OUTPUT, YEARLY_OUTPUT = _output_paths()
LOG_PATH = DEFAULT_OUTPUT / "picker.log"
INDEX_PATH = _bundle_dir() / "static" / "index.html"
EDH_HOST = "https://data.earthdatahub.destine.eu"
EDH_DATA_HOST = "data.earthdatahub.destine.eu"
STAC_ITEMS = "https://earthdatahub.destine.eu/api/stac/v1/collections/era5/items"
DEFAULT_DATASET = "era5-pressure-levels"
DAILY_ATTR = "daily_mean of 00:00-23:00 UTC hourly analyses"
STANDARD_NAMES = {
    "q": "specific_humidity",
    "u": "eastward_wind",
    "v": "northward_wind",
}
MAX_OUTPUT_BYTES = 4 * 1024**3
MAX_CHUNKS = 20_000
MAX_BLOCK_BYTES = int(1.5 * 1024**3)
HOST = "127.0.0.1"
PRIORITY = [
    "q", "u", "v", "t", "r", "z", "w",
    "t2m", "d2m", "u10", "v10", "u100", "v100", "tp", "msl", "sp", "sst",
]

# Menu order only. The name shown is the catalogue title, not a translation.
# DOI is attached only for the three stores whose CDS record was checked earlier.
CATALOGUE_ORDER = [
    "era5-pressure-levels",
    "era5-pressure-levels-monthly",
    "era5-pressure-levels-for-wind-developers",
    "era5-single-levels-atmosphere",
    "era5-single-levels-atmosphere-daily",
    "era5-single-levels-atmosphere-monthly",
    "era5-single-levels-atmosphere-ensemble-spread",
    "era5-land",
    "era5-land-daily",
    "era5-land-monthly",
    "era5-single-levels-ocean-waves",
    "era5-single-levels-ocean-waves-monthly",
    "ecv-for-climate-change",
]
KNOWN_DOI = {
    "era5-pressure-levels": "10.24381/cds.bd0915c6",
    "era5-land": "10.24381/cds.e2161bac",
    "era5-land-monthly": "10.24381/cds.68d2bb30",
}
# Used only when the live catalogue cannot be read. Titles are the catalogue names.
FALLBACK_PATHS = [
    ("era5-pressure-levels", "era5/era5-pressure-levels-v0.zarr", "ERA5 hourly data on pressure levels"),
    ("era5-pressure-levels-monthly", "era5/era5-pressure-levels-monthly-v0.zarr", "ERA5 monthly data on pressure levels"),
    ("era5-single-levels-atmosphere", "era5/era5-single-levels-atmosphere-v0.zarr", "ERA5 hourly data on single levels - atmosphere"),
    ("era5-single-levels-atmosphere-daily", "era5/era5-single-levels-atmosphere-daily-utc-v0.zarr", "ERA5 daily data on single levels - atmosphere"),
    ("era5-single-levels-atmosphere-monthly", "era5/era5-single-levels-atmosphere-monthly-v0.zarr", "ERA5 monthly data on single levels - atmosphere"),
    ("era5-land", "era5/era5-land-v0.zarr", "ERA5-Land hourly data"),
    ("era5-land-daily", "era5/era5-land-daily-utc-v1.zarr", "ERA5-Land daily data"),
    ("era5-land-monthly", "era5/era5-land-monthly-v0.zarr", "ERA5-Land monthly data"),
    ("era5-single-levels-ocean-waves", "era5/era5-single-levels-ocean-waves-v0.zarr", "ERA5 hourly data on single levels - ocean waves"),
    ("era5-single-levels-ocean-waves-monthly", "era5/era5-single-levels-ocean-waves-monthly-v0.zarr", "ERA5 monthly data on single levels - ocean waves"),
]
ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,80}$")
DOI_RE = re.compile(r"10\.24381/[A-Za-z0-9._/-]+")



SECRETS: set[str] = set()
SECRET_LOCK = threading.Lock()
STATE_LOCK = threading.Lock()
SESSIONS: dict[str, "Session"] = {}
CURRENT: dict[str, "Job | None"] = {"job": None}


def scrub(text: object) -> str:
    cleaned = str(text)
    with SECRET_LOCK:
        secrets_now = list(SECRETS)
    for secret in secrets_now:
        if not secret:
            continue
        cleaned = cleaned.replace(secret, "***")
        cleaned = cleaned.replace(quote(secret, safe=""), "***")
    cleaned = re.sub(r"https://edh:[^@\s/]+@", "https://edh:***@", cleaned)
    return cleaned


def log(message: str) -> None:
    line = datetime.now().strftime("%Y-%m-%dT%H:%M:%S") + "\t" + scrub(message)
    try:
        print(line, flush=True)
    except Exception:
        pass
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(line + "\n")


_ORIGINAL_SHOWWARNING = warnings.showwarning


def _showwarning(message, category, filename, lineno, file=None, line=None):
    _ORIGINAL_SHOWWARNING(scrub(message), category, filename, lineno, file=file, line=line)


warnings.showwarning = _showwarning


class _ScrubFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = scrub(record.getMessage())
        record.args = ()
        return True


def _install_log_filter() -> None:
    scrubber = _ScrubFilter()
    root = logging.getLogger()
    root.addFilter(scrubber)
    for handler in list(root.handlers) + ([logging.lastResort] if logging.lastResort else []):
        handler.addFilter(scrubber)
    for name in ("fsspec", "urllib3", "aiohttp", "asyncio", "zarr", "xarray"):
        logger = logging.getLogger(name)
        logger.setLevel(logging.WARNING)
        logger.addFilter(scrubber)


def remember(secret: str) -> None:
    if secret:
        with SECRET_LOCK:
            SECRETS.add(secret)


def public_error(error: Exception) -> str:
    text = scrub(error)
    lowered = text.lower()
    if any(token in lowered for token in ("401", "403", "unauthorized", "forbidden")):
        return "key 无效或没有权限"
    text = " ".join(text.split())
    if len(text) > 240:
        text = text[:240] + "…"
    return text or "读取失败"


def storage_options(key: str) -> dict:
    return {
        "skip_instance_cache": True,
        "client_kwargs": {
            "trust_env": False,
            "auth": aiohttp.BasicAuth("edh", key),
        },
    }


def source_text(product: dict) -> str:
    text = f"Earth Data Hub {product['path']}, ECMWF ERA5"
    if product.get("doi"):
        text += f", DOI {product['doi']}"
    return text


def dataset_menu(products: dict) -> list[dict]:
    return [{"id": item["id"], "label": item["label"]} for item in products.values()]


def _http_json(url: str) -> dict:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json, application/geo+json",
            "User-Agent": "edh-era5-picker",
        },
    )
    with urllib.request.urlopen(request, timeout=40) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("目录不是 JSON")
    return payload


def iter_catalogue_features():
    url = STAC_ITEMS + "?limit=100"
    seen: set[str] = set()
    for _ in range(20):
        if url in seen:
            break
        seen.add(url)
        page = _http_json(url)
        features = page.get("features")
        if not isinstance(features, list):
            raise RuntimeError("目录没有数据集列表")
        yield from features
        nxt = ""
        for link in page.get("links") or []:
            if isinstance(link, dict) and link.get("rel") == "next" and isinstance(link.get("href"), str):
                nxt = link["href"]
                break
        if not nxt:
            return
        url = nxt
    raise RuntimeError("目录分页过长")


def _data_href(feature: dict) -> str:
    assets = feature.get("assets")
    if not isinstance(assets, dict):
        return ""
    hrefs: list[str] = []
    for key, asset in assets.items():
        if not isinstance(asset, dict):
            continue
        roles = asset.get("roles") or []
        if key != "data" and not (isinstance(roles, list) and "data" in roles):
            continue
        href = asset.get("href")
        if isinstance(href, str) and href not in hrefs:
            hrefs.append(href)
    if len(hrefs) != 1:
        return ""
    parsed = urlparse(hrefs[0])
    if parsed.scheme != "https" or (parsed.hostname or "").lower() != EDH_DATA_HOST:
        return ""
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        return ""
    parts = [part for part in parsed.path.split("/") if part]
    if not parts or any(part == ".." for part in parts):
        return ""
    return f"https://{EDH_DATA_HOST}/" + "/".join(parts)


def _doi_from_feature(feature: dict, item_id: str) -> str:
    blobs: list[str] = []
    props = feature.get("properties") if isinstance(feature.get("properties"), dict) else {}
    for key in ("description", "dc:abstract"):
        value = props.get(key)
        if isinstance(value, str):
            blobs.append(value)
    for link in feature.get("links") or []:
        if not isinstance(link, dict):
            continue
        for key in ("href", "title"):
            value = link.get(key)
            if isinstance(value, str):
                blobs.append(value)
    for blob in blobs:
        found = DOI_RE.search(blob)
        if found:
            return found.group(0).rstrip(".,)")
    return KNOWN_DOI.get(item_id, "")


def _label_for(item_id: str, title: object) -> str:
    if isinstance(title, str) and title.strip():
        return " ".join(title.split())
    return item_id


def products_from_features(features: list, catalogue_source: str) -> dict[str, dict]:
    found: list[dict] = []
    seen: set[str] = set()
    for feature in features:
        if not isinstance(feature, dict):
            continue
        props = feature.get("properties") if isinstance(feature.get("properties"), dict) else {}
        if feature.get("deprecated") or props.get("deprecated"):
            continue
        item_id = feature.get("id")
        if not isinstance(item_id, str) or not ID_RE.fullmatch(item_id) or item_id in seen:
            continue
        href = _data_href(feature)
        if not href:
            continue
        seen.add(item_id)
        found.append(
            {
                "id": item_id,
                "label": _label_for(item_id, props.get("title") or feature.get("title")),
                "url": href,
                "path": urlparse(href).path.lstrip("/"),
                "doi": _doi_from_feature(feature, item_id),
                "catalogue_source": catalogue_source,
            }
        )
    rank = {name: index for index, name in enumerate(CATALOGUE_ORDER)}
    found.sort(key=lambda item: (rank.get(item["id"], 1000), item["label"]))
    return {item["id"]: item for item in found}


def fallback_products() -> dict[str, dict]:
    features = []
    for item_id, path, title in FALLBACK_PATHS:
        features.append(
            {
                "id": item_id,
                "properties": {"title": title},
                "assets": {"data": {"href": f"{EDH_HOST}/{path}", "roles": ["data"]}},
            }
        )
    return products_from_features(features, "backup")


def discover_products() -> dict[str, dict]:
    try:
        products = products_from_features(list(iter_catalogue_features()), "live")
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, RuntimeError, OSError) as error:
        log(f"CATALOGUE\tbackup\t{public_error(error)}")
        return fallback_products()
    if not products:
        log("CATALOGUE\tbackup\tempty")
        return fallback_products()
    log(f"CATALOGUE\tlive\tn={len(products)}")
    return products


def open_store(key: str, url: str) -> xr.Dataset:
    return xr.open_dataset(
        url,
        engine="zarr",
        chunks={},
        storage_options=storage_options(key),
    )


def as_utc_naive(value) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is not None:
        stamp = stamp.tz_convert("UTC").tz_localize(None)
    return stamp


def json_number(value: float):
    number = float(value)
    if number.is_integer():
        return int(number)
    return number


def human_bytes(size: int) -> str:
    if size < 1024:
        return f"{size} B"
    if size < 1024**2:
        return f"{size / 1024:.0f} KB"
    if size < 1024**3:
        return f"{size / 1024**2:.0f} MB"
    return f"{size / 1024**3:.2f} GB"


def lat_text(value: float) -> str:
    if value < 0:
        return f"{-value:.2f}°S"
    if value > 0:
        return f"{value:.2f}°N"
    return "0°"


def lon_text(value: float) -> str:
    if value < 0:
        return f"{-value:.2f}°W"
    if value > 180:
        return f"{value:.2f}°E"
    if value > 0:
        return f"{value:.2f}°E"
    return "0°"


def step_text(value: float) -> str:
    return f"{value:.4f}".rstrip("0").rstrip(".")


def grid_text(lat_step: float, lon_step: float) -> str:
    if abs(lat_step - lon_step) < 1e-9:
        return f"{step_text(lat_step)}°"
    return f"纬向 {step_text(lat_step)}°、经向 {step_text(lon_step)}°"


def month_index(stamp: pd.Timestamp, origin: pd.Timestamp) -> int:
    return (stamp.year - origin.year) * 12 + (stamp.month - origin.month)


def index_to_stamp(origin: pd.Timestamp, native: str, index: int) -> pd.Timestamp:
    index = int(index)
    if native == "hourly":
        return origin + pd.Timedelta(hours=index)
    if native == "3hourly":
        return origin + pd.Timedelta(hours=3 * index)
    if native == "daily":
        return origin + pd.Timedelta(days=index)
    month0 = origin.month - 1 + index
    return pd.Timestamp(year=origin.year + month0 // 12, month=month0 % 12 + 1, day=origin.day)


def resolution_menu(native: str, day_aligned: bool) -> list[dict]:
    if native == "monthly":
        return [{"id": "monthly", "label": "月值"}]
    if native == "daily":
        return [
            {"id": "daily", "label": "日值"},
            {"id": "monthly", "label": "月平均"},
        ]
    if native == "3hourly":
        items = [
            {"id": "3hourly", "label": "3 小时"},
            {"id": "6hourly", "label": "6 小时"},
        ]
        if day_aligned:
            items.append({"id": "daily", "label": "日平均"})
            items.append({"id": "monthly", "label": "月平均"})
        return items
    items = [
        {"id": "hourly", "label": "小时"},
        {"id": "3hourly", "label": "3 小时"},
        {"id": "6hourly", "label": "6 小时"},
    ]
    if day_aligned:
        items.append({"id": "daily", "label": "日平均"})
        items.append({"id": "monthly", "label": "月平均"})
    return items


def statistic_text(native: str, resolution: str) -> str:
    if native == "3hourly" and resolution == "3hourly":
        return "native 3-hourly analyses at 00, 03, 06, 09, 12, 15, 18, 21 UTC"
    if native == "3hourly" and resolution == "6hourly":
        return "analyses at 00, 06, 12, 18 UTC taken from the 3-hourly store"
    if native == "3hourly" and resolution == "daily":
        return "daily mean of the 00, 03, 06, 09, 12, 15, 18 and 21 UTC analyses"
    if native == "3hourly" and resolution == "monthly":
        return (
            "monthly mean of daily means over the selected days; "
            "each day is the mean of the 8 analyses at 00, 03, 06, 09, 12, 15, 18, 21 UTC; "
            "a month is partial when the selected range does not cover it"
        )
    if native == "hourly" and resolution == "hourly":
        return "hourly analyses from 00:00 through 23:00 UTC"
    if native == "hourly" and resolution == "3hourly":
        return "analyses at 00, 03, 06, 09, 12, 15, 18, 21 UTC"
    if native == "hourly" and resolution == "6hourly":
        return "analyses at 00, 06, 12, 18 UTC"
    if native == "hourly" and resolution == "daily":
        return DAILY_ATTR
    if native == "hourly" and resolution == "monthly":
        return (
            "monthly mean of daily means over the selected days; "
            "each day is the mean of 00:00-23:00 UTC hourly analyses; "
            "a month is partial when the selected range does not cover it"
        )
    if native == "daily" and resolution == "daily":
        return (
            "native daily values as stored; instantaneous fields are typically "
            "daily means and accumulated fields are typically daily totals"
        )
    if native == "daily" and resolution == "monthly":
        return "monthly mean of the dataset daily values over the selected days"
    if native == "monthly" and resolution == "monthly":
        return "native monthly values as stored for each selected month"
    raise RuntimeError("未知的时间尺度")


def output_steps(selection: "Selection", native: str) -> int:
    if selection.resolution == "hourly":
        return selection.days * 24
    if selection.resolution == "3hourly":
        return selection.days * 8
    if selection.resolution == "6hourly":
        return selection.days * 4
    if selection.resolution == "daily":
        return selection.days
    if selection.resolution == "monthly":
        return (
            (selection.end.year - selection.start.year) * 12
            + (selection.end.month - selection.start.month)
            + 1
        )
    raise RuntimeError("未知的时间尺度")


@dataclass
class Session:
    key: str
    catalogs: dict
    products: dict
    created: float


@dataclass
class Selection:
    dataset_id: str
    variables: list[str]
    levels: list[float]
    start: date
    end: date
    north: float
    south: float
    west: float
    east: float
    resolution: str
    lat_i0: int
    lat_i1: int
    lon_segments: list
    time_i0: int
    time_i1: int

    @property
    def days(self) -> int:
        return (self.end - self.start).days + 1

    @property
    def nlat(self) -> int:
        return self.lat_i1 - self.lat_i0 + 1

    @property
    def nlon(self) -> int:
        return sum(segment["i1"] - segment["i0"] + 1 for segment in self.lon_segments)

    @property
    def nlevel(self) -> int:
        return len(self.levels)


class RequestError(Exception):
    def __init__(self, message: str, code: int = 400):
        super().__init__(message)
        self.code = code


class Job:
    def __init__(self, selection: Selection, destination: Path):
        self.id = secrets.token_hex(8)
        self.selection = selection
        self.destination = destination
        self.temporary = destination.with_suffix(".writing.nc")
        self.cancel = threading.Event()
        self.lock = threading.Lock()
        self.state = "running"
        self.done = 0
        self.total = 1
        self.message = "准备读取"
        self.path = str(destination)
        self.nbytes = 0

    def update(self, **fields) -> None:
        with self.lock:
            for name, value in fields.items():
                setattr(self, name, value)

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "job": self.id,
                "state": self.state,
                "done": self.done,
                "total": self.total,
                "message": self.message,
                "path": self.path if self.state == "done" else "",
                "bytes": self.nbytes if self.state == "done" else 0,
            }


def chunk_map(data_array: xr.DataArray, dims: tuple[str, ...]) -> dict:
    preferred = data_array.encoding.get("preferred_chunks") or {}
    missing = [dim for dim in dims if dim not in preferred]
    if missing:
        raise RuntimeError("数据集没有分块信息：" + ", ".join(missing))
    return {dim: int(preferred[dim]) for dim in dims}


def coordinate_name(store: xr.Dataset, candidates: tuple[str, ...], what: str) -> str:
    for name in candidates:
        if name in store.dims or name in store.coords or name in store.variables:
            return name
    raise RuntimeError(f"数据集里没有{what}")


def native_of(times: pd.DatetimeIndex) -> str:
    delta = times[1] - times[0]
    seconds = float(delta.total_seconds())
    if abs(seconds - 3600) < 1:
        return "hourly"
    if abs(seconds - 10800) < 1:
        return "3hourly"
    if abs(seconds - 86400) < 1:
        return "daily"
    if 27 * 86400 <= seconds <= 32 * 86400:
        return "monthly"
    raise RuntimeError(f"无法识别时间步长（{seconds:.0f} 秒）")


def assert_regular(times: pd.DatetimeIndex, native: str) -> None:
    if native == "hourly":
        hour_ns = np.int64(3_600_000_000_000)
        if not np.all(np.diff(times.asi8) == hour_ns):
            raise RuntimeError("时间轴不是连续的小时")
        return
    if native == "3hourly":
        step_ns = np.int64(10_800_000_000_000)
        if not np.all(np.diff(times.asi8) == step_ns):
            raise RuntimeError("时间轴不是连续的 3 小时")
        return
    if native == "daily":
        day_ns = np.int64(86_400_000_000_000)
        if not np.all(np.diff(times.asi8) == day_ns):
            raise RuntimeError("时间轴不是连续的日")
        return
    months = times.year * 12 + times.month
    if not np.all(np.diff(months) == 1):
        raise RuntimeError("时间轴不是连续的月")


def choose_variables(store: xr.Dataset, product: dict, time_name: str, lat_name: str, lon_name: str):
    groups: dict[str, list[str]] = {}
    for name in store.data_vars:
        data_array = store[name]
        dims = set(data_array.dims)
        if time_name not in dims or lat_name not in dims or lon_name not in dims:
            continue
        if data_array.dtype.kind not in "fiu":
            continue
        extra = [dim for dim in data_array.dims if dim not in {time_name, lat_name, lon_name}]
        real_extra = [dim for dim in extra if int(store.sizes[dim]) != 1]
        if len(real_extra) > 1:
            continue
        key = real_extra[0] if real_extra else ""
        groups.setdefault(key, []).append(str(name))
    if not groups:
        raise RuntimeError("数据集里没有可选取的时间变量")
    if "pressure" in product["id"]:
        keys = [key for key in groups if key]
        if not keys:
            raise RuntimeError("这个气压层数据集里没有垂直坐标")
        key = "isobaricInhPa" if "isobaricInhPa" in keys else keys[0]
    else:
        if "" not in groups:
            raise RuntimeError("这个数据集带有垂直坐标，请改用气压层数据集")
        key = ""
    return groups[key], (key or None)


def build_catalog(store: xr.Dataset, product: dict) -> dict:
    time_name = coordinate_name(store, ("valid_time", "time"), "时间坐标")
    lat_name = coordinate_name(store, ("latitude", "lat"), "纬度")
    lon_name = coordinate_name(store, ("longitude", "lon"), "经度")
    times = pd.DatetimeIndex(pd.to_datetime(np.asarray(store[time_name].values)))
    if getattr(times, "tz", None) is not None:
        times = times.tz_convert("UTC").tz_localize(None)
    if len(times) < 2:
        raise RuntimeError("时间轴太短")
    detected = native_of(times)
    assert_regular(times, detected)
    t0 = as_utc_naive(times[0])
    t1 = as_utc_naive(times[-1])
    latitude = np.asarray(store[lat_name].values, dtype="float64")
    longitude = np.asarray(store[lon_name].values, dtype="float64")
    if latitude.size < 2 or longitude.size < 2:
        raise RuntimeError("经纬网格太小")
    lat_step = float(latitude[0] - latitude[1])
    lon_step = float(longitude[1] - longitude[0])
    if lat_step <= 0 or not np.allclose(np.diff(latitude), -lat_step):
        raise RuntimeError("纬度不是从北向南的等间距网格")
    if lon_step <= 0 or not np.allclose(np.diff(longitude), lon_step):
        raise RuntimeError("经度不是从西向东的等间距网格")
    if float(longitude[0]) < -1e-6 or float(longitude[-1]) >= 360:
        raise RuntimeError("经度不是 0–360 网格")
    names, level_name = choose_variables(store, product, time_name, lat_name, lon_name)
    dims = [time_name]
    if level_name:
        dims.append(level_name)
    dims.extend([lat_name, lon_name])
    dim_tuple = tuple(dims)
    chunks = None
    var_dims: list[str] | None = None
    variables = []
    for name in names:
        current = chunk_map(store[name], dim_tuple)
        current_dims = [str(dim) for dim in store[name].dims]
        if chunks is None:
            chunks = current
            var_dims = current_dims
        elif current != chunks or current_dims != var_dims:
            raise RuntimeError(f"{name} 的分块和其他变量不同")
        attrs = store[name].attrs
        variables.append(
            {
                "name": name,
                "long_name": str(attrs.get("long_name", name)),
                "units": str(attrs.get("units", "")),
            }
        )
    variables.sort(
        key=lambda item: (
            PRIORITY.index(item["name"]) if item["name"] in PRIORITY else 100,
            item["name"],
        )
    )
    day_aligned = True
    if detected == "hourly":
        if t0.hour != 0 or t0.minute != 0 or t0.second != 0:
            raise RuntimeError("小时数据集不是从 00 UTC 开始")
        day_aligned = int(chunks[time_name]) % 24 == 0
        last_complete = t1.date() if t1.hour == 23 else (t1 - pd.Timedelta(days=1)).date()
        first_day = t0.date()
        if last_complete < first_day:
            raise RuntimeError("没有完整的一天")
    elif detected == "3hourly":
        if t0.hour != 0 or t0.minute != 0 or t0.second != 0:
            raise RuntimeError("3 小时数据集不是从 00 UTC 开始")
        day_aligned = int(chunks[time_name]) % 8 == 0
        last_complete = t1.date() if t1.hour == 21 else (t1 - pd.Timedelta(days=1)).date()
        first_day = t0.date()
        if last_complete < first_day:
            raise RuntimeError("没有完整的一天")
    elif detected == "daily":
        first_day = t0.date()
        last_complete = t1.date()
    else:
        first_day = t0.date().replace(day=1)
        last_complete = (t1 + pd.offsets.MonthEnd(0)).date()
    levels = []
    if level_name:
        levels = [float(value) for value in np.asarray(store[level_name].values)]
    notes: list[str] = []
    if product.get("catalogue_source") != "live":
        notes.append("目录暂时读不到，列表来自已核对的数据集。")
    if product["id"] == "era5-land-monthly":
        notes.append("部分月变量因 1950–2019 年数据有误已撤下。")
    note = " ".join(notes)
    return {
        "id": product["id"],
        "label": product["label"],
        "url": product["url"],
        "store": product["path"],
        "native": detected,
        "source": source_text(product),
        "note": note,
        "time_name": time_name,
        "lat_name": lat_name,
        "lon_name": lon_name,
        "level_name": level_name,
        "time_start": t0.isoformat(),
        "time_end": t1.isoformat(),
        "first_day": first_day.isoformat(),
        "last_day": last_complete.isoformat(),
        "time_size": int(times.size),
        "levels": levels,
        "variables": variables,
        "var_dims": var_dims or [],
        "var_sizes": {dim: int(store.sizes[dim]) for dim in (var_dims or [])},
        "lat_start": float(latitude[0]),
        "lat_step": lat_step,
        "lat_size": int(latitude.size),
        "lon_start": float(longitude[0]),
        "lon_step": lon_step,
        "lon_size": int(longitude.size),
        "grid": grid_text(lat_step, lon_step),
        "chunks": {
            "time": int(chunks[time_name]),
            "level": int(chunks[level_name]) if level_name else 1,
            "latitude": int(chunks[lat_name]),
            "longitude": int(chunks[lon_name]),
        },
        "resolutions": resolution_menu(detected, day_aligned),
    }


def require_session(payload: dict) -> Session:
    token = payload.get("session")
    if not isinstance(token, str):
        raise ValueError("请先接入")
    with STATE_LOCK:
        session = SESSIONS.get(token)
    if session is None:
        raise ValueError("请重新接入")
    return session


def catalog_for(session: Session, payload: dict) -> dict:
    dataset_id = payload.get("dataset")
    if not isinstance(dataset_id, str) or dataset_id not in session.products:
        raise ValueError("请选择数据集")
    with STATE_LOCK:
        catalog = session.catalogs.get(dataset_id)
    if catalog is None:
        raise ValueError("请先打开这个数据集")
    return catalog


def parse_day(value, label: str) -> date:
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError(f"{label}格式应为 YYYY-MM-DD")
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as error:
        raise ValueError(f"{label}不是有效日期") from error


def parse_float(value, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label}需要是数字")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{label}需要是有限数字")
    return number


def bound_missing(value) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def region_selection(payload: dict, catalog: dict) -> tuple[float, float, int, int, list[dict]]:
    raw = [payload.get(name) for name in ("north", "west", "south", "east")]
    if all(bound_missing(value) for value in raw):
        lat_i1 = int(catalog["lat_size"]) - 1
        lon_i1 = int(catalog["lon_size"]) - 1
        north = float(catalog["lat_start"])
        south = float(catalog["lat_start"] - catalog["lat_step"] * lat_i1)
        west = float(catalog["lon_start"])
        east = float(catalog["lon_start"] + catalog["lon_step"] * lon_i1)
        segments = [{"i0": 0, "i1": lon_i1, "shift": 0.0, "west": west, "east": east}]
        return north, south, 0, lat_i1, segments
    if any(bound_missing(value) for value in raw):
        raise ValueError("区域请四个都填，或不填表示全球")
    north, lat_i0 = snap_latitude(parse_float(payload.get("north"), "北界"), catalog)
    south, lat_i1 = snap_latitude(parse_float(payload.get("south"), "南界"), catalog)
    if lat_i0 > lat_i1:
        raise ValueError("北界要在南界以北")
    segments = longitude_segments(
        parse_float(payload.get("west"), "西界"),
        parse_float(payload.get("east"), "东界"),
        catalog,
    )
    return north, south, lat_i0, lat_i1, segments


def snap_latitude(value: float, catalog: dict) -> tuple[float, int]:
    index = int(np.rint((catalog["lat_start"] - value) / catalog["lat_step"]))
    index = max(0, min(catalog["lat_size"] - 1, index))
    snapped = catalog["lat_start"] - catalog["lat_step"] * index
    return float(snapped), index


def snap_longitude(value: float, catalog: dict, role: str) -> tuple[float, int]:
    if value < -180 or value > 360:
        raise ValueError("经度需在 -180 到 360 之间")
    # East = 360° is the same meridian as 0°, already stored as the last grid point.
    if role == "east" and value >= 360 - 1e-8:
        last = catalog["lon_size"] - 1
        snapped = catalog["lon_start"] + catalog["lon_step"] * last
        return float(snapped), last
    grid = value
    if grid < 0:
        grid += 360
    if grid >= 360:
        grid -= 360
    index = int(np.rint((grid - catalog["lon_start"]) / catalog["lon_step"]))
    index = max(0, min(catalog["lon_size"] - 1, index))
    snapped = catalog["lon_start"] + catalog["lon_step"] * index
    return float(snapped), index


def longitude_segments(west: float, east: float, catalog: dict) -> list[dict]:
    if west < -180 or west > 360 or east < -180 or east > 360:
        raise ValueError("经度需在 -180 到 360 之间")
    if east <= west:
        raise ValueError(
            "东界要在西界以东。跨过 0° 时西界写负数，例如西 -20、东 20。"
            "跨日期变更线时东界写更大的 0–360 经度，例如西 170、东 190。"
        )
    step = float(catalog["lon_step"])
    if east - west > 360 + 1e-6:
        raise ValueError("经度范围要在 360° 以内")
    last = catalog["lon_size"] - 1
    if east - west >= 360 - 1e-6:
        snapped_west = float(catalog["lon_start"])
        snapped_east = float(catalog["lon_start"] + step * last)
        return [{"i0": 0, "i1": last, "shift": 0.0, "west": snapped_west, "east": snapped_east}]
    west_snap, i0 = snap_longitude(west, catalog, "west")
    east_snap, i1 = snap_longitude(east, catalog, "east")
    if i0 <= i1:
        return [{"i0": i0, "i1": i1, "shift": 0.0, "west": west_snap, "east": east_snap}]
    # The box crosses longitude 0 on a 0–360 grid. Keep the output increasing
    # by writing the western piece with longitude minus 360.
    left_west = west_snap - 360.0
    left_east = float(catalog["lon_start"] + step * last) - 360.0
    return [
        {"i0": i0, "i1": last, "shift": -360.0, "west": left_west, "east": left_east},
        {"i0": 0, "i1": i1, "shift": 0.0, "west": float(catalog["lon_start"]), "east": east_snap},
    ]


def match_levels(requested, catalog: dict) -> list[float]:
    if not catalog["levels"]:
        return []
    if not isinstance(requested, list) or not requested:
        raise ValueError("请至少选择一个气压层")
    chosen = []
    for item in requested:
        value = parse_float(item, "气压层")
        matches = [level for level in catalog["levels"] if abs(level - value) < 1e-6]
        if not matches:
            raise ValueError(f"没有气压层 {json_number(value)} hPa")
        if matches[0] not in chosen:
            chosen.append(matches[0])
    return chosen


def time_bounds(start: date, end: date, catalog: dict) -> tuple[int, int]:
    origin = pd.Timestamp(catalog["time_start"])
    native = catalog["native"]
    if native == "monthly":
        start_stamp = pd.Timestamp(year=start.year, month=start.month, day=int(origin.day))
        end_stamp = pd.Timestamp(year=end.year, month=end.month, day=int(origin.day))
        i0 = month_index(start_stamp, origin)
        i1 = month_index(end_stamp, origin)
    else:
        clock = origin - origin.normalize()
        if native == "hourly":
            step = pd.Timedelta(hours=1)
        elif native == "3hourly":
            step = pd.Timedelta(hours=3)
        else:
            step = pd.Timedelta(days=1)
        start_stamp = pd.Timestamp(start) + clock
        end_stamp = pd.Timestamp(end) + clock
        if native == "hourly":
            end_stamp = pd.Timestamp(end) + pd.Timedelta(hours=23)
        elif native == "3hourly":
            end_stamp = pd.Timestamp(end) + pd.Timedelta(hours=21)
        delta0 = (start_stamp - origin) / step
        delta1 = (end_stamp - origin) / step
        i0 = int(round(float(delta0)))
        i1 = int(round(float(delta1)))
        if abs(float(delta0) - i0) > 1e-6 or abs(float(delta1) - i1) > 1e-6:
            raise ValueError("日期没有落在时间轴上")
    if i0 < 0 or i1 >= int(catalog["time_size"]) or i1 < i0:
        raise ValueError("日期超出数据集覆盖范围")
    return i0, i1


def parse_selection(payload: dict, catalog: dict) -> Selection:
    names = payload.get("variables")
    if not isinstance(names, list) or not names:
        raise ValueError("请至少选择一个变量")
    known = {item["name"] for item in catalog["variables"]}
    variables = []
    for name in names:
        if not isinstance(name, str) or name not in known:
            raise ValueError(f"没有变量 {name}")
        if name not in variables:
            variables.append(name)
    levels = match_levels(payload.get("levels"), catalog)
    start = parse_day(payload.get("start"), "开始日期")
    end = parse_day(payload.get("end"), "结束日期")
    if end < start:
        raise ValueError("结束日期要在开始日期之后或同一天")
    first = date.fromisoformat(catalog["first_day"])
    last = date.fromisoformat(catalog["last_day"])
    if start < first or end > last:
        raise ValueError(f"日期需落在 {first.isoformat()} 到 {last.isoformat()} 之间")
    allowed = {item["id"] for item in catalog["resolutions"]}
    resolution = payload.get("resolution")
    if resolution not in allowed:
        labels = "、".join(item["label"] for item in catalog["resolutions"])
        raise ValueError(f"这个数据集的时间尺度是：{labels}")
    north, south, lat_i0, lat_i1, segments = region_selection(payload, catalog)
    i0, i1 = time_bounds(start, end, catalog)
    return Selection(
        dataset_id=catalog["id"],
        variables=variables,
        levels=levels,
        start=start,
        end=end,
        north=north,
        south=south,
        west=float(segments[0]["west"]),
        east=float(segments[-1]["east"]),
        resolution=resolution,
        lat_i0=lat_i0,
        lat_i1=lat_i1,
        lon_segments=segments,
        time_i0=i0,
        time_i1=i1,
    )


def span_chunks(start_index: int, end_index: int, chunk: int) -> int:
    return (end_index // chunk) - (start_index // chunk) + 1


def resolution_label(selection: Selection, catalog: dict) -> str:
    for item in catalog["resolutions"]:
        if item["id"] == selection.resolution:
            return item["label"]
    return selection.resolution


def estimate(selection: Selection, catalog: dict) -> dict:
    chunks_spec = catalog["chunks"]
    n_time_chunks = span_chunks(selection.time_i0, selection.time_i1, chunks_spec["time"])
    n_lat_chunks = span_chunks(selection.lat_i0, selection.lat_i1, chunks_spec["latitude"])
    n_lon_chunks = sum(
        span_chunks(segment["i0"], segment["i1"], chunks_spec["longitude"])
        for segment in selection.lon_segments
    )
    if selection.levels:
        level_indexes = [catalog["levels"].index(level) for level in selection.levels]
        n_level_chunks = len({index // chunks_spec["level"] for index in level_indexes})
        level_depth = n_level_chunks * chunks_spec["level"]
    else:
        n_level_chunks = 1
        level_depth = 1
    n_chunks = (
        n_time_chunks
        * n_lat_chunks
        * n_lon_chunks
        * n_level_chunks
        * len(selection.variables)
    )
    decoded_one = (
        chunks_spec["time"]
        * n_lat_chunks * chunks_spec["latitude"]
        * n_lon_chunks * chunks_spec["longitude"]
        * level_depth
        * 4
    )
    ntime = output_steps(selection, catalog["native"])
    nlevel = selection.nlevel or 1
    output_bytes = ntime * selection.nlat * selection.nlon * len(selection.variables) * nlevel * 4
    reasons = []
    if n_chunks > MAX_CHUNKS:
        reasons.append("链接超过 20000 条，请缩小范围。")
    span = selection.lon_segments
    whole = (
        selection.lat_i0 == 0
        and selection.lat_i1 == int(catalog["lat_size"]) - 1
        and len(span) == 1
        and int(span[0]["i0"]) == 0
        and int(span[0]["i1"]) == int(catalog["lon_size"]) - 1
    )
    region_text = "全球，" if whole else ""
    message = (
        f"{catalog['label']}，{region_text}{selection.start.isoformat()} 至 {selection.end.isoformat()}，"
        f"{len(selection.variables)} 个变量，{n_chunks} 条链接。"
        "每条是覆盖该日期和区域的原始分块，不截取、不平均。"
    )
    if reasons:
        message += "".join(reasons)
    return {
        "allowed": not reasons,
        "message": message,
        "output_bytes": output_bytes,
        "chunks": n_chunks,
        "nlat": selection.nlat,
        "nlon": selection.nlon,
        "ntime": ntime,
        "days": selection.days,
        "nlevel": len(selection.levels),
        "nvar": len(selection.variables),
        "decoded_one_variable": decoded_one,
        "snapped": {
            "north": selection.north,
            "west": selection.west,
            "south": selection.south,
            "east": selection.east,
        },
    }


def resolve_output_dir(value) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("请填写输出目录")
    path = Path(value.strip()).expanduser()
    if not path.is_absolute():
        raise ValueError("输出目录请用绝对路径")
    resolved = path.resolve()
    if YEARLY_OUTPUT is not None:
        try:
            resolved.relative_to(YEARLY_OUTPUT.resolve())
        except ValueError:
            pass
        else:
            raise ValueError("这个目录是已经下好的 2002–2024 年度文件，请换一个目录")
    if resolved.exists() and not resolved.is_dir():
        raise ValueError("输出路径不是文件夹")
    return resolved


def output_name(selection: Selection) -> str:
    variables = "-".join(selection.variables)
    if len(variables) > 40:
        variables = f"{len(selection.variables)}vars"
    parts = ["era5", selection.dataset_id, variables]
    if selection.levels:
        levels = "-".join(str(json_number(level)) for level in selection.levels)
        if len(levels) > 40:
            levels = f"{len(selection.levels)}lev"
        parts.append(f"{levels}hPa")
    parts.append(selection.start.isoformat())
    parts.append(selection.end.isoformat())
    name = "_".join(parts) + ".txt"
    if any(char in name for char in '\\/:*?"<>|'):
        raise ValueError("文件名无效")
    return name


def encode_chunk_key(style: str, separator: str, coords: tuple[int, ...]) -> str:
    if style == "default" and separator in {"/", "."}:
        return separator.join(str(part) for part in ("c", *coords))
    if style == "v2" and separator in {"/", "."}:
        body = separator.join(str(part) for part in coords)
        return body or "0"
    raise RuntimeError("这个数据集的分块地址无法写成链接")


def shard_ids(ranges: list[tuple[int, int]], size: int) -> list[int]:
    found: set[int] = set()
    for start, end in ranges:
        found.update(range(start // size, end // size + 1))
    return sorted(found)


def dimension_ranges(dim: str, selection: Selection, catalog: dict) -> list[tuple[int, int]]:
    if dim == catalog["time_name"]:
        return [(selection.time_i0, selection.time_i1)]
    if dim == catalog["lat_name"]:
        return [(selection.lat_i0, selection.lat_i1)]
    if dim == catalog["lon_name"]:
        return [(int(segment["i0"]), int(segment["i1"])) for segment in selection.lon_segments]
    if dim == catalog["level_name"]:
        return [(catalog["levels"].index(level), catalog["levels"].index(level)) for level in selection.levels]
    if int(catalog["var_sizes"].get(dim, 0)) == 1:
        return [(0, 0)]
    raise RuntimeError("变量含有无法选择的维度")


def read_layout(key: str, url: str, names: list[str], dims: list[str]) -> dict:
    import zarr

    group = zarr.open_group(url, mode="r", storage_options=storage_options(key))
    layout = None
    try:
        for name in names:
            array = group[name]
            encoding = array.metadata.chunk_key_encoding
            outer = array.shards if array.shards is not None else array.chunks
            current = {
                "style": str(encoding.name),
                "separator": str(encoding.separator),
                "shape": [int(value) for value in outer],
            }
            if len(current["shape"]) != len(dims) or any(value <= 0 for value in current["shape"]):
                raise RuntimeError(f"{name} 的分块无法写成链接")
            if layout is None:
                layout = current
            elif current != layout:
                raise RuntimeError(f"{name} 的分块和其他变量不同")
        if layout is None:
            raise RuntimeError("没有可写出的变量")
        return layout
    finally:
        closer = getattr(getattr(group, "store", None), "close", None)
        if closer is not None:
            result = closer()
            if hasattr(result, "close"):
                result.close()


def authenticated_root(url: str, key: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.netloc != EDH_DATA_HOST or parsed.username or parsed.query:
        raise RuntimeError("数据集地址无效")
    return f"https://edh:{quote(key, safe='')}@{parsed.netloc}{parsed.path.rstrip('/')}"


def link_lines(selection: Selection, catalog: dict, key: str) -> list[str]:
    dims = [str(dim) for dim in catalog["var_dims"]]
    layout = read_layout(key, catalog["url"], selection.variables, dims)
    axes = [
        shard_ids(dimension_ranges(dim, selection, catalog), size)
        for dim, size in zip(dims, layout["shape"], strict=True)
    ]
    root = authenticated_root(catalog["url"], key)
    lines: list[str] = []
    for name in selection.variables:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise RuntimeError("变量名无法写成链接")
        for coords in product(*axes):
            key_text = encode_chunk_key(layout["style"], layout["separator"], coords)
            lines.append(f"{root}/{name}/{key_text}")
    if len(lines) > MAX_CHUNKS:
        raise RuntimeError("链接超过 20000 条，请缩小范围。")
    return lines


def write_links(selection: Selection, catalog: dict, key: str, directory: Path) -> tuple[Path, int]:
    destination = directory / output_name(selection)
    if destination.exists():
        raise ValueError(f"目标文件已存在：{destination.name}。请换目录或先移走该文件。")
    lines = link_lines(selection, catalog, key)
    temporary = destination.with_name(destination.name + ".writing")
    try:
        temporary.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
        temporary.replace(destination)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return destination, len(lines)


def time_blocks(selection: Selection, catalog: dict):
    chunk = catalog["chunks"]["time"]
    first = selection.time_i0 // chunk
    last = selection.time_i1 // chunk
    origin = pd.Timestamp(catalog["time_start"])
    for block in range(first, last + 1):
        start = max(selection.time_i0, block * chunk)
        end = min(selection.time_i1, (block + 1) * chunk - 1)
        yield start, end, index_to_stamp(origin, catalog["native"], start), index_to_stamp(
            origin, catalog["native"], end
        )


def assert_complete(index: pd.DatetimeIndex, selection: Selection, catalog: dict) -> None:
    native = catalog["native"]
    if native == "hourly":
        expected = pd.date_range(
            f"{selection.start.isoformat()}T00:00:00",
            f"{selection.end.isoformat()}T23:00:00",
            freq="h",
        )
        observed = pd.Series(1, index=index).resample("1D").sum()
        observed = observed.reindex(pd.date_range(selection.start, selection.end, freq="D"), fill_value=0)
        bad = observed[observed != 24]
        if len(bad):
            sample = ", ".join(stamp.strftime("%Y-%m-%d") for stamp in list(bad.index[:3]))
            raise RuntimeError(
                f"有 {len(bad)} 天不是 24 个时次（例如 {sample}）。需要当天 00–23 UTC 都在。"
            )
        if len(index) != len(expected) or not index.equals(expected):
            raise RuntimeError("时次不连续")
        return
    if native == "3hourly":
        expected = pd.date_range(
            f"{selection.start.isoformat()}T00:00:00",
            f"{selection.end.isoformat()}T21:00:00",
            freq="3h",
        )
        observed = pd.Series(1, index=index).resample("1D").sum()
        observed = observed.reindex(pd.date_range(selection.start, selection.end, freq="D"), fill_value=0)
        bad = observed[observed != 8]
        if len(bad):
            sample = ", ".join(stamp.strftime("%Y-%m-%d") for stamp in list(bad.index[:3]))
            raise RuntimeError(
                f"有 {len(bad)} 天不是 8 个时次（例如 {sample}）。需要当天 00–21 UTC 的 3 小时分析都在。"
            )
        if len(index) != len(expected) or not index.equals(expected):
            raise RuntimeError("时次不连续")
        return
    if native == "daily":
        expected = pd.date_range(selection.start, selection.end, freq="D")
        observed = pd.DatetimeIndex(index).normalize()
        if len(observed) != len(expected) or not observed.equals(expected):
            raise RuntimeError("有日期不在数据集里，或这些日期不是逐日连续")
        return
    expected = pd.period_range(selection.start.isoformat(), selection.end.isoformat(), freq="M")
    observed = pd.DatetimeIndex(index).to_period("M")
    if len(observed) != len(expected) or not observed.equals(pd.PeriodIndex(expected)):
        raise RuntimeError("有月份不在数据集里，或这些月份不是逐月连续")


def load_piece(piece: xr.Dataset) -> xr.Dataset:
    import dask

    with dask.config.set({"scheduler": "threads", "num_workers": 4}):
        return piece.load()


def spatial_subset(store: xr.Dataset, selection: Selection, catalog: dict, i0: int, i1: int) -> xr.Dataset:
    pieces = []
    lon_name = catalog["lon_name"]
    for segment in selection.lon_segments:
        piece = store[selection.variables].isel(
            {
                catalog["lat_name"]: slice(selection.lat_i0, selection.lat_i1 + 1),
                lon_name: slice(segment["i0"], segment["i1"] + 1),
                catalog["time_name"]: slice(i0, i1 + 1),
            }
        )
        if segment["shift"]:
            piece = piece.assign_coords({lon_name: piece[lon_name] + segment["shift"]})
        pieces.append(piece)
    data = pieces[0] if len(pieces) == 1 else xr.concat(pieces, dim=lon_name)
    if catalog["level_name"]:
        data = data.sel({catalog["level_name"]: list(selection.levels)})
    return data


def apply_time_reduction(piece: xr.Dataset, selection: Selection, catalog: dict) -> xr.Dataset:
    native = catalog["native"]
    resolution = selection.resolution
    time_name = catalog["time_name"]
    if native == "3hourly" and resolution == "6hourly":
        hours = pd.DatetimeIndex(pd.to_datetime(piece[time_name].values)).hour.to_numpy()
        keep = np.flatnonzero(hours % 6 == 0)
        if keep.size == 0:
            raise RuntimeError("这个时间块里没有对应的分析时次")
        return piece.isel({time_name: keep})
    if native == "3hourly" and resolution in {"daily", "monthly"}:
        return piece.resample({time_name: "1D"}).mean(skipna=False)
    if native == "hourly" and resolution in {"3hourly", "6hourly"}:
        hours = pd.DatetimeIndex(pd.to_datetime(piece[time_name].values)).hour.to_numpy()
        step = 3 if resolution == "3hourly" else 6
        keep = np.flatnonzero(hours % step == 0)
        if keep.size == 0:
            raise RuntimeError("这个时间块里没有对应的分析时次")
        return piece.isel({time_name: keep})
    if native == "hourly" and resolution in {"daily", "monthly"}:
        return piece.resample({time_name: "1D"}).mean(skipna=False)
    return piece


def read_slice(store: xr.Dataset, selection: Selection, catalog: dict, i0: int, i1: int, split: bool):
    base = spatial_subset(store, selection, catalog, i0, i1)

    def one(names: list[str]) -> xr.Dataset:
        piece = apply_time_reduction(base[names], selection, catalog)
        return load_piece(piece)

    if not split:
        return one(selection.variables)
    return xr.merge([one([name]) for name in selection.variables], compat="override")


def tidy(dataset: xr.Dataset, selection: Selection, catalog: dict) -> xr.Dataset:
    rename = {}
    if catalog["time_name"] != "time":
        rename[catalog["time_name"]] = "time"
    if catalog["lat_name"] != "latitude":
        rename[catalog["lat_name"]] = "latitude"
    if catalog["lon_name"] != "longitude":
        rename[catalog["lon_name"]] = "longitude"
    if rename:
        dataset = dataset.rename(rename)
    level_name = catalog["level_name"]
    keep = ["time", "latitude", "longitude"] + ([level_name] if level_name else [])
    for name, size in list(dataset.sizes.items()):
        if name not in keep and size == 1:
            dataset = dataset.squeeze(name, drop=True)
    extra = [name for name in dataset.coords if name not in keep]
    if extra:
        dataset = dataset.drop_vars(extra)
    order = ["time"] + ([level_name] if level_name else []) + ["latitude", "longitude"]
    dataset = dataset.transpose(*order)
    for name in list(dataset.variables):
        source = dict(dataset[name].attrs)
        if name in dataset.data_vars:
            kept = {
                "units": str(source.get("units", "")),
                "long_name": str(source.get("long_name", name)),
            }
            if name in STANDARD_NAMES:
                kept["standard_name"] = STANDARD_NAMES[name]
            dataset[name].attrs = kept
        else:
            kept = {}
            for key in ("units", "long_name", "standard_name", "axis", "positive"):
                if key in source and "http" not in str(source[key]).lower():
                    kept[key] = str(source[key])
            dataset[name].attrs = kept
        dataset[name].encoding = {}
    dataset.encoding = {}
    statistic = statistic_text(catalog["native"], selection.resolution)
    attrs = {
        "source": catalog["source"],
        "time_zone": "utc+00:00",
        "area_north_west_south_east": (
            f"{selection.north} {selection.west} {selection.south} {selection.east}"
        ),
        "variables": ", ".join(selection.variables),
        "resolution": selection.resolution,
        "dataset": catalog["id"],
        "statistic": statistic,
        "history": "edh_era5_picker",
    }
    if selection.levels and level_name:
        attrs["pressure_levels_hPa"] = ", ".join(str(json_number(level)) for level in selection.levels)
    if statistic == DAILY_ATTR:
        attrs["daily_statistic"] = DAILY_ATTR
    dataset.attrs = attrs
    return dataset


def assert_no_secret(path: Path) -> None:
    with path.open("rb") as handle:
        head = handle.read(8 * 1024 * 1024)
    with SECRET_LOCK:
        secrets_now = list(SECRETS)
    for secret in secrets_now:
        if secret and secret.encode("utf-8") in head:
            path.unlink(missing_ok=True)
            raise RuntimeError("输出文件含有 key，已删除")


def validate_file(path: Path, selection: Selection, catalog: dict) -> None:
    expected_steps = output_steps(selection, catalog["native"])
    level_name = catalog["level_name"]
    dims = ("time",) + ((level_name,) if level_name else ()) + ("latitude", "longitude")
    statistic = statistic_text(catalog["native"], selection.resolution)
    with xr.open_dataset(path) as dataset:
        if int(dataset.sizes.get("time", -1)) != expected_steps:
            raise RuntimeError(f"时次数量是 {dataset.sizes.get('time')}，应为 {expected_steps}")
        if int(dataset.sizes.get("latitude", -1)) != selection.nlat:
            raise RuntimeError("纬度格点数和请求不一致")
        if int(dataset.sizes.get("longitude", -1)) != selection.nlon:
            raise RuntimeError("经度格点数和请求不一致")
        latitude = dataset["latitude"].values
        longitude = dataset["longitude"].values
        if not np.allclose(latitude[[0, -1]], [selection.north, selection.south]):
            raise RuntimeError("纬度端点和请求不一致")
        if not np.allclose(longitude[[0, -1]], [selection.west, selection.east]):
            raise RuntimeError("经度端点和请求不一致")
        if selection.nlat > 1 and not np.allclose(np.diff(latitude), -catalog["lat_step"]):
            raise RuntimeError("纬度间距不对")
        if selection.nlon > 1 and not np.allclose(np.diff(longitude), catalog["lon_step"]):
            raise RuntimeError("经度间距不对")
        if selection.levels:
            levels = [float(value) for value in dataset[level_name].values]
            if len(levels) != len(selection.levels) or any(
                abs(left - right) > 1e-6 for left, right in zip(levels, selection.levels)
            ):
                raise RuntimeError("气压层和请求不一致")
        elif level_name and level_name in dataset.dims:
            raise RuntimeError("单层数据集不应写出气压层")
        for name in selection.variables:
            if tuple(dataset[name].dims) != dims:
                raise RuntimeError(f"{name} 的维度是 {tuple(dataset[name].dims)}")
            if not np.isfinite(dataset[name].values).all():
                raise RuntimeError(f"{name} 含有缺测或非有限值")
        if dataset.attrs.get("statistic") != statistic:
            raise RuntimeError("时间尺度说明没有写入")
        if statistic == DAILY_ATTR and dataset.attrs.get("daily_statistic") != DAILY_ATTR:
            raise RuntimeError("日平均说明没有写入")
    assert_no_secret(path)


def perform(job: Job, session: Session, selection: Selection, catalog: dict, plan: dict) -> None:
    destination = job.destination
    temporary = job.temporary
    temporary.unlink(missing_ok=True)
    blocks = list(time_blocks(selection, catalog))
    job.update(total=len(blocks), done=0, message="正在打开数据集")
    split = plan["decoded_one_variable"] * len(selection.variables) > MAX_BLOCK_BYTES
    store = open_store(session.key, catalog["url"])
    pieces: list[xr.Dataset] = []
    replaced = False
    time_name = catalog["time_name"]
    try:
        if job.cancel.is_set():
            raise RuntimeError("已取消")
        index = pd.DatetimeIndex(
            pd.to_datetime(
                store[time_name].isel({time_name: slice(selection.time_i0, selection.time_i1 + 1)}).values
            )
        )
        if index.tz is not None:
            index = index.tz_convert("UTC").tz_localize(None)
        assert_complete(index, selection, catalog)
        for number, (i0, i1, t0, t1) in enumerate(blocks, start=1):
            if job.cancel.is_set():
                raise RuntimeError("已取消")
            label = f"{t0.strftime('%Y-%m-%d')}–{t1.strftime('%Y-%m-%d')}"
            job.update(message=f"正在读取 {label}（{number}/{len(blocks)}）")
            loaded = None
            for attempt in (1, 2):
                try:
                    loaded = read_slice(store, selection, catalog, i0, i1, split)
                    break
                except Exception as error:
                    if attempt == 2 or job.cancel.is_set():
                        raise
                    log(f"RETRY\t{label}\tattempt={attempt}\t{public_error(error)}")
                    store.close()
                    store = open_store(session.key, catalog["url"])
            pieces.append(loaded)
            del loaded
            gc.collect()
            job.update(done=number)
        if job.cancel.is_set():
            raise RuntimeError("已取消")
        combined = pieces[0] if len(pieces) == 1 else xr.concat(pieces, dim=time_name)
        del pieces
        if selection.resolution == "monthly" and catalog["native"] != "monthly":
            combined = combined.resample({time_name: "MS"}).mean(skipna=False)
            combined = combined.load()
        dataset = tidy(combined, selection, catalog)
        del combined
        job.update(message="正在写入文件")
        encoding = {
            name: {"dtype": "float32", "zlib": True, "complevel": 4, "_FillValue": None}
            for name in selection.variables
        }
        dataset.to_netcdf(temporary, engine="netcdf4", encoding=encoding)
        dataset.close()
        del dataset
        if job.cancel.is_set():
            raise RuntimeError("已取消")
        temporary.replace(destination)
        replaced = True
        validate_file(destination, selection, catalog)
        job.update(
            state="done",
            message="已保存",
            path=str(destination),
            nbytes=destination.stat().st_size,
        )
        log(f"WROTE\t{destination}\tbytes={destination.stat().st_size}")
    except Exception:
        temporary.unlink(missing_ok=True)
        if replaced:
            destination.unlink(missing_ok=True)
        raise
    finally:
        store.close()
        gc.collect()


def run_job(job: Job, session: Session, selection: Selection, catalog: dict, plan: dict) -> None:
    try:
        perform(job, session, selection, catalog, plan)
    except Exception as error:
        cancelled = job.cancel.is_set() or public_error(error) == "已取消"
        if cancelled:
            job.temporary.unlink(missing_ok=True)
            job.update(state="cancelled", message="已取消")
            log("CANCELLED")
            return
        log("ERROR\t" + scrub(traceback.format_exc()))
        job.temporary.unlink(missing_ok=True)
        job.update(state="error", message=public_error(error))


def load_catalog(key: str, product: dict) -> dict:
    store = open_store(key, product["url"])
    try:
        return build_catalog(store, product)
    finally:
        store.close()


def catalog_payload(catalog: dict) -> dict:
    return {
        "dataset": catalog["id"],
        "label": catalog["label"],
        "store": catalog["store"],
        "native": catalog["native"],
        "note": catalog["note"],
        "grid": catalog["grid"],
        "time_start": catalog["time_start"],
        "time_end": catalog["time_end"],
        "first_day": catalog["first_day"],
        "last_day": catalog["last_day"],
        "levels": [json_number(level) for level in catalog["levels"]],
        "variables": catalog["variables"],
        "resolutions": catalog["resolutions"],
        "latitude": {
            "start": catalog["lat_start"],
            "end": catalog["lat_start"] - catalog["lat_step"] * (catalog["lat_size"] - 1),
            "step": -catalog["lat_step"],
            "size": catalog["lat_size"],
        },
        "longitude": {
            "start": catalog["lon_start"],
            "end": catalog["lon_start"] + catalog["lon_step"] * (catalog["lon_size"] - 1),
            "step": catalog["lon_step"],
            "size": catalog["lon_size"],
        },
        "default_output": str(DEFAULT_OUTPUT),
    }


def prune_sessions() -> None:
    cutoff = datetime.now().timestamp() - 12 * 3600
    with STATE_LOCK:
        expired = [token for token, session in SESSIONS.items() if session.created < cutoff]
        if len(SESSIONS) > 8:
            ordered = sorted(SESSIONS.items(), key=lambda item: item[1].created)
            expired.extend(token for token, _ in ordered[: len(SESSIONS) - 8])
        for token in set(expired):
            SESSIONS.pop(token, None)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args) -> None:
        log(scrub(fmt % args))

    def _send(self, code: int, payload: dict | None = None, body: bytes | None = None, content_type: str = "") -> None:
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            content_type = "application/json; charset=utf-8"
        if body is None:
            body = b""
        self.send_response(code)
        self.send_header("Content-Type", content_type or "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _error(self, code: int, message: str) -> None:
        self._send(code, {"error": scrub(message)})

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length > 1_000_000:
            raise ValueError("请求太大")
        raw = self.rfile.read(length) if length else b""
        if not raw:
            return {}
        try:
            payload = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as error:
            raise ValueError("请求不是 JSON") from error
        if not isinstance(payload, dict):
            raise ValueError("请求不是 JSON 对象")
        return payload

    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path == "/favicon.ico":
            self._send(204, body=b"", content_type="image/x-icon")
            return
        if path == "/":
            if not INDEX_PATH.exists():
                self._error(500, "页面文件缺失")
                return
            body = INDEX_PATH.read_bytes()
            self._send(200, body=body, content_type="text/html; charset=utf-8")
            return
        if path == "/api/status":
            query = urlparse(self.path).query
            job_id = ""
            for part in query.split("&"):
                if part.startswith("job="):
                    job_id = part[4:]
            with STATE_LOCK:
                job = CURRENT["job"]
            if job is None or (job_id and job.id != job_id):
                self._send(200, {"state": "idle"})
                return
            self._send(200, job.snapshot())
            return
        self._error(404, "没有这个地址")

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        try:
            payload = self._read_json()
            if path == "/api/connect":
                self._connect(payload)
            elif path == "/api/open":
                self._open(payload)
            elif path == "/api/estimate":
                self._estimate(payload)
            elif path == "/api/download":
                self._download(payload)
            elif path == "/api/cancel":
                self._cancel(payload)
            else:
                self._error(404, "没有这个地址")
        except RequestError as error:
            self._error(error.code, str(error))
        except ValueError as error:
            self._error(400, str(error))
        except Exception as error:
            message = public_error(error)
            if message == "key 无效或没有权限":
                log("CONNECT\trejected")
                self._error(401, message)
                return
            log("ERROR\t" + scrub(traceback.format_exc()))
            self._error(500, message)

    def _connect(self, payload: dict) -> None:
        key = payload.get("key", "")
        if not isinstance(key, str) or len(key.strip()) < 8:
            raise ValueError("请填写 key")
        key = key.strip()
        remember(key)
        products = discover_products()
        default_id = DEFAULT_DATASET if DEFAULT_DATASET in products else next(iter(products))
        catalog = load_catalog(key, products[default_id])
        token = secrets.token_urlsafe(24)
        with STATE_LOCK:
            SESSIONS[token] = Session(
                key=key,
                catalogs={catalog["id"]: catalog},
                products=products,
                created=datetime.now().timestamp(),
            )
        prune_sessions()
        log(
            "CONNECT\t"
            f"{catalog['id']}\tvariables={len(catalog['variables'])}\t"
            f"levels={len(catalog['levels'])}\t"
            f"chunks={catalog['chunks']['time']},{catalog['chunks']['level']},"
            f"{catalog['chunks']['latitude']},{catalog['chunks']['longitude']}"
        )
        body = catalog_payload(catalog)
        body["session"] = token
        body["datasets"] = dataset_menu(products)
        self._send(200, body)

    def _open(self, payload: dict) -> None:
        session = require_session(payload)
        dataset_id = payload.get("dataset")
        if not isinstance(dataset_id, str) or dataset_id not in session.products:
            raise ValueError("请选择数据集")
        with STATE_LOCK:
            catalog = session.catalogs.get(dataset_id)
        if catalog is None:
            catalog = load_catalog(session.key, session.products[dataset_id])
            with STATE_LOCK:
                current = SESSIONS.get(payload.get("session"))
                if current is None:
                    raise ValueError("请重新接入")
                current.catalogs[dataset_id] = catalog
            log(
                "OPEN\t"
                f"{catalog['id']}\tvariables={len(catalog['variables'])}\t"
                f"levels={len(catalog['levels'])}\tnative={catalog['native']}"
            )
        self._send(200, catalog_payload(catalog))

    def _estimate(self, payload: dict) -> None:
        session = require_session(payload)
        catalog = catalog_for(session, payload)
        selection = parse_selection(payload, catalog)
        plan = estimate(selection, catalog)
        log(
            "ESTIMATE\t"
            f"{selection.dataset_id}\t{','.join(selection.variables)}\t{selection.resolution}\t"
            f"{selection.start.isoformat()}..{selection.end.isoformat()}\t"
            f"chunks={plan['chunks']}\tbytes={plan['output_bytes']}\tallowed={int(plan['allowed'])}"
        )
        self._send(200, plan)

    def _download(self, payload: dict) -> None:
        session = require_session(payload)
        catalog = catalog_for(session, payload)
        selection = parse_selection(payload, catalog)
        plan = estimate(selection, catalog)
        if not plan["allowed"]:
            raise ValueError(plan["message"])
        directory = resolve_output_dir(payload.get("output_dir"))
        directory.mkdir(parents=True, exist_ok=True)
        destination, count = write_links(selection, catalog, session.key, directory)
        log(f"LINKS\t{destination.name}\tcount={count}")
        self._send(200, {"path": str(destination), "count": count})

    def _cancel(self, payload: dict) -> None:
        job_id = payload.get("job")
        with STATE_LOCK:
            job = CURRENT["job"]
        if job is None or job.id != job_id:
            raise ValueError("没有正在进行的下载")
        job.cancel.set()
        job.update(message="正在取消")
        log("CANCEL\trequested")
        self._send(200, {"ok": True})


def tell_user(message: str) -> None:
    if not getattr(sys, "frozen", False):
        return
    try:
        import ctypes

        ctypes.windll.user32.MessageBoxW(None, scrub(message), "ERA5", 0x10)
    except Exception:
        pass


def listen(port: int | None) -> ThreadingHTTPServer:
    candidates = [8765, 8766] if port is None else [port]
    last_error: Exception | None = None
    for candidate in candidates:
        try:
            return ThreadingHTTPServer((HOST, candidate), Handler)
        except OSError as error:
            last_error = error
            log(f"PORT\t{candidate}\tbusy")
    tell_user(f"端口打不开：{last_error}")
    raise SystemExit(f"端口打不开：{last_error}")


def main() -> None:
    _install_log_filter()
    parser = argparse.ArgumentParser(description="Local Earth Data Hub ERA5 picker.")
    parser.add_argument("--port", type=int)
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args()
    DEFAULT_OUTPUT.mkdir(parents=True, exist_ok=True)
    server = listen(args.port)
    host, port = server.server_address[:2]
    url = f"http://{host}:{port}"
    log(f"LISTEN\t{url}")
    if not args.no_browser:
        threading.Timer(0.3, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        with STATE_LOCK:
            job = CURRENT["job"]
        if job is not None:
            job.cancel.set()
        log("STOP")
    finally:
        server.server_close()


if __name__ == "__main__":
    import multiprocessing

    multiprocessing.freeze_support()
    try:
        main()
    except SystemExit:
        raise
    except Exception as error:
        tell_user(str(error))
        raise
