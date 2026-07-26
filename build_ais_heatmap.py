# -*- coding: utf-8 -*-
"""
AIS raster-density heatmap builder.

Supported metrics:
- track_km: estimated vessel track kilometres distributed along valid segments.
- vessel_hours: elapsed vessel-hours distributed along valid segments.
- point_count: qualifying AIS observations counted in their containing pixels.

All runtime settings are loaded from config.env beside this script.

Created on Sat May 23 14:07:17 2026
@author: Craig Pearce
"""

from __future__ import annotations

import argparse
import json
import io
import math
import os
import re
import shutil
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path
from typing import Iterable

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from PIL import Image
from tqdm import tqdm
from dotenv import dotenv_values

# Rasterio is used for the optional analytical Float32 GeoTIFF tile output.
# The visual MBTiles/XYZ output does not require it.
try:
    import rasterio
    from rasterio.transform import from_origin
    from rasterio.windows import Window
except ImportError:  # pragma: no cover - handled at runtime when GeoTIFF output is requested
    rasterio = None
    from_origin = None
    Window = None


TILE_SIZE = 256
WEB_MERCATOR_MAX_LAT = 85.0511287798066
EARTH_RADIUS_KM = 6371.0088
WEB_MERCATOR_RADIUS_M = 6378137.0
WEB_MERCATOR_HALF_WORLD_M = math.pi * WEB_MERCATOR_RADIUS_M

CONFIG_PATH = Path(__file__).resolve().parent / "config.env"
_NONE_VALUES = {"", "none", "null"}
SUPPORTED_METRICS = frozenset({"track_km", "vessel_hours", "point_count"})
SEGMENT_METRICS = frozenset({"track_km", "vessel_hours"})


def metric_description(metric: str) -> str:
    descriptions = {
        "track_km": "Accumulated vessel track kilometres per Web Mercator pixel",
        "vessel_hours": "Accumulated vessel-hours per Web Mercator pixel",
        "point_count": (
            "Count of qualifying AIS position reports per Web Mercator pixel"
        ),
    }

    try:
        return descriptions[metric]
    except KeyError as exc:
        raise ValueError(
            f"Unsupported metric {metric!r}. Expected one of: "
            f"{', '.join(sorted(SUPPORTED_METRICS))}."
        ) from exc


def _config_value(
    config: dict[str, str | None],
    key: str,
    default: str | None = None,
    *,
    required: bool = False,
) -> str | None:
    raw = config.get(key)

    if raw is None or str(raw).strip() == "":
        if required and default is None:
            raise ValueError(f"Missing required config.env setting: {key}")
        return default

    return str(raw).strip()


def _config_string(
    config: dict[str, str | None],
    key: str,
    default: str | None = None,
    *,
    required: bool = False,
) -> str | None:
    value = _config_value(config, key, default, required=required)
    if value is None or value.lower() in _NONE_VALUES:
        return None
    return value


def _config_bool(
    config: dict[str, str | None],
    key: str,
    default: bool,
) -> bool:
    value = _config_value(config, key, str(default))
    normalized = str(value).strip().lower()

    if normalized in {"true", "1", "yes", "y", "on"}:
        return True
    if normalized in {"false", "0", "no", "n", "off"}:
        return False

    raise ValueError(
        f"Invalid boolean for {key}: {value!r}. Use true or false."
    )


def _config_int(
    config: dict[str, str | None],
    key: str,
    default: int,
) -> int:
    value = _config_value(config, key, str(default))
    try:
        return int(str(value).replace("_", ""))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid integer for {key}: {value!r}") from exc


def _config_optional_int(
    config: dict[str, str | None],
    key: str,
    default: int | None = None,
) -> int | None:
    fallback = "None" if default is None else str(default)
    value = _config_value(config, key, fallback)

    if value is None or str(value).strip().lower() in _NONE_VALUES:
        return None

    try:
        return int(str(value).replace("_", ""))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid integer or None for {key}: {value!r}") from exc


def _config_float(
    config: dict[str, str | None],
    key: str,
    default: float,
) -> float:
    value = _config_value(config, key, str(default))
    try:
        return float(str(value).replace("_", ""))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid number for {key}: {value!r}") from exc


def _config_path(
    config: dict[str, str | None],
    key: str,
    config_dir: Path,
    default: str | None = None,
    *,
    required: bool = False,
) -> str | None:
    value = _config_string(config, key, default, required=required)
    if value is None:
        return None

    value = os.path.expandvars(os.path.expanduser(value))

    # Preserve Windows drive and UNC paths even if validation is performed on
    # another operating system. Resolve ordinary relative paths beside config.env.
    is_windows_absolute = bool(re.match(r"^[A-Za-z]:[\\/]", value))
    is_unc = value.startswith("\\\\") or value.startswith("//")
    path = Path(value)

    if not path.is_absolute() and not is_windows_absolute and not is_unc:
        path = (config_dir / path).resolve()
        return str(path)

    return value


def _config_list(
    config: dict[str, str | None],
    key: str,
    default: list[str],
) -> list[str]:
    value = _config_value(config, key, ",".join(default))
    items = [item.strip() for item in str(value).split(",") if item.strip()]

    if not items:
        raise ValueError(f"{key} must contain at least one value.")

    return items


def _config_value_output_zooms(
    config: dict[str, str | None],
    max_zoom: int,
) -> list[int] | None:
    value = _config_value(config, "VALUE_OUTPUT_ZOOMS", "max")
    normalized = str(value).strip().lower()

    if normalized in {"max", "maximum"}:
        return [max_zoom]
    if normalized in {"all", "*"}:
        return []
    if normalized in _NONE_VALUES:
        return None

    try:
        return [
            int(item.strip().replace("_", ""))
            for item in str(value).split(",")
            if item.strip()
        ]
    except ValueError as exc:
        raise ValueError(
            "VALUE_OUTPUT_ZOOMS must be max, all, None, or a comma-separated "
            "list such as 9,10,11."
        ) from exc


def validate_config_args(args: argparse.Namespace) -> None:
    if args.metric not in SUPPORTED_METRICS:
        raise ValueError(
            "METRIC must be one of: track_km, vessel_hours, point_count."
        )

    if args.min_zoom < 0 or args.max_zoom < args.min_zoom:
        raise ValueError("MIN_ZOOM and MAX_ZOOM define an invalid zoom range.")

    if args.speed_scale <= 0:
        raise ValueError("SPEED_SCALE must be greater than zero.")
    if args.min_speed_knots < 0:
        raise ValueError("MIN_SPEED_KNOTS cannot be negative.")

    if args.metric in SEGMENT_METRICS:
        if args.max_gap_minutes < 1:
            raise ValueError("MAX_GAP_MINUTES must be at least 1.")
        if args.max_implied_speed_knots <= 0:
            raise ValueError(
                "MAX_IMPLIED_SPEED_KNOTS must be greater than zero."
            )
        if args.sample_step_px <= 0:
            raise ValueError("SAMPLE_STEP_PX must be greater than zero.")
        if args.max_segment_samples < 1:
            raise ValueError("MAX_SEGMENT_SAMPLES must be at least 1.")

    positive_int_settings = {
        "THREADS": args.threads,
        "SHIP_ID_SHARDS": args.ship_id_shards,
        "PIXEL_FLUSH_THRESHOLD": args.pixel_flush_threshold,
        "SEGMENT_BATCH_SIZE": args.segment_batch_size,
    }
    for name, value in positive_int_settings.items():
        if value < 1:
            raise ValueError(f"{name} must be at least 1.")

    if args.blur_radius < 0:
        raise ValueError("BLUR_RADIUS cannot be negative.")
    if not 0 < args.colour_quantile <= 1:
        raise ValueError("COLOUR_QUANTILE must be greater than 0 and at most 1.")
    if not 0 <= args.alpha_min <= args.alpha_max <= 255:
        raise ValueError("ALPHA_MIN and ALPHA_MAX must satisfy 0 <= min <= max <= 255.")

    groups_upper = [group.upper() for group in args.groups]
    if "ALL" in groups_upper and len(groups_upper) > 1:
        raise ValueError("GROUPS cannot combine ALL with specific vessel groups.")

    if args.output_name and ("/" in args.output_name or "\\" in args.output_name):
        raise ValueError("OUTPUT_NAME must be a name, not a path.")

    if args.value_output_zooms:
        invalid_zooms = [
            zoom
            for zoom in args.value_output_zooms
            if zoom < args.min_zoom or zoom > args.max_zoom
        ]
        if invalid_zooms:
            raise ValueError(
                "VALUE_OUTPUT_ZOOMS contains zooms outside MIN_ZOOM..MAX_ZOOM: "
                f"{invalid_zooms}"
            )

    if args.value_tiff_tile_limit is not None and args.value_tiff_tile_limit < 1:
        raise ValueError("VALUE_TIFF_TILE_LIMIT must be None or at least 1.")

    if not any(
        [
            args.write_xyz,
            args.write_mbtiles,
            args.write_value_pixels,
            args.write_value_tiffs,
            args.write_value_composite_geotiff,
        ]
    ):
        raise ValueError("At least one WRITE_* output setting must be true.")

    if (args.write_value_tiffs or args.write_value_composite_geotiff) and rasterio is None:
        raise RuntimeError(
            "WRITE_VALUE_TIFFS or WRITE_VALUE_COMPOSITE_GEOTIFF is true, but "
            "rasterio is not installed. Install requirements.txt or disable those outputs."
        )


def load_args_from_config(config_path: Path = CONFIG_PATH) -> argparse.Namespace:
    """Load all former Spyder/command-line settings from config.env."""
    config_path = config_path.resolve()

    if not config_path.is_file():
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}. "
            "Place config.env beside this script."
        )

    # Disable interpolation so literal dollar signs in paths or values are preserved.
    config = dict(dotenv_values(config_path, interpolate=False))
    config_dir = config_path.parent

    min_zoom = _config_int(config, "MIN_ZOOM", 0)
    max_zoom = _config_int(config, "MAX_ZOOM", 11)

    args = argparse.Namespace(
        ais_folder=_config_path(
            config, "AIS_FOLDER", config_dir, required=True
        ),
        vessel_csv=_config_path(
            config, "VESSEL_CSV", config_dir, required=True
        ),
        output_dir=_config_path(
            config, "OUTPUT_DIR", config_dir, required=True
        ),
        output_name=_config_string(config, "OUTPUT_NAME", None),
        ship_id_column=_config_string(config, "SHIP_ID_COLUMN", "SHIP_ID"),
        lat_column=_config_string(config, "LAT_COLUMN", "LAT"),
        lon_column=_config_string(config, "LON_COLUMN", "LON"),
        speed_column=_config_string(config, "SPEED_COLUMN", "SPEED"),
        timestamp_column=_config_string(config, "TIMESTAMP_COLUMN", "TIMESTAMP"),
        vessel_ship_id_column=_config_string(
            config, "VESSEL_SHIP_ID_COLUMN", "SHIP_ID"
        ),
        vessel_group_column=_config_string(
            config, "VESSEL_GROUP_COLUMN", "COMFLEET_GROUPEDTYPE"
        ),
        groups=_config_list(config, "GROUPS", ["DRY BULK"]),
        metric=_config_string(config, "METRIC", "track_km"),
        speed_scale=_config_float(config, "SPEED_SCALE", 10.0),
        min_speed_knots=_config_float(config, "MIN_SPEED_KNOTS", 1.0),
        require_both_endpoint_speeds=_config_bool(
            config, "REQUIRE_BOTH_ENDPOINT_SPEEDS", True
        ),
        min_zoom=min_zoom,
        max_zoom=max_zoom,
        max_gap_minutes=_config_int(config, "MAX_GAP_MINUTES", 60),
        max_implied_speed_knots=_config_float(
            config, "MAX_IMPLIED_SPEED_KNOTS", 80.0
        ),
        threads=_config_int(config, "THREADS", 4),
        duckdb_memory_limit=_config_string(
            config, "DUCKDB_MEMORY_LIMIT", "32GB"
        ),
        duckdb_temp_dir=_config_path(
            config, "DUCKDB_TEMP_DIR", config_dir, None
        ),
        duckdb_preserve_insertion_order=_config_bool(
            config, "DUCKDB_PRESERVE_INSERTION_ORDER", False
        ),
        ship_id_shards=_config_int(config, "SHIP_ID_SHARDS", 8),
        sample_step_px=_config_float(config, "SAMPLE_STEP_PX", 3.0),
        max_segment_samples=_config_int(config, "MAX_SEGMENT_SAMPLES", 4096),
        pixel_flush_threshold=_config_int(
            config, "PIXEL_FLUSH_THRESHOLD", 750_000
        ),
        segment_batch_size=_config_int(config, "SEGMENT_BATCH_SIZE", 100_000),
        blur_radius=_config_int(config, "BLUR_RADIUS", 2),
        colour_quantile=_config_float(config, "COLOUR_QUANTILE", 0.995),
        alpha_min=_config_int(config, "ALPHA_MIN", 25),
        alpha_max=_config_int(config, "ALPHA_MAX", 230),
        write_xyz=_config_bool(config, "WRITE_XYZ", True),
        write_mbtiles=_config_bool(config, "WRITE_MBTILES", True),
        write_value_pixels=_config_bool(config, "WRITE_VALUE_PIXELS", True),
        write_value_tiffs=_config_bool(config, "WRITE_VALUE_TIFFS", False),
        write_value_composite_geotiff=_config_bool(
            config, "WRITE_VALUE_COMPOSITE_GEOTIFF", True
        ),
        value_output_zooms=_config_value_output_zooms(config, max_zoom),
        value_tiff_tile_limit=_config_optional_int(
            config, "VALUE_TIFF_TILE_LIMIT", None
        ),
        keep_work=_config_bool(config, "KEEP_WORK", False),
    )

    validate_config_args(args)
    return args


def sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def safe_slug(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "value"


def find_parquet_files(folder: Path) -> list[str]:
    patterns = ["*.parquet", "*.geoparquet", "**/*.parquet", "**/*.geoparquet"]
    files: set[Path] = set()
    for pattern in patterns:
        files.update(folder.glob(pattern))
    files = {p for p in files if p.is_file()}
    return sorted(p.as_posix() for p in files)


def lonlat_arrays_to_global_pixels(
    lon: np.ndarray,
    lat: np.ndarray,
    zoom: int,
) -> tuple[np.ndarray, np.ndarray]:
    world_px = TILE_SIZE * (1 << zoom)

    lat = np.clip(lat.astype("float64"), -WEB_MERCATOR_MAX_LAT, WEB_MERCATOR_MAX_LAT)
    lon = lon.astype("float64")

    x = (lon + 180.0) / 360.0 * world_px

    sin_lat = np.sin(np.deg2rad(lat))
    y = (0.5 - np.log((1.0 + sin_lat) / (1.0 - sin_lat)) / (4.0 * math.pi)) * world_px

    return x, y


def add_segment_to_pixel_accumulator(
    accumulator: dict[int, float],
    x1: float,
    y1: float,
    x2: float,
    y2: float,
    value: float,
    world_px: int,
    sample_step_px: float,
    max_segment_samples: int,
) -> None:
    if not np.isfinite(x1 + y1 + x2 + y2 + value):
        return

    if value <= 0:
        return

    dx = x2 - x1

    # Handle tracks crossing the antimeridian by drawing the shortest wrapped line.
    if dx > world_px / 2:
        x2 -= world_px
    elif dx < -world_px / 2:
        x2 += world_px

    dx = x2 - x1
    dy = y2 - y1

    pixel_length = max(abs(dx), abs(dy))
    steps = max(1, int(math.ceil(pixel_length / max(sample_step_px, 0.1))))
    steps = min(steps, max_segment_samples)

    weight = value / float(steps + 1)

    for i in range(steps + 1):
        t = i / steps
        gx = int(math.floor((x1 + dx * t) % world_px))
        gy = int(math.floor(y1 + dy * t))

        if 0 <= gy < world_px:
            key = gy * world_px + gx
            accumulator[key] += weight


def flush_pixel_accumulator(
    accumulator: dict[int, float],
    output_dir: Path,
    part_number: int,
    world_px: int,
) -> int:
    if not accumulator:
        return part_number

    keys = np.fromiter(accumulator.keys(), dtype=np.int64, count=len(accumulator))
    values = np.fromiter(accumulator.values(), dtype=np.float64, count=len(accumulator))

    gy = keys // world_px
    gx = keys - gy * world_px

    table = pa.table(
        {
            "gx": pa.array(gx, type=pa.int64()),
            "gy": pa.array(gy, type=pa.int64()),
            "value": pa.array(values, type=pa.float64()),
        }
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"pixels_part_{part_number:06d}.parquet"
    pq.write_table(table, output_path, compression="zstd")

    accumulator.clear()
    return part_number + 1

def apply_duckdb_settings(
    con: duckdb.DuckDBPyConnection,
    args: argparse.Namespace,
    work_dir: Path,
    temp_subdir: str,
) -> Path:
    """
    Apply DuckDB settings consistently.

    Lower thread counts reduce memory spikes during large JOIN / SORT /
    WINDOW operations. A dedicated temp directory helps DuckDB spill to disk.
    """
    threads = max(1, int(args.threads))
    con.execute(f"SET threads TO {threads}")

    preserve = bool(getattr(args, "duckdb_preserve_insertion_order", False))
    con.execute(f"SET preserve_insertion_order={'true' if preserve else 'false'}")

    if args.duckdb_memory_limit:
        con.execute(f"SET memory_limit = {sql_str(str(args.duckdb_memory_limit))}")

    duckdb_temp_dir = getattr(args, "duckdb_temp_dir", None)

    if duckdb_temp_dir:
        temp_dir = Path(duckdb_temp_dir) / temp_subdir
    else:
        temp_dir = work_dir / "duckdb_temp" / temp_subdir

    temp_dir.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory = {sql_str(temp_dir.as_posix())}")

    # Helps when writing many partitioned Parquet shards.
    try:
        con.execute("SET partitioned_write_max_open_files=16")
    except Exception:
        pass

    print("DuckDB settings:")
    print(f"  threads: {threads}")
    print(f"  memory_limit: {args.duckdb_memory_limit}")
    print(f"  preserve_insertion_order: {preserve}")
    print(f"  temp_directory: {temp_dir}")

    return temp_dir

def create_selected_vessels_table(
    con: duckdb.DuckDBPyConnection,
    args: argparse.Namespace,
) -> list[str]:
    vessel_ship_col = sql_identifier(args.vessel_ship_id_column)
    vessel_group_col = sql_identifier(args.vessel_group_column)

    groups = [
        g.strip().upper()
        for g in args.groups
        if g.strip().upper() != "ALL"
    ]

    if groups:
        group_values = ",".join(sql_str(g) for g in groups)
        selected_vessel_group_sql = f"AND comfleet_groupedtype IN ({group_values})"
    else:
        selected_vessel_group_sql = ""

    con.execute("DROP TABLE IF EXISTS selected_vessels")

    con.execute(
        f"""
        CREATE TEMP TABLE selected_vessels AS
        WITH vessel_metadata AS (
            SELECT
                CAST({vessel_ship_col} AS VARCHAR) AS vessel_ship_id,
                NULLIF(
                    UPPER(TRIM(CAST({vessel_group_col} AS VARCHAR))),
                    ''
                ) AS comfleet_groupedtype
            FROM read_csv_auto({sql_str(Path(args.vessel_csv).as_posix())})
        )
        SELECT
            vessel_ship_id,
            MAX(comfleet_groupedtype) AS comfleet_groupedtype
        FROM vessel_metadata
        WHERE
            vessel_ship_id IS NOT NULL
            {selected_vessel_group_sql}
        GROUP BY vessel_ship_id
        """
    )

    selected_vessel_count = con.execute(
        "SELECT COUNT(*) FROM selected_vessels"
    ).fetchone()[0]

    if groups:
        print(
            f"Vessel metadata prefilter selected {selected_vessel_count:,} unique "
            f"SHIP_ID values for groups: {groups}"
        )

        if selected_vessel_count == 0:
            raise RuntimeError(
                "The vessel metadata prefilter selected zero vessels. "
                "Check the COMFLEET_GROUPEDTYPE spelling."
            )
    else:
        print(
            f"Groups = ALL. Loaded {selected_vessel_count:,} vessel metadata rows "
            "for optional vessel type attribution."
        )

    return groups


def stage_points_by_ship_id_hash(
    con: duckdb.DuckDBPyConnection,
    args: argparse.Namespace,
    work_dir: Path,
    ais_files: list[str],
    groups: list[str],
) -> Path:
    """
    One-pass AIS staging step.

    This reads the original AIS Parquet files once, applies the vessel-group
    filter, keeps only the columns needed for track-building, and writes a
    staged Parquet dataset partitioned by SHIP_ID hash shard.

    The expensive LAG/PARTITION BY step happens later, one shard at a time.
    """
    staged_dir = work_dir / "staged_points_by_ship_id_shard"

    if staged_dir.exists():
        shutil.rmtree(staged_dir)
    staged_dir.mkdir(parents=True, exist_ok=True)

    ais_files_sql = "[" + ",".join(sql_str(p) for p in ais_files) + "]"

    ship_col = sql_identifier(args.ship_id_column)
    lat_col = sql_identifier(args.lat_column)
    lon_col = sql_identifier(args.lon_column)
    speed_col = sql_identifier(args.speed_column)
    ts_col = sql_identifier(args.timestamp_column)

    shard_count = int(args.ship_id_shards)

    if groups:
        ais_join_sql = "INNER JOIN selected_vessels AS v"
    else:
        ais_join_sql = "LEFT JOIN selected_vessels AS v"

    print("")
    print("Staging AIS points by SHIP_ID hash shard...")
    print(f"  shard_count: {shard_count}")
    print(f"  staged output: {staged_dir}")

    sql = f"""
    COPY (
        SELECT
            ship_id,
            lat,
            lon,
            speed_kts,
            ts,
            group_type,
            CAST(hash(ship_id) % {shard_count} AS INTEGER) AS ship_id_shard
        FROM (
            SELECT
                CAST(a.{ship_col} AS VARCHAR) AS ship_id,
                CAST(a.{lat_col} AS DOUBLE) AS lat,
                CAST(a.{lon_col} AS DOUBLE) AS lon,
                CAST(a.{speed_col} AS DOUBLE) / {float(args.speed_scale)} AS speed_kts,
                TRY_CAST(a.{ts_col} AS TIMESTAMP) AS ts,
                COALESCE(v.comfleet_groupedtype, 'UNKNOWN') AS group_type
            FROM read_parquet({ais_files_sql}, union_by_name=true) AS a
            {ais_join_sql}
                ON CAST(a.{ship_col} AS VARCHAR) = v.vessel_ship_id
        ) AS p
        WHERE
            ship_id IS NOT NULL
            AND ts IS NOT NULL
            AND lat BETWEEN {-WEB_MERCATOR_MAX_LAT} AND {WEB_MERCATOR_MAX_LAT}
            AND lon BETWEEN -180.0 AND 180.0
            AND speed_kts IS NOT NULL
    )
    TO {sql_str(staged_dir.as_posix())}
    (
        FORMAT PARQUET,
        COMPRESSION ZSTD,
        PARTITION_BY (ship_id_shard)
    );
    """

    con.execute(sql)

    staged_files = list(staged_dir.glob("ship_id_shard=*/*.parquet"))
    if not staged_files:
        raise RuntimeError("No staged AIS point files were produced.")

    staged_glob = (staged_dir / "ship_id_shard=*" / "*.parquet").as_posix()

    staged_point_count = con.execute(
        f"SELECT COUNT(*) FROM read_parquet({sql_str(staged_glob)})"
    ).fetchone()[0]

    print(f"Staged {staged_point_count:,} AIS points into {len(staged_files):,} Parquet files.")

    return staged_dir


def build_segments_for_one_shard(
    args: argparse.Namespace,
    work_dir: Path,
    staged_dir: Path,
    segments_dir: Path,
    shard: int,
) -> int:
    shard_dir = staged_dir / f"ship_id_shard={shard}"
    shard_files = list(shard_dir.glob("*.parquet"))

    if not shard_files:
        print(f"Shard {shard:04d}: no staged points, skipping.")
        return 0

    shard_glob = (shard_dir / "*.parquet").as_posix()
    segment_out = segments_dir / f"segments_shard_{shard:04d}.parquet"

    if segment_out.exists():
        segment_out.unlink()

    if args.require_both_endpoint_speeds:
        speed_filter_sql = (
            f"AND speed1_kts >= {float(args.min_speed_knots)} "
            f"AND speed2_kts >= {float(args.min_speed_knots)}"
        )
    else:
        speed_filter_sql = f"AND avg_speed_kts >= {float(args.min_speed_knots)}"

    if args.metric == "vessel_hours":
        value_sql = "dt_seconds / 3600.0"
    elif args.metric == "track_km":
        value_sql = "distance_km"
    else:
        raise ValueError(
            "Segment construction is only valid for track_km or vessel_hours."
        )

    con = duckdb.connect()
    apply_duckdb_settings(
        con=con,
        args=args,
        work_dir=work_dir,
        temp_subdir=f"segments_shard_{shard:04d}",
    )

    sql = f"""
    COPY (
        WITH clean_points AS (
            SELECT
                ship_id,
                group_type,
                lat,
                lon,
                speed_kts,
                ts
            FROM read_parquet({sql_str(shard_glob)})
            WHERE
                ship_id IS NOT NULL
                AND ts IS NOT NULL
                AND lat IS NOT NULL
                AND lon IS NOT NULL
                AND speed_kts IS NOT NULL
        ),

        ordered_points AS (
            SELECT
                ship_id,
                group_type,
                lat AS lat2,
                lon AS lon2,
                speed_kts AS speed2_kts,
                ts AS ts2,
                LAG(lat) OVER (PARTITION BY ship_id ORDER BY ts) AS lat1,
                LAG(lon) OVER (PARTITION BY ship_id ORDER BY ts) AS lon1,
                LAG(speed_kts) OVER (PARTITION BY ship_id ORDER BY ts) AS speed1_kts,
                LAG(ts) OVER (PARTITION BY ship_id ORDER BY ts) AS ts1
            FROM clean_points
        ),

        candidate_segments AS (
            SELECT
                ship_id,
                group_type,
                lon1,
                lat1,
                lon2,
                lat2,
                ts1,
                ts2,
                speed1_kts,
                speed2_kts,
                (speed1_kts + speed2_kts) / 2.0 AS avg_speed_kts,
                EXTRACT(EPOCH FROM (ts2 - ts1)) AS dt_seconds
            FROM ordered_points
            WHERE
                lon1 IS NOT NULL
                AND lat1 IS NOT NULL
                AND ts1 IS NOT NULL
                AND ts2 > ts1
        ),

        measured_segments AS (
            SELECT
                *,
                (
                    2.0 * {EARTH_RADIUS_KM} * ASIN(
                        LEAST(
                            1.0,
                            SQRT(
                                POWER(SIN(RADIANS(lat2 - lat1) / 2.0), 2.0)
                                +
                                COS(RADIANS(lat1))
                                * COS(RADIANS(lat2))
                                * POWER(
                                    SIN(
                                        RADIANS(
                                            ((lon2 - lon1 + 540.0) % 360.0) - 180.0
                                        ) / 2.0
                                    ),
                                    2.0
                                )
                            )
                        )
                    )
                ) AS distance_km
            FROM candidate_segments
        )

        SELECT
            ship_id,
            group_type,
            lon1,
            lat1,
            lon2,
            lat2,
            ts1,
            ts2,
            dt_seconds,
            distance_km,
            speed1_kts,
            speed2_kts,
            avg_speed_kts,
            {value_sql} AS value
        FROM measured_segments
        WHERE
            dt_seconds BETWEEN 1 AND {int(args.max_gap_minutes) * 60}
            AND distance_km > 0
            AND distance_km / NULLIF(dt_seconds / 3600.0, 0) / 1.852 <= {float(args.max_implied_speed_knots)}
            {speed_filter_sql}
    )
    TO {sql_str(segment_out.as_posix())}
    (FORMAT PARQUET, COMPRESSION ZSTD);
    """

    try:
        con.execute(sql)

        segment_count = con.execute(
            f"SELECT COUNT(*) FROM read_parquet({sql_str(segment_out.as_posix())})"
        ).fetchone()[0]

    finally:
        con.close()

    if segment_count == 0:
        try:
            segment_out.unlink()
        except FileNotFoundError:
            pass
    else:
        print(f"Shard {shard:04d}: built {segment_count:,} segments.")

    return int(segment_count)


def build_segments_parquet(args: argparse.Namespace, work_dir: Path) -> Path:
    """
    Sharded segment builder.

    Returns a directory containing Parquet segment files:
        segments_sharded/segments_shard_0000.parquet
        segments_sharded/segments_shard_0001.parquet
        ...

    This directory is then read by the existing tile-building code.
    """
    ais_files = find_parquet_files(Path(args.ais_folder))
    if not ais_files:
        raise FileNotFoundError(f"No Parquet or GeoParquet files found in: {args.ais_folder}")

    shard_count = int(args.ship_id_shards)
    if shard_count < 1:
        raise ValueError("ship_id_shards must be at least 1.")

    db_path = work_dir / "stage_points.duckdb"
    con = duckdb.connect(db_path.as_posix())

    try:
        apply_duckdb_settings(
            con=con,
            args=args,
            work_dir=work_dir,
            temp_subdir="stage_points",
        )

        groups = create_selected_vessels_table(con=con, args=args)

        staged_dir = stage_points_by_ship_id_hash(
            con=con,
            args=args,
            work_dir=work_dir,
            ais_files=ais_files,
            groups=groups,
        )

    finally:
        con.close()

    segments_dir = work_dir / "segments_sharded"
    if segments_dir.exists():
        shutil.rmtree(segments_dir)
    segments_dir.mkdir(parents=True, exist_ok=True)

    print("")
    print("Building track segments shard by shard...")

    total_segments = 0

    for shard in range(shard_count):
        total_segments += build_segments_for_one_shard(
            args=args,
            work_dir=work_dir,
            staged_dir=staged_dir,
            segments_dir=segments_dir,
            shard=shard,
        )

    segment_files = list(segments_dir.glob("*.parquet"))

    if total_segments == 0 or not segment_files:
        raise RuntimeError(
            "No valid segments were produced. Check the group filter, speed threshold, "
            "timestamp parsing, and join key."
        )

    print("")
    print(f"Built {total_segments:,} track segments across {len(segment_files):,} shard files.")

    return segments_dir


def build_point_count_parquet(
    args: argparse.Namespace,
    work_dir: Path,
) -> Path:
    """
    Stage qualifying AIS observations for the point_count metric.

    Each source row that passes the vessel-group, coordinate, and inclusive
    speed filters is retained once. Timestamp and segment-quality settings are
    deliberately not used because point_count is an observation-density metric,
    not a track metric.
    """
    ais_files = find_parquet_files(Path(args.ais_folder))
    if not ais_files:
        raise FileNotFoundError(
            f"No Parquet or GeoParquet files found in: {args.ais_folder}"
        )

    shard_count = int(args.ship_id_shards)
    if shard_count < 1:
        raise ValueError("ship_id_shards must be at least 1.")

    points_dir = work_dir / "point_count_points"
    if points_dir.exists():
        shutil.rmtree(points_dir)
    points_dir.mkdir(parents=True, exist_ok=True)

    db_path = work_dir / "stage_point_count.duckdb"
    con = duckdb.connect(db_path.as_posix())

    try:
        apply_duckdb_settings(
            con=con,
            args=args,
            work_dir=work_dir,
            temp_subdir="stage_point_count",
        )

        groups = create_selected_vessels_table(con=con, args=args)

        ais_files_sql = "[" + ",".join(sql_str(p) for p in ais_files) + "]"

        ship_col = sql_identifier(args.ship_id_column)
        lat_col = sql_identifier(args.lat_column)
        lon_col = sql_identifier(args.lon_column)
        speed_col = sql_identifier(args.speed_column)

        if groups:
            ais_join_sql = "INNER JOIN selected_vessels AS v"
        else:
            ais_join_sql = "LEFT JOIN selected_vessels AS v"

        print("")
        print("Staging qualifying AIS observations for point_count...")
        print(
            f"  inclusive speed filter: SPEED / {float(args.speed_scale)} "
            f">= {float(args.min_speed_knots)} kt"
        )
        print(f"  point shards: {shard_count}")
        print(f"  staged output: {points_dir}")

        sql = f"""
        COPY (
            SELECT
                lon,
                lat,
                CAST(hash(ship_id) % {shard_count} AS INTEGER) AS point_shard
            FROM (
                SELECT
                    CAST(a.{ship_col} AS VARCHAR) AS ship_id,
                    TRY_CAST(a.{lat_col} AS DOUBLE) AS lat,
                    TRY_CAST(a.{lon_col} AS DOUBLE) AS lon,
                    TRY_CAST(a.{speed_col} AS DOUBLE)
                        / {float(args.speed_scale)} AS speed_kts
                FROM read_parquet({ais_files_sql}, union_by_name=true) AS a
                {ais_join_sql}
                    ON CAST(a.{ship_col} AS VARCHAR) = v.vessel_ship_id
            ) AS p
            WHERE
                ship_id IS NOT NULL
                AND lat IS NOT NULL
                AND lon IS NOT NULL
                AND speed_kts IS NOT NULL
                AND isfinite(lat)
                AND isfinite(lon)
                AND isfinite(speed_kts)
                AND lat BETWEEN {-WEB_MERCATOR_MAX_LAT} AND {WEB_MERCATOR_MAX_LAT}
                AND lon BETWEEN -180.0 AND 180.0
                AND speed_kts >= {float(args.min_speed_knots)}
        )
        TO {sql_str(points_dir.as_posix())}
        (
            FORMAT PARQUET,
            COMPRESSION ZSTD,
            PARTITION_BY (point_shard)
        );
        """

        con.execute(sql)

        point_files = list(points_dir.glob("point_shard=*/*.parquet"))
        if not point_files:
            raise RuntimeError(
                "No qualifying AIS observations were produced. Check the vessel "
                "group, speed threshold, source columns, and coordinates."
            )

        point_glob = (points_dir / "point_shard=*" / "*.parquet").as_posix()
        point_count = con.execute(
            f"SELECT COUNT(*) FROM read_parquet({sql_str(point_glob)})"
        ).fetchone()[0]

        if point_count == 0:
            raise RuntimeError(
                "No qualifying AIS observations remained after applying the "
                "inclusive speed threshold."
            )

        print(
            f"Staged {point_count:,} qualifying AIS observations into "
            f"{len(point_files):,} Parquet files."
        )

    finally:
        con.close()

    return points_dir


def build_segment_pixel_parts_for_zoom(
    segment_path: Path,
    zoom: int,
    stage_dir: Path,
    args: argparse.Namespace,
) -> bool:
    if stage_dir.exists():
        shutil.rmtree(stage_dir)
    stage_dir.mkdir(parents=True, exist_ok=True)

    dataset = ds.dataset(segment_path.as_posix(), format="parquet")
    scanner = dataset.scanner(
        columns=["lon1", "lat1", "lon2", "lat2", "value"],
        batch_size=int(args.segment_batch_size),
    )

    world_px = TILE_SIZE * (1 << zoom)
    accumulator: dict[int, float] = defaultdict(float)
    part_number = 0
    segments_seen = 0

    print(f"Aggregating zoom {zoom} into sparse pixels...")

    for batch in tqdm(scanner.to_batches(), desc=f"z{zoom}", unit="batch"):
        names = batch.schema.names

        def col(name: str) -> np.ndarray:
            return batch.column(names.index(name)).to_numpy(zero_copy_only=False)

        lon1 = col("lon1").astype("float64")
        lat1 = col("lat1").astype("float64")
        lon2 = col("lon2").astype("float64")
        lat2 = col("lat2").astype("float64")
        value = col("value").astype("float64")

        x1, y1 = lonlat_arrays_to_global_pixels(lon1, lat1, zoom)
        x2, y2 = lonlat_arrays_to_global_pixels(lon2, lat2, zoom)

        valid = (
            np.isfinite(x1)
            & np.isfinite(y1)
            & np.isfinite(x2)
            & np.isfinite(y2)
            & np.isfinite(value)
            & (value > 0)
        )

        for a, b, c, d, v in zip(x1[valid], y1[valid], x2[valid], y2[valid], value[valid]):
            add_segment_to_pixel_accumulator(
                accumulator=accumulator,
                x1=float(a),
                y1=float(b),
                x2=float(c),
                y2=float(d),
                value=float(v),
                world_px=world_px,
                sample_step_px=float(args.sample_step_px),
                max_segment_samples=int(args.max_segment_samples),
            )

        segments_seen += int(valid.sum())

        if len(accumulator) >= int(args.pixel_flush_threshold):
            part_number = flush_pixel_accumulator(
                accumulator=accumulator,
                output_dir=stage_dir,
                part_number=part_number,
                world_px=world_px,
            )

    part_number = flush_pixel_accumulator(
        accumulator=accumulator,
        output_dir=stage_dir,
        part_number=part_number,
        world_px=world_px,
    )

    parts = list(stage_dir.glob("*.parquet"))
    print(f"Zoom {zoom}: processed {segments_seen:,} segments into {len(parts):,} pixel part files.")

    return bool(parts)


def build_point_pixel_parts_for_zoom(
    point_path: Path,
    zoom: int,
    stage_dir: Path,
    args: argparse.Namespace,
) -> bool:
    """Aggregate qualifying AIS observations into sparse Web Mercator pixels."""
    if stage_dir.exists():
        shutil.rmtree(stage_dir)
    stage_dir.mkdir(parents=True, exist_ok=True)

    dataset = ds.dataset(point_path.as_posix(), format="parquet")
    scanner = dataset.scanner(
        columns=["lon", "lat"],
        batch_size=int(args.segment_batch_size),
    )

    world_px = TILE_SIZE * (1 << zoom)
    accumulator: dict[int, float] = defaultdict(float)
    part_number = 0
    observations_seen = 0

    print(f"Aggregating point_count zoom {zoom} into sparse pixels...")

    for batch in tqdm(scanner.to_batches(), desc=f"point_count z{zoom}", unit="batch"):
        names = batch.schema.names

        def col(name: str) -> np.ndarray:
            return batch.column(names.index(name)).to_numpy(zero_copy_only=False)

        lon = col("lon").astype("float64")
        lat = col("lat").astype("float64")

        x, y = lonlat_arrays_to_global_pixels(lon, lat, zoom)

        valid = np.isfinite(x) & np.isfinite(y)
        if not valid.any():
            continue

        gx = np.floor(x[valid]).astype(np.int64) % world_px
        gy = np.floor(y[valid]).astype(np.int64)
        gy = np.clip(gy, 0, world_px - 1)
        keys = gy * world_px + gx

        unique_keys, counts = np.unique(keys, return_counts=True)
        for key, count in zip(unique_keys, counts):
            accumulator[int(key)] += float(count)

        observations_seen += int(keys.size)

        if len(accumulator) >= int(args.pixel_flush_threshold):
            part_number = flush_pixel_accumulator(
                accumulator=accumulator,
                output_dir=stage_dir,
                part_number=part_number,
                world_px=world_px,
            )

    part_number = flush_pixel_accumulator(
        accumulator=accumulator,
        output_dir=stage_dir,
        part_number=part_number,
        world_px=world_px,
    )

    parts = list(stage_dir.glob("*.parquet"))
    print(
        f"Zoom {zoom}: counted {observations_seen:,} qualifying AIS observations "
        f"into {len(parts):,} pixel part files."
    )

    return bool(parts)


def build_pixel_parts_for_zoom(
    source_path: Path,
    zoom: int,
    stage_dir: Path,
    args: argparse.Namespace,
) -> bool:
    """Dispatch to segment or observation rasterisation for the selected metric."""
    if args.metric == "point_count":
        return build_point_pixel_parts_for_zoom(
            point_path=source_path,
            zoom=zoom,
            stage_dir=stage_dir,
            args=args,
        )

    return build_segment_pixel_parts_for_zoom(
        segment_path=source_path,
        zoom=zoom,
        stage_dir=stage_dir,
        args=args,
    )


def aggregate_pixel_parts(
    con: duckdb.DuckDBPyConnection,
    stage_dir: Path,
    zoom: int,
) -> tuple[str, int]:
    table_name = f"pixels_z{zoom}"
    glob_path = (stage_dir / "*.parquet").as_posix()

    con.execute(f"DROP TABLE IF EXISTS {table_name}")

    print(f"Combining duplicate pixels for zoom {zoom}...")

    con.execute(
        f"""
        CREATE TABLE {table_name} AS
        SELECT
            CAST(gx AS BIGINT) AS gx,
            CAST(gy AS BIGINT) AS gy,
            CAST(SUM(value) AS DOUBLE) AS value
        FROM read_parquet({sql_str(glob_path)})
        GROUP BY 1, 2
        HAVING SUM(value) > 0
        ORDER BY gx, gy
        """
    )

    pixel_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

    try:
        con.execute(f"CREATE INDEX idx_{table_name}_xy ON {table_name}(gx, gy)")
    except Exception:
        pass

    print(f"Zoom {zoom}: {pixel_count:,} non-zero pixels after aggregation.")
    return table_name, pixel_count



def should_export_value_zoom(args: argparse.Namespace, zoom: int) -> bool:
    """
    Decide whether analytical value outputs should be written for this zoom.

    args.value_output_zooms can be:
      - None: only args.max_zoom, which is the default behaviour
      - []: all processed zooms
      - [9, 10]: only listed zooms
    """
    zooms = getattr(args, "value_output_zooms", None)
    if zooms is None:
        return int(zoom) == int(args.max_zoom)
    if len(zooms) == 0:
        return True
    return int(zoom) in {int(z) for z in zooms}


def web_mercator_resolution_m(zoom: int) -> float:
    world_px = TILE_SIZE * (1 << int(zoom))
    return (2.0 * WEB_MERCATOR_HALF_WORLD_M) / float(world_px)


def write_value_metadata(
    metadata_path: Path,
    args: argparse.Namespace,
    zoom: int,
    output_name: str,
    pixel_count: int,
) -> None:
    metadata = {
        "output_name": output_name,
        "zoom": int(zoom),
        "tile_size": TILE_SIZE,
        "crs": "EPSG:3857",
        "metric": args.metric,
        "metric_description": metric_description(args.metric),
        "value_column": "value",
        "pixel_count": int(pixel_count),
        "speed_filter_knots": float(args.min_speed_knots),
        "speed_filter_operator": ">=",
        "speed_scale": float(args.speed_scale),
        "segment_quality_filters_applied": args.metric in SEGMENT_METRICS,
        "require_both_endpoint_speeds": (
            bool(args.require_both_endpoint_speeds)
            if args.metric in SEGMENT_METRICS
            else None
        ),
        "max_gap_minutes": (
            int(args.max_gap_minutes) if args.metric in SEGMENT_METRICS else None
        ),
        "max_implied_speed_knots": (
            float(args.max_implied_speed_knots)
            if args.metric in SEGMENT_METRICS
            else None
        ),
        "resolution_m_at_equator": web_mercator_resolution_m(zoom),
        "note": (
            "These values are pre-colour-rendering analytical density values. "
            "For point_count, every qualifying AIS source row contributes exactly "
            "one count to its containing Web Mercator pixel. They are not the RGBA "
            "values stored in the visual MBTiles/XYZ PNG output."
        ),
    }
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")


def export_value_pixels_parquet(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    zoom: int,
    args: argparse.Namespace,
    output_dir: Path,
    output_name: str,
    pixel_count: int,
) -> Path:
    """
    Export the aggregated pre-rendering pixel values as a compact sparse Parquet table.

    This is the most scalable analytical output because it writes only non-zero pixels.
    It is suitable for DuckDB, Python, and downstream conversion to other formats.
    """
    value_root = output_dir / f"{output_name}_value_pixels"
    value_root.mkdir(parents=True, exist_ok=True)

    out_path = value_root / f"{output_name}_z{int(zoom)}_{safe_slug(args.metric)}_pixels.parquet"
    metadata_path = value_root / f"{output_name}_z{int(zoom)}_{safe_slug(args.metric)}_metadata.json"

    if out_path.exists():
        out_path.unlink()

    world_px = TILE_SIZE * (1 << int(zoom))
    res = web_mercator_resolution_m(zoom)
    half = WEB_MERCATOR_HALF_WORLD_M

    # Include lon/lat and EPSG:3857 pixel-centre coordinates to make inspection easier.
    # The value column is the original aggregated density value before colour rendering.
    con.execute(
        f"""
        COPY (
            SELECT
                CAST({int(zoom)} AS INTEGER) AS zoom,
                CAST(gx AS BIGINT) AS gx,
                CAST(gy AS BIGINT) AS gy,
                CAST(FLOOR(gx / {TILE_SIZE}) AS BIGINT) AS tile_x,
                CAST(FLOOR(gy / {TILE_SIZE}) AS BIGINT) AS tile_y,
                CAST(gx % {TILE_SIZE} AS INTEGER) AS pixel_x,
                CAST(gy % {TILE_SIZE} AS INTEGER) AS pixel_y,
                CAST({-half} + (gx + 0.5) * {res} AS DOUBLE) AS x_mercator_m,
                CAST({half} - (gy + 0.5) * {res} AS DOUBLE) AS y_mercator_m,
                CAST(((gx + 0.5) / {world_px}) * 360.0 - 180.0 AS DOUBLE) AS lon_center,
                CAST(DEGREES(ATAN(SINH(PI() * (1.0 - 2.0 * ((gy + 0.5) / {world_px}))))) AS DOUBLE) AS lat_center,
                CAST(value AS DOUBLE) AS value,
                CAST(value AS DOUBLE) AS {sql_identifier(safe_slug(args.metric))}
            FROM {table_name}
            WHERE value > 0
            ORDER BY gy, gx
        )
        TO {sql_str(out_path.as_posix())}
        (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )

    write_value_metadata(metadata_path, args, zoom, output_name, pixel_count)
    print(f"Zoom {zoom}: wrote analytical value pixels: {out_path}")
    print(f"Zoom {zoom}: wrote analytical value metadata: {metadata_path}")
    return out_path


def write_web_mercator_prj(path: Path) -> None:
    # ESRI-style Web Mercator WKT that QGIS/GDAL can usually recognise as EPSG:3857.
    prj = (
        'PROJCS["WGS_1984_Web_Mercator_Auxiliary_Sphere",'
        'GEOGCS["GCS_WGS_1984",'
        'DATUM["D_WGS_1984",'
        'SPHEROID["WGS_1984",6378137.0,298.257223563]],'
        'PRIMEM["Greenwich",0.0],'
        'UNIT["Degree",0.0174532925199433]],'
        'PROJECTION["Mercator_Auxiliary_Sphere"],'
        'PARAMETER["False_Easting",0.0],'
        'PARAMETER["False_Northing",0.0],'
        'PARAMETER["Central_Meridian",0.0],'
        'PARAMETER["Standard_Parallel_1",0.0],'
        'PARAMETER["Auxiliary_Sphere_Type",0.0],'
        'UNIT["Meter",1.0]]'
    )
    path.write_text(prj, encoding="utf-8")


def export_value_tiff_tiles(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    zoom: int,
    args: argparse.Namespace,
    output_dir: Path,
    output_name: str,
    pixel_count: int,
) -> int:
    """
    Export non-empty analytical value tiles as Float32 GeoTIFFs.

    Each GeoTIFF tile is a single-band 256 x 256 raster in EPSG:3857.
    Pixel values are the original aggregated pre-colour-rendering values:
      - args.metric == "track_km": accumulated vessel track-km per pixel
      - args.metric == "vessel_hours": accumulated vessel-hours per pixel
      - args.metric == "point_count": qualifying AIS observation count per pixel

    This output does not apply the MBTiles colour ramp, log scaling, alpha,
    or visual blur. It is designed for QGIS Identify/query and analysis.
    """
    if rasterio is None or from_origin is None:
        raise RuntimeError(
            "Float32 GeoTIFF value tile output requires rasterio. "
            "Install it in the Python environment used by Spyder, for example: "
            "conda install -c conda-forge rasterio"
        )

    tiff_root = output_dir / f"{output_name}_value_geotiff_tiles" / f"z{int(zoom)}"
    if tiff_root.exists():
        shutil.rmtree(tiff_root)
    tiff_root.mkdir(parents=True, exist_ok=True)

    metadata_path = output_dir / f"{output_name}_value_geotiff_tiles" / f"{output_name}_z{int(zoom)}_{safe_slug(args.metric)}_geotiff_metadata.json"

    tile_rows = con.execute(
        f"""
        SELECT DISTINCT
            CAST(FLOOR(gx / {TILE_SIZE}) AS INTEGER) AS tile_x,
            CAST(FLOOR(gy / {TILE_SIZE}) AS INTEGER) AS tile_y
        FROM {table_name}
        ORDER BY tile_y, tile_x
        """
    ).fetchall()

    limit = getattr(args, "value_tiff_tile_limit", None)
    if limit is not None:
        tile_rows = tile_rows[: int(limit)]

    res = web_mercator_resolution_m(zoom)
    half = WEB_MERCATOR_HALF_WORLD_M
    tiles_written = 0

    for tile_x, tile_y in tqdm(tile_rows, desc=f"value GeoTIFF z{zoom}", unit="tile"):
        tile_x = int(tile_x)
        tile_y = int(tile_y)
        xmin_px = tile_x * TILE_SIZE
        xmax_px = (tile_x + 1) * TILE_SIZE - 1
        ymin_px = tile_y * TILE_SIZE
        ymax_px = (tile_y + 1) * TILE_SIZE - 1

        rows = con.execute(
            f"""
            SELECT gx, gy, value
            FROM {table_name}
            WHERE gx BETWEEN ? AND ?
              AND gy BETWEEN ? AND ?
            """,
            [int(xmin_px), int(xmax_px), int(ymin_px), int(ymax_px)],
        ).fetchall()

        if not rows:
            continue

        arr = np.zeros((TILE_SIZE, TILE_SIZE), dtype=np.float32)
        for gx, gy, value in rows:
            px = int(gx) - xmin_px
            py = int(gy) - ymin_px
            if 0 <= px < TILE_SIZE and 0 <= py < TILE_SIZE:
                arr[py, px] = float(value)

        if not np.isfinite(arr).any() or float(np.nanmax(arr)) <= 0.0:
            continue

        # GeoTIFF transform uses the upper-left corner of the upper-left pixel.
        x_left = -half + xmin_px * res
        y_top = half - ymin_px * res
        transform = from_origin(x_left, y_top, res, res)

        tile_dir = tiff_root / str(tile_x)
        tile_dir.mkdir(parents=True, exist_ok=True)
        tif_path = tile_dir / f"{tile_y}.tif"

        with rasterio.open(
            tif_path,
            "w",
            driver="GTiff",
            height=TILE_SIZE,
            width=TILE_SIZE,
            count=1,
            dtype="float32",
            crs="EPSG:3857",
            transform=transform,
            compress="deflate",
            predictor=3,
            tiled=True,
            blockxsize=TILE_SIZE,
            blockysize=TILE_SIZE,
            bigtiff="IF_SAFER",
        ) as dst:
            dst.write(arr, 1)
            dst.set_band_description(1, args.metric)
            dst.update_tags(
                metric=args.metric,
                value_meaning=metric_description(args.metric),
                zoom=str(int(zoom)),
                tile_x=str(tile_x),
                tile_y=str(tile_y),
                speed_filter_knots=str(float(args.min_speed_knots)),
                speed_filter_operator=">=",
                speed_scale=str(float(args.speed_scale)),
            )

        tiles_written += 1

    write_value_metadata(metadata_path, args, zoom, output_name, pixel_count)
    print(f"Zoom {zoom}: wrote {tiles_written:,} analytical Float32 GeoTIFF value tiles: {tiff_root}")
    print(f"Zoom {zoom}: wrote analytical GeoTIFF metadata: {metadata_path}")
    return tiles_written




def export_value_composite_geotiff(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    zoom: int,
    args: argparse.Namespace,
    output_dir: Path,
    output_name: str,
    pixel_count: int,
) -> Path | None:
    """
    Export one composite analytical Float32 GeoTIFF for a zoom level.

    This writes a single-band EPSG:3857 GeoTIFF cropped to the set of
    Web Mercator tiles that contain non-zero analytical values. The pixel
    values are the original aggregated pre-colour-rendering values:
      - args.metric == "track_km": accumulated vessel track-km per pixel
      - args.metric == "vessel_hours": accumulated vessel-hours per pixel
      - args.metric == "point_count": qualifying AIS observation count per pixel

    It does not apply the visual MBTiles colour ramp, log scaling, alpha,
    or blur. It is designed for QGIS Identify/query and analysis.
    """
    if rasterio is None or from_origin is None or Window is None:
        raise RuntimeError(
            "Composite Float32 GeoTIFF value output requires rasterio. "
            "Install it in the Python environment used by Spyder, for example: "
            "conda install -c conda-forge rasterio"
        )

    bounds = con.execute(
        f"""
        SELECT
            MIN(CAST(FLOOR(gx / {TILE_SIZE}) AS BIGINT)) AS min_tile_x,
            MAX(CAST(FLOOR(gx / {TILE_SIZE}) AS BIGINT)) AS max_tile_x,
            MIN(CAST(FLOOR(gy / {TILE_SIZE}) AS BIGINT)) AS min_tile_y,
            MAX(CAST(FLOOR(gy / {TILE_SIZE}) AS BIGINT)) AS max_tile_y
        FROM {table_name}
        WHERE value > 0
        """
    ).fetchone()

    if bounds is None or bounds[0] is None:
        print(f"Zoom {zoom}: no analytical values found for composite GeoTIFF.")
        return None

    min_tile_x, max_tile_x, min_tile_y, max_tile_y = [int(v) for v in bounds]
    tile_count_x = max_tile_x - min_tile_x + 1
    tile_count_y = max_tile_y - min_tile_y + 1
    width = tile_count_x * TILE_SIZE
    height = tile_count_y * TILE_SIZE

    # The output is cropped to full Web Mercator tile boundaries covering
    # all non-zero pixels. This keeps the GeoTIFF aligned with the visual
    # tile pyramid while avoiding a full-world raster where possible.
    res = web_mercator_resolution_m(zoom)
    half = WEB_MERCATOR_HALF_WORLD_M
    x_left = -half + (min_tile_x * TILE_SIZE) * res
    y_top = half - (min_tile_y * TILE_SIZE) * res
    transform = from_origin(x_left, y_top, res, res)

    composite_root = output_dir / f"{output_name}_value_composite_geotiff"
    composite_root.mkdir(parents=True, exist_ok=True)

    tif_path = composite_root / f"{output_name}_z{int(zoom)}_{safe_slug(args.metric)}_values.tif"
    metadata_path = composite_root / f"{output_name}_z{int(zoom)}_{safe_slug(args.metric)}_composite_metadata.json"

    if tif_path.exists():
        tif_path.unlink()

    tile_rows = con.execute(
        f"""
        SELECT DISTINCT
            CAST(FLOOR(gx / {TILE_SIZE}) AS INTEGER) AS tile_x,
            CAST(FLOOR(gy / {TILE_SIZE}) AS INTEGER) AS tile_y
        FROM {table_name}
        WHERE value > 0
        ORDER BY tile_y, tile_x
        """
    ).fetchall()

    limit = getattr(args, "value_tiff_tile_limit", None)
    if limit is not None:
        print(
            "Warning: value_tiff_tile_limit is set, so the composite GeoTIFF "
            "will be incomplete and should be used only for testing."
        )
        tile_rows = tile_rows[: int(limit)]

    print("")
    print(f"Writing composite analytical Float32 GeoTIFF for zoom {zoom}...")
    print(f"  output: {tif_path}")
    print(f"  tile range x: {min_tile_x} to {max_tile_x}")
    print(f"  tile range y: {min_tile_y} to {max_tile_y}")
    print(f"  raster size: {width:,} x {height:,} pixels")
    print(f"  resolution at equator: {res:.3f} m/pixel")

    with rasterio.open(
        tif_path,
        "w",
        driver="GTiff",
        height=height,
        width=width,
        count=1,
        dtype="float32",
        crs="EPSG:3857",
        transform=transform,
        nodata=0.0,
        compress="deflate",
        predictor=3,
        tiled=True,
        blockxsize=TILE_SIZE,
        blockysize=TILE_SIZE,
        bigtiff="IF_SAFER",
        sparse_ok=True,
    ) as dst:
        dst.set_band_description(1, args.metric)
        dst.update_tags(
            metric=args.metric,
            value_meaning=metric_description(args.metric),
            zoom=str(int(zoom)),
            speed_filter_knots=str(float(args.min_speed_knots)),
            speed_filter_operator=">=",
            speed_scale=str(float(args.speed_scale)),
            min_tile_x=str(min_tile_x),
            max_tile_x=str(max_tile_x),
            min_tile_y=str(min_tile_y),
            max_tile_y=str(max_tile_y),
        )

        for tile_x, tile_y in tqdm(tile_rows, desc=f"composite GeoTIFF z{zoom}", unit="tile"):
            tile_x = int(tile_x)
            tile_y = int(tile_y)
            xmin_px = tile_x * TILE_SIZE
            xmax_px = (tile_x + 1) * TILE_SIZE - 1
            ymin_px = tile_y * TILE_SIZE
            ymax_px = (tile_y + 1) * TILE_SIZE - 1

            rows = con.execute(
                f"""
                SELECT gx, gy, value
                FROM {table_name}
                WHERE gx BETWEEN ? AND ?
                  AND gy BETWEEN ? AND ?
                  AND value > 0
                """,
                [int(xmin_px), int(xmax_px), int(ymin_px), int(ymax_px)],
            ).fetchall()

            if not rows:
                continue

            arr = np.zeros((TILE_SIZE, TILE_SIZE), dtype=np.float32)
            for gx, gy, value in rows:
                px = int(gx) - xmin_px
                py = int(gy) - ymin_px
                if 0 <= px < TILE_SIZE and 0 <= py < TILE_SIZE:
                    arr[py, px] = float(value)

            if float(np.nanmax(arr)) <= 0.0:
                continue

            window = Window(
                col_off=(tile_x - min_tile_x) * TILE_SIZE,
                row_off=(tile_y - min_tile_y) * TILE_SIZE,
                width=TILE_SIZE,
                height=TILE_SIZE,
            )
            dst.write(arr, 1, window=window)

    metadata = {
        "output_name": output_name,
        "zoom": int(zoom),
        "tile_size": TILE_SIZE,
        "crs": "EPSG:3857",
        "metric": args.metric,
        "metric_description": metric_description(args.metric),
        "value_column": "value",
        "pixel_count": int(pixel_count),
        "source_table": table_name,
        "min_tile_x": int(min_tile_x),
        "max_tile_x": int(max_tile_x),
        "min_tile_y": int(min_tile_y),
        "max_tile_y": int(max_tile_y),
        "width_pixels": int(width),
        "height_pixels": int(height),
        "resolution_m_at_equator": float(res),
        "nodata": 0.0,
        "speed_filter_knots": float(args.min_speed_knots),
        "speed_filter_operator": ">=",
        "speed_scale": float(args.speed_scale),
        "segment_quality_filters_applied": args.metric in SEGMENT_METRICS,
        "require_both_endpoint_speeds": (
            bool(args.require_both_endpoint_speeds)
            if args.metric in SEGMENT_METRICS
            else None
        ),
        "max_gap_minutes": (
            int(args.max_gap_minutes) if args.metric in SEGMENT_METRICS else None
        ),
        "max_implied_speed_knots": (
            float(args.max_implied_speed_knots)
            if args.metric in SEGMENT_METRICS
            else None
        ),
        "note": (
            "This is a single-band Float32 analytical raster. Pixel values are "
            "pre-colour-rendering density values, not the RGBA values in the visual "
            "MBTiles/XYZ PNG output. For point_count, every qualifying source row "
            "contributes exactly one count to its containing pixel. The raster is "
            "cropped to the full Web Mercator tile extent covering non-zero values."
        ),
    }
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    print(f"Zoom {zoom}: wrote composite analytical GeoTIFF: {tif_path}")
    print(f"Zoom {zoom}: wrote composite metadata: {metadata_path}")
    return tif_path

def export_value_outputs(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    zoom: int,
    args: argparse.Namespace,
    output_dir: Path,
    output_name: str,
    pixel_count: int,
) -> None:
    if not should_export_value_zoom(args, zoom):
        return

    if getattr(args, "write_value_pixels", False):
        export_value_pixels_parquet(
            con=con,
            table_name=table_name,
            zoom=zoom,
            args=args,
            output_dir=output_dir,
            output_name=output_name,
            pixel_count=pixel_count,
        )

    if getattr(args, "write_value_tiffs", False):
        export_value_tiff_tiles(
            con=con,
            table_name=table_name,
            zoom=zoom,
            args=args,
            output_dir=output_dir,
            output_name=output_name,
            pixel_count=pixel_count,
        )

    if getattr(args, "write_value_composite_geotiff", False):
        export_value_composite_geotiff(
            con=con,
            table_name=table_name,
            zoom=zoom,
            args=args,
            output_dir=output_dir,
            output_name=output_name,
            pixel_count=pixel_count,
        )


def gaussian_kernel(radius: int) -> np.ndarray:
    if radius <= 0:
        return np.ones((1, 1), dtype=np.float32)

    sigma = max(radius / 2.0, 0.8)
    axis = np.arange(-radius, radius + 1, dtype=np.float32)
    xx, yy = np.meshgrid(axis, axis)
    kernel = np.exp(-(xx * xx + yy * yy) / (2.0 * sigma * sigma))
    kernel /= kernel.sum()
    return kernel.astype(np.float32)


def splat_value(
    arr: np.ndarray,
    center_x: int,
    center_y: int,
    value: float,
    kernel: np.ndarray,
    radius: int,
) -> None:
    x0 = max(0, center_x - radius)
    x1 = min(TILE_SIZE - 1, center_x + radius)
    y0 = max(0, center_y - radius)
    y1 = min(TILE_SIZE - 1, center_y + radius)

    if x0 > x1 or y0 > y1:
        return

    kx0 = x0 - (center_x - radius)
    ky0 = y0 - (center_y - radius)
    kx1 = kx0 + (x1 - x0) + 1
    ky1 = ky0 + (y1 - y0) + 1

    arr[y0 : y1 + 1, x0 : x1 + 1] += value * kernel[ky0:ky1, kx0:kx1]


def colourise_density(
    density: np.ndarray,
    vmax: float,
    alpha_min: int,
    alpha_max: int,
) -> np.ndarray:
    rgba = np.zeros((TILE_SIZE, TILE_SIZE, 4), dtype=np.uint8)

    if vmax <= 0 or not np.isfinite(vmax):
        return rgba

    positive = density > 0
    if not positive.any():
        return rgba

    norm = np.zeros_like(density, dtype=np.float32)
    norm[positive] = np.log1p(density[positive]) / math.log1p(vmax)
    norm = np.clip(norm, 0.0, 1.0)

    stops = np.array([0.0, 0.25, 0.55, 0.78, 1.0], dtype=np.float32)
    reds = np.array([0, 0, 255, 255, 255], dtype=np.float32)
    greens = np.array([45, 210, 255, 145, 0], dtype=np.float32)
    blues = np.array([255, 255, 0, 0, 0], dtype=np.float32)

    rgba[:, :, 0] = np.interp(norm, stops, reds).astype(np.uint8)
    rgba[:, :, 1] = np.interp(norm, stops, greens).astype(np.uint8)
    rgba[:, :, 2] = np.interp(norm, stops, blues).astype(np.uint8)

    alpha = alpha_min + (alpha_max - alpha_min) * np.power(norm, 0.75)
    rgba[:, :, 3] = np.where(positive, np.clip(alpha, 0, alpha_max), 0).astype(np.uint8)

    return rgba


def png_bytes_from_rgba(rgba: np.ndarray) -> bytes:
    image = Image.fromarray(rgba, mode="RGBA")
    buffer = io.BytesIO()
    image.save(buffer, format="PNG", optimize=True)
    return buffer.getvalue()


def init_mbtiles(
    path: Path,
    name: str,
    min_zoom: int,
    max_zoom: int,
    metric: str,
) -> sqlite3.Connection:
    if path.exists():
        path.unlink()

    conn = sqlite3.connect(path.as_posix())
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA temp_store=MEMORY")

    conn.execute("CREATE TABLE metadata (name TEXT, value TEXT)")
    conn.execute(
        """
        CREATE TABLE tiles (
            zoom_level INTEGER,
            tile_column INTEGER,
            tile_row INTEGER,
            tile_data BLOB
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX tile_index
        ON tiles (zoom_level, tile_column, tile_row)
        """
    )

    metadata = {
        "name": name,
        "type": "overlay",
        "version": "1.0",
        "description": f"AIS raster-density heatmap: {metric_description(metric)}",
        "format": "png",
        "minzoom": str(min_zoom),
        "maxzoom": str(max_zoom),
        "bounds": "-180.0,-85.05112878,180.0,85.05112878",
        "center": "0.0,0.0,2",
    }

    conn.executemany("INSERT INTO metadata (name, value) VALUES (?, ?)", metadata.items())
    conn.commit()
    return conn


def get_zoom_vmax(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    quantile: float,
) -> float:
    try:
        vmax = con.execute(
            f"SELECT approx_quantile(value, {float(quantile)}) FROM {table_name} WHERE value > 0"
        ).fetchone()[0]
    except Exception:
        vmax = con.execute(
            f"SELECT quantile_cont(value, {float(quantile)}) FROM {table_name} WHERE value > 0"
        ).fetchone()[0]

    if vmax is None or vmax <= 0:
        vmax = con.execute(f"SELECT MAX(value) FROM {table_name}").fetchone()[0]

    return float(vmax or 0.0)


def fetch_tile_pixels(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    tile_x: int,
    tile_y: int,
    zoom: int,
    radius: int,
) -> list[tuple[int, int, float]]:
    world_px = TILE_SIZE * (1 << zoom)

    xmin = tile_x * TILE_SIZE - radius
    xmax = (tile_x + 1) * TILE_SIZE - 1 + radius
    ymin = max(0, tile_y * TILE_SIZE - radius)
    ymax = min(world_px - 1, (tile_y + 1) * TILE_SIZE - 1 + radius)

    if ymin > ymax:
        return []

    x_ranges: list[tuple[int, int, int]] = []

    if xmin < 0:
        x_ranges.append((0, xmax, 0))
        x_ranges.append((world_px + xmin, world_px - 1, -world_px))
    elif xmax >= world_px:
        x_ranges.append((xmin, world_px - 1, 0))
        x_ranges.append((0, xmax - world_px, world_px))
    else:
        x_ranges.append((xmin, xmax, 0))

    rows: list[tuple[int, int, float]] = []

    for lo, hi, x_shift in x_ranges:
        if lo > hi:
            continue

        result = con.execute(
            f"""
            SELECT gx, gy, value
            FROM {table_name}
            WHERE gx BETWEEN ? AND ?
              AND gy BETWEEN ? AND ?
            """,
            [int(lo), int(hi), int(ymin), int(ymax)],
        ).fetchall()

        for gx, gy, value in result:
            rows.append((int(gx) + int(x_shift), int(gy), float(value)))

    return rows


def render_zoom(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    zoom: int,
    args: argparse.Namespace,
    xyz_root: Path | None,
    mbtiles_conn: sqlite3.Connection | None,
) -> int:
    radius = int(args.blur_radius)
    kernel = gaussian_kernel(radius)

    vmax = get_zoom_vmax(con, table_name, float(args.colour_quantile))
    print(f"Zoom {zoom}: colour scale vmax at q={args.colour_quantile} is {vmax:,.6f}")

    tile_rows = con.execute(
        f"""
        SELECT DISTINCT
            CAST(FLOOR(gx / {TILE_SIZE}) AS INTEGER) AS tile_x,
            CAST(FLOOR(gy / {TILE_SIZE}) AS INTEGER) AS tile_y
        FROM {table_name}
        ORDER BY tile_y, tile_x
        """
    ).fetchall()

    tiles_written = 0

    for tile_x, tile_y in tqdm(tile_rows, desc=f"render z{zoom}", unit="tile"):
        tile_x = int(tile_x)
        tile_y = int(tile_y)

        pixel_rows = fetch_tile_pixels(
            con=con,
            table_name=table_name,
            tile_x=tile_x,
            tile_y=tile_y,
            zoom=zoom,
            radius=radius,
        )

        if not pixel_rows:
            continue

        density = np.zeros((TILE_SIZE, TILE_SIZE), dtype=np.float32)

        for gx, gy, value in pixel_rows:
            px = gx - tile_x * TILE_SIZE
            py = gy - tile_y * TILE_SIZE
            splat_value(
                arr=density,
                center_x=int(px),
                center_y=int(py),
                value=float(value),
                kernel=kernel,
                radius=radius,
            )

        rgba = colourise_density(
            density=density,
            vmax=vmax,
            alpha_min=int(args.alpha_min),
            alpha_max=int(args.alpha_max),
        )

        if rgba[:, :, 3].max() == 0:
            continue

        png_data = png_bytes_from_rgba(rgba)

        if xyz_root is not None:
            tile_path = xyz_root / str(zoom) / str(tile_x) / f"{tile_y}.png"
            tile_path.parent.mkdir(parents=True, exist_ok=True)
            tile_path.write_bytes(png_data)

        if mbtiles_conn is not None:
            # MBTiles uses TMS row order, so flip Y from XYZ.
            tms_y = (1 << zoom) - 1 - tile_y
            mbtiles_conn.execute(
                """
                INSERT OR REPLACE INTO tiles
                    (zoom_level, tile_column, tile_row, tile_data)
                VALUES (?, ?, ?, ?)
                """,
                [int(zoom), int(tile_x), int(tms_y), sqlite3.Binary(png_data)],
            )

        tiles_written += 1

        if mbtiles_conn is not None and tiles_written % 1000 == 0:
            mbtiles_conn.commit()

    if mbtiles_conn is not None:
        mbtiles_conn.commit()

    print(f"Zoom {zoom}: wrote {tiles_written:,} tiles.")
    return tiles_written


def main() -> None:
    args = load_args_from_config()

    ais_folder = Path(args.ais_folder)
    vessel_csv = Path(args.vessel_csv)

    if not ais_folder.is_dir():
        raise FileNotFoundError(f"AIS_FOLDER does not exist or is not a directory: {ais_folder}")
    if not vessel_csv.is_file():
        raise FileNotFoundError(f"VESSEL_CSV does not exist or is not a file: {vessel_csv}")

    print(f"Configuration: {CONFIG_PATH}")
    print("Running with settings:")
    print(f"AIS folder: {args.ais_folder}")
    print(f"Vessel CSV: {args.vessel_csv}")
    print(f"Output dir: {args.output_dir}")
    print(f"Groups: {args.groups}")
    print(f"Metric: {args.metric}")
    print(f"Metric meaning: {metric_description(args.metric)}")
    print(
        f"Speed threshold: >= {args.min_speed_knots} kt, "
        f"using SPEED / {args.speed_scale}"
    )
    if args.metric == "point_count":
        print(
            "Point-count mode: every qualifying AIS position report contributes "
            "1 to its containing pixel."
        )
    elif args.require_both_endpoint_speeds:
        print("Segment speed test: both endpoints must meet the threshold.")
    else:
        print("Segment speed test: average endpoint speed must meet the threshold.")
    print(f"Zooms: {args.min_zoom} to {args.max_zoom}")
    print(f"Write analytical value pixels: {getattr(args, 'write_value_pixels', False)}")
    print(f"Write analytical value GeoTIFF tiles: {getattr(args, 'write_value_tiffs', False)}")
    print(f"Write analytical composite GeoTIFF: {getattr(args, 'write_value_composite_geotiff', False)}")
    print(f"Analytical value output zooms: {getattr(args, 'value_output_zooms', None)}")
        
    if args.min_zoom < 0 or args.max_zoom < args.min_zoom:
        raise ValueError("Invalid zoom range.")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    groups_without_all = [g for g in args.groups if g.upper() != "ALL"]
    if groups_without_all:
        group_slug = "_".join(safe_slug(g) for g in groups_without_all)
    else:
        group_slug = "all_vessels"

    if args.output_name:
        output_name = args.output_name
    else:
        output_name = (
            f"ais_2025_{group_slug}_{args.metric}_ge_{safe_slug(str(args.min_speed_knots))}"
            f"kt_z{args.min_zoom}_{args.max_zoom}"
        )

    work_dir = output_dir / f"_work_{output_name}"
    if work_dir.exists():
        shutil.rmtree(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)

    xyz_root = output_dir / f"{output_name}_xyz" if args.write_xyz else None
    if xyz_root is not None and xyz_root.exists():
        shutil.rmtree(xyz_root)

    mbtiles_path = output_dir / f"{output_name}.mbtiles"
    mbtiles_conn = None

    try:
        if args.metric == "point_count":
            density_source_path = build_point_count_parquet(
                args=args,
                work_dir=work_dir,
            )
        else:
            density_source_path = build_segments_parquet(
                args=args,
                work_dir=work_dir,
            )

        if args.write_mbtiles:
            mbtiles_conn = init_mbtiles(
                path=mbtiles_path,
                name=output_name,
                min_zoom=args.min_zoom,
                max_zoom=args.max_zoom,
                metric=args.metric,
            )

        render_db_path = work_dir / "render.duckdb"
        con = duckdb.connect(render_db_path.as_posix())
        
        apply_duckdb_settings(
            con=con,
            args=args,
            work_dir=work_dir,
            temp_subdir="render",
        )

        total_tiles = 0

        for zoom in range(args.min_zoom, args.max_zoom + 1):
            stage_dir = work_dir / f"pixel_parts_z{zoom}"

            has_parts = build_pixel_parts_for_zoom(
                source_path=density_source_path,
                zoom=zoom,
                stage_dir=stage_dir,
                args=args,
            )

            if not has_parts:
                print(f"Zoom {zoom}: no pixel parts, skipping.")
                continue

            table_name, pixel_count = aggregate_pixel_parts(
                con=con,
                stage_dir=stage_dir,
                zoom=zoom,
            )

            if pixel_count > 0:
                export_value_outputs(
                    con=con,
                    table_name=table_name,
                    zoom=zoom,
                    args=args,
                    output_dir=output_dir,
                    output_name=output_name,
                    pixel_count=pixel_count,
                )

                total_tiles += render_zoom(
                    con=con,
                    table_name=table_name,
                    zoom=zoom,
                    args=args,
                    xyz_root=xyz_root,
                    mbtiles_conn=mbtiles_conn,
                )

            con.execute(f"DROP TABLE IF EXISTS {table_name}")

            if not args.keep_work:
                shutil.rmtree(stage_dir, ignore_errors=True)

        con.close()

        if mbtiles_conn is not None:
            mbtiles_conn.commit()
            mbtiles_conn.close()

        print("")
        print("Done.")
        print(f"Tiles written: {total_tiles:,}")

        if args.write_mbtiles:
            print(f"MBTiles: {mbtiles_path}")
        if args.write_xyz:
            print(f"XYZ folder: {xyz_root}")

    finally:
        if mbtiles_conn is not None:
            try:
                mbtiles_conn.close()
            except Exception:
                pass

        if not args.keep_work:
            shutil.rmtree(work_dir, ignore_errors=True)

if __name__ == "__main__":
    main()