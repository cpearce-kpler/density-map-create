# -*- coding: utf-8 -*-
"""
Resumable AIS density-map pipeline.

This refactored single-file version separates analytical, render and export
state; uses compact production checkpoints; and provides dedicated vectorised
zero-blur rendering while retaining compatibility with existing resumable runs.

Original author: Craig Pearce
Refactor version: 2026-09-02 benchmark-validated performance revision 2
"""

from __future__ import annotations

import argparse
import copy
import csv
import ctypes
import hashlib
import io
import json
import math
import platform
import threading
import os
import re
import shutil
import socket
import sqlite3
import sys
import traceback
import uuid
from datetime import datetime, timezone
from dataclasses import dataclass
from collections import defaultdict
from concurrent.futures import (
    FIRST_COMPLETED, ProcessPoolExecutor, ThreadPoolExecutor, as_completed, wait,
)
from contextlib import contextmanager, nullcontext
from multiprocessing import get_context as multiprocessing_get_context
import time
from pathlib import Path
from typing import Any, Iterable, Iterator, MutableMapping

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from PIL import Image
from tqdm import tqdm


# Optional accelerators and diagnostics. The script retains safe fallbacks when
# they are unavailable, but the optimised path benefits substantially from them.
try:
    import psutil
except ImportError:  # pragma: no cover - optional diagnostics only
    psutil = None

try:
    from numba import get_num_threads as numba_get_num_threads
    from numba import njit, prange, set_num_threads as numba_set_num_threads
except ImportError:  # pragma: no cover - Python fallback remains available
    njit = None
    prange = range
    numba_get_num_threads = None
    numba_set_num_threads = None

try:
    from scipy.ndimage import convolve as scipy_ndimage_convolve
except ImportError:  # pragma: no cover - legacy splat fallback remains available
    scipy_ndimage_convolve = None

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


RUN_STATE_SCHEMA_VERSION = 2
# The adjacent descending pyramid is analytically equivalent to the previous
# direct-from-max reduction, so keep this version stable for checkpoint resumption.
OUTPUT_ALGORITHM_VERSION = "2026-08-16-maxzoom-streaming-v2"
STAGE_MARKER_FILENAME = "_COMPLETE.json"
PERFORMANCE_IMPLEMENTATION_VERSION = "2026-09-02-benchmark-validated-output-v2"
SEGMENT_CHECKPOINT_VERSION = 2
RENDER_SEMANTIC_VERSION = 2
RENDER_SOURCE_CACHE_VERSION = 2
RENDER_BAND_EXECUTION_VERSION = 2
GEOTIFF_WRITER_VERSION = 2
XYZ_EXPORT_LAYOUT_VERSION = 1
EXPORT_LAYOUT_VERSION = 2

# File-system layout version. This is deliberately NOT part of the analytical
# configuration hash because it changes only path names, not map values.
PATH_LAYOUT_VERSION = 2

# Compact internal/output names keep Windows paths comfortably below MAX_PATH.
# An 8-character key preserves isolation if multiple named outputs share one
# output directory, while remaining much shorter than embedding output_name.
COMPACT_WORK_PREFIX = "_work"
COMPACT_STATE_PREFIX = "_state"
COMPACT_VALUE_PIXELS_PREFIX = "value_pixels"
COMPACT_VALUE_TIFFS_PREFIX = "value_geotiff_tiles"
COMPACT_VALUE_COMPOSITE_PREFIX = "value_composite"
COLOUR_STOPS = np.array([0.0, 0.25, 0.55, 0.78, 1.0], dtype=np.float32)
COLOUR_REDS = np.array([0, 0, 255, 255, 255], dtype=np.float32)
COLOUR_GREENS = np.array([45, 210, 255, 145, 0], dtype=np.float32)
COLOUR_BLUES = np.array([255, 255, 0, 0, 0], dtype=np.float32)

def get_args_for_spyder() -> argparse.Namespace:
    """
    Settings for running directly from Spyder.

    Change these values here instead of using PowerShell command-line arguments.
    """
    args = parse_args([])

    # Input / output paths
    args.ais_folder = r"C:\Users\Craig Pearce\Desktop\Data_sets\ais_files\ais_2025"
    args.vessel_csv = r"C:\Users\Craig Pearce\Desktop\Data_sets\vessel_data\MT_vessel_data.csv"
    args.output_dir = r"C:\Users\Craig Pearce\Desktop\Data_sets\ais_files\heatmap\density_cargo_track_km_z11_2025_sp1_blur1_xyz"
    
    # One-time migration only. Leave None for normal resumable runs.
    args.legacy_first_incomplete_zoom = None

    # Set an explicit name when you want resumption to target one exact output.
    # None derives a name from dataset_label / the AIS folder name.
    args.output_name = None
    args.dataset_label = None  # e.g. "ais_2023"; None infers it from ais_folder.

    # Use ["ALL"] for all vessels, or one/more COMFLEET_GROUPEDTYPE values.
    args.groups = ["DRY BREAKBULK", "DRY BULK"]

    # "track_km" gives the route-density look.
    # "vessel_hours" gives time-spent-moving density.
    args.metric = "track_km"

    # Your SPEED column is knots * 10.
    # Therefore min_speed_knots = 1.0 means raw SPEED > 10.
    args.speed_scale = 10.0
    args.min_speed_knots = 1.0

    # Zoom range
    args.min_zoom = 0
    args.max_zoom = 11

    # Track quality controls
    args.max_gap_minutes = 20
    args.max_implied_speed_knots = 40.0

    # DuckDB resource controls. duckdb_temp_dir is a BASE directory.
    # The script creates a separate run root and execution subdirectory beneath it.
    args.threads = 2
    args.duckdb_memory_limit = "32GB"
    args.duckdb_temp_dir = r"C:\ais_duckdb_temp"
    args.duckdb_preserve_insertion_order = False

    # SHIP_ID hash sharding.
    args.ship_id_shards = 8

    # Tile rendering controls
    args.blur_radius = 1
    args.sample_step_px = 3.0
    args.colour_quantile = 0.995

    # -------------------------------------------------------------------------
    # Performance optimisation controls
    # -------------------------------------------------------------------------

    # True: rasterise vessel segments only at args.max_zoom, then build a
    # descending analytical pyramid one adjacent zoom at a time:
    # max_zoom -> max_zoom - 1 -> ... -> min_zoom. Each step sums 2 x 2 source
    # pixels, so every successive aggregation scans a smaller analytical table.
    args.derive_lower_zooms_from_max = True

    # Number of rows fetched per DuckDB Arrow batch during streaming render and
    # composite GeoTIFF writing. Higher can be faster but uses more memory.
    args.render_row_batch_size = 500_000

    # Write stage timing/profiling information to the run-state folder.
    args.profile_stages = True
    # Sample CPU/RAM/process I/O to CSV and print a liveness heartbeat during
    # long operations so apparent console silence can be diagnosed.
    args.profile_resource_sample_seconds = 15.0
    args.profile_console_heartbeat_seconds = 300.0

    # Prevent Windows system sleep while a long production run is active.
    # The display may still switch off. The state is always restored on exit.
    args.prevent_windows_sleep = True


    # Native/vectorised rasterisation. Numba removes the Python loop over every
    # segment/sample. Set False only when diagnosing compatibility issues.
    args.use_numba_rasterizer = True
    args.numba_threads = 0  # 0 lets Numba choose; otherwise set a physical-core count.

    # Larger Arrow batches and NumPy sample buffers reduce Python/file overhead.
    args.segment_batch_size = 250_000
    args.pixel_flush_threshold = 5_000_000
    args.arrow_batch_readahead = 16
    args.arrow_fragment_readahead = 8

    # Intermediate pixel-part Parquet files favour write/read speed.
    args.pixel_part_parquet_compression = "zstd"
    args.pixel_part_parquet_compression_level = 1
    # DuckDB checkpoint/final Parquet writes also favour throughput. The helper
    # automatically falls back if an older DuckDB lacks COMPRESSION_LEVEL.
    args.duckdb_parquet_compression_level = 1

    # Segment shards can be built in parallel. Keep 1 for the safest Spyder/
    # Windows behaviour; 2 is a useful benchmark on fast NVMe storage.
    args.segment_shard_workers = 1
    args.segment_worker_duckdb_threads = None
    # Compact stores only lon1/lat1/lon2/lat2/value. Diagnostic retains extra
    # segment fields for investigations at the cost of substantially more I/O.
    args.segment_checkpoint_schema = "compact"

    # "compact" reuses the aggregated checkpoint (gx, gy, value) instead of
    # recalculating and sorting a second enriched billion-row Parquet export.
    # Use "enriched" only when all convenience coordinate columns are required.
    args.value_pixels_schema = "compact"

    # Lossless GeoTIFF settings selected by the corrected z11 benchmarks.
    # These preserve the Float32 values/extent/CRS/nodata semantics while avoiding
    # the severe late-write slowdown of DEFLATE + predictor 3 + ALL_CPUS.
    args.geotiff_compression = "zstd"
    args.geotiff_compression_level = 1
    args.geotiff_predictor = None
    args.geotiff_num_threads = "1"
    args.geotiff_block_size = 512
    args.geotiff_write_tiles_per_chunk = 64

    # Visual tile pipeline. MBTiles is the canonical rendered PNG archive.
    # Whole-tile workers perform placement, blur, colourisation and PNG encoding.
    args.use_vectorized_tile_blur = True
    args.png_compress_level = 1
    args.png_optimize = False
    args.tile_render_workers = 8
    args.tile_render_queue = 32

    # High-zoom memory control. Destination render/checkpoint bands remain 16
    # tile rows for bounded memory and fine-grained resumption. The Parquet cache
    # itself is partitioned into 64-row groups, which was the fastest exact-valid
    # tested layout and reduced file fragmentation by more than tenfold.
    args.render_stream_mode = "auto"  # auto, global_sort, or banded
    args.render_banded_min_zoom = 10
    args.render_banded_min_pixels = 100_000_000
    args.render_tile_band_rows = 16
    args.render_cache_partition_band_rows = 64
    args.render_cache_duckdb_threads = 4
    args.render_tile_parts_compression = "zstd"
    args.render_tile_parts_compression_level = 1
    args.delete_render_tile_parts_after_mbtiles = True

    # MBTiles is written first. XYZ is then copied from the already-encoded PNG
    # blobs and can resume independently by tile-X column.
    args.mbtiles_insert_batch_size = 1_000
    args.mbtiles_cache_mb = 256
    args.xyz_extract_workers = 8
    args.xyz_extract_queue = 64

    # Output choices. Requesting XYZ implicitly requires the canonical MBTiles
    # archive even when write_mbtiles is False.
    args.write_xyz = True
    args.write_mbtiles = False

    # Analytical value outputs.
    # These preserve the pre-colour-rendering density values.
    # For --metric track_km: value = accumulated vessel track-km per Web Mercator pixel.
    # For --metric vessel_hours: value = accumulated vessel-hours per Web Mercator pixel.
    args.write_value_pixels = False

    # Extra analytical Float32 GeoTIFF outputs.
    # write_value_tiffs creates many separate 256 x 256 GeoTIFF tiles.
    # write_value_composite_geotiff creates one composite GeoTIFF per selected zoom.
    # The composite is usually the better QGIS option.
    args.write_value_tiffs = False
    args.write_value_composite_geotiff = False

    # Which zooms should get analytical outputs?
    # [args.max_zoom] means only the chosen maximum zoom.
    # [] means all processed zooms.
    args.value_output_zooms = [args.max_zoom]

    # Optional cap for value TIFF tile export during testing. Use None for no cap.
    args.value_tiff_tile_limit = None

    # -------------------------------------------------------------------------
    # Restart / resume controls
    # -------------------------------------------------------------------------

    # True: reuse an existing output with this output_name and continue.
    # False: delete only this named output and start it again from the beginning.
    args.resume_existing = True

    # Refuse to mix density outputs made with different analytical/render settings.
    args.allow_resume_setting_mismatch = False

    # Write a compact aggregated-pixel Parquet checkpoint for every zoom. This
    # allows rendering to restart without re-rasterising all track segments.
    args.checkpoint_aggregated_pixels = True

    # Preserve useful intermediate Parquet/DuckDB files after any failure.
    # They are removed only after every requested zoom is complete, unless
    # keep_work_after_success is True.
    args.keep_work_on_failure = True
    args.keep_work_after_success = False

    # Keep the run-specific DuckDB spill root after a failed execution for
    # diagnostics. A resumed execution gets a NEW execution subdirectory, so
    # stale spill files are never reused by DuckDB.
    args.keep_duckdb_temp_on_failure = True
    args.keep_duckdb_temp_after_success = False

    # Progress / ETA controls for long rasterisation stages.
    # This writes a small JSON file and updates the tqdm progress display with
    # percent complete, throughput and estimated remaining time.
    args.write_progress_json = True
    args.progress_update_seconds = 15.0

    # Set True only when you have confirmed that an existing lock belongs to a
    # dead process and automatic stale-lock detection cannot remove it.
    args.force_remove_output_lock = False

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


def short_output_key(output_name: str) -> str:
    """Return a stable short key used only to keep generated paths compact."""
    return hashlib.sha256(str(output_name).encode("utf-8")).hexdigest()[:8]


def compact_named_dir(output_dir: Path, prefix: str, output_name: str) -> Path:
    return output_dir / f"{prefix}_{short_output_key(output_name)}"


def prefer_existing_path(compact_path: Path, legacy_path: Path) -> Path:
    """Prefer the new compact layout, but resume an existing legacy layout."""
    if compact_path.exists():
        return compact_path
    if legacy_path.exists():
        return legacy_path
    return compact_path


def compact_work_dir(output_dir: Path, output_name: str) -> Path:
    return compact_named_dir(output_dir, COMPACT_WORK_PREFIX, output_name)


def compact_state_dir(output_dir: Path, output_name: str) -> Path:
    return compact_named_dir(output_dir, COMPACT_STATE_PREFIX, output_name)


def resolve_work_dir(output_dir: Path, output_name: str) -> Path:
    return prefer_existing_path(
        compact_work_dir(output_dir, output_name),
        output_dir / f"_work_{output_name}",
    )


def resolve_state_dir(output_dir: Path, output_name: str) -> Path:
    return prefer_existing_path(
        compact_state_dir(output_dir, output_name),
        output_dir / f"{output_name}_run_state",
    )


def compact_analytical_root(output_dir: Path, prefix: str, output_name: str) -> Path:
    return compact_named_dir(output_dir, prefix, output_name)


def analytical_root_candidates(
    output_dir: Path,
    output_name: str,
    compact_prefix: str,
    legacy_suffix: str,
) -> list[Path]:
    return [
        compact_analytical_root(output_dir, compact_prefix, output_name),
        output_dir / f"{output_name}{legacy_suffix}",
    ]


def resolve_analytical_root(
    output_dir: Path,
    output_name: str,
    compact_prefix: str,
    legacy_suffix: str,
) -> Path:
    compact_root, legacy_root = analytical_root_candidates(
        output_dir, output_name, compact_prefix, legacy_suffix
    )
    return prefer_existing_path(compact_root, legacy_root)


def value_pixels_root(output_dir: Path, output_name: str) -> Path:
    return resolve_analytical_root(
        output_dir, output_name, COMPACT_VALUE_PIXELS_PREFIX, "_value_pixels"
    )


def value_tiffs_root(output_dir: Path, output_name: str) -> Path:
    return resolve_analytical_root(
        output_dir, output_name, COMPACT_VALUE_TIFFS_PREFIX, "_value_geotiff_tiles"
    )


def value_composite_root(output_dir: Path, output_name: str) -> Path:
    return resolve_analytical_root(
        output_dir, output_name, COMPACT_VALUE_COMPOSITE_PREFIX,
        "_value_composite_geotiff",
    )


def all_analytical_roots(output_dir: Path, output_name: str) -> list[Path]:
    roots: list[Path] = []
    for prefix, suffix in (
        (COMPACT_VALUE_PIXELS_PREFIX, "_value_pixels"),
        (COMPACT_VALUE_TIFFS_PREFIX, "_value_geotiff_tiles"),
        (COMPACT_VALUE_COMPOSITE_PREFIX, "_value_composite_geotiff"),
    ):
        roots.extend(analytical_root_candidates(output_dir, output_name, prefix, suffix))
    return roots


def compact_lock_path(output_dir: Path, output_name: str) -> Path:
    return output_dir / f".run_{short_output_key(output_name)}.lock"


def resolve_lock_path(output_dir: Path, output_name: str) -> Path:
    return prefer_existing_path(
        compact_lock_path(output_dir, output_name),
        output_dir / f".{output_name}.run.lock",
    )


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    """Write JSON atomically using a deliberately short temporary filename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    # Do not repeat the destination filename here: on Windows that can push an
    # otherwise valid path beyond MAX_PATH. The UUID fragment still makes the
    # temporary name collision-resistant within the destination directory.
    temp_path = path.parent / f".tmp_{os.getpid()}_{uuid.uuid4().hex[:12]}"
    temp_path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temp_path, path)


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def stable_json_hash(data: dict[str, Any]) -> str:
    payload = json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def normalised_path(value: str | Path | None) -> str | None:
    if value is None:
        return None
    return str(Path(value).expanduser().resolve(strict=False))


@dataclass(frozen=True)
class StageFingerprint:
    """Primary stage hash plus compatible hashes accepted from older runs."""

    primary: str
    aliases: tuple[str, ...] = ()

    @property
    def accepted(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys((self.primary, *self.aliases)))


@dataclass(frozen=True)
class PipelineConfigs:
    """Settings grouped by the stage whose outputs they materially define."""

    segment: dict[str, Any]
    analytical: dict[str, Any]
    render: dict[str, Any]
    export: dict[str, Any]

    def as_dict(self) -> dict[str, dict[str, Any]]:
        return {
            "segment": self.segment,
            "analytical": self.analytical,
            "render": self.render,
            "export": self.export,
        }


@dataclass(frozen=True)
class RenderSourceCache:
    """Compact analytical pixels partitioned into tile-row bands for rendering."""

    root: Path
    cache_hash: str
    band_tile_rows: int
    min_base_tile_y: int
    max_base_tile_y: int
    parquet_file_count: int
    byte_size: int


@dataclass(frozen=True)
class RenderZoomResult:
    """Summary returned after canonical MBTiles rendering."""

    total_tiles: int
    tiles_written_this_execution: int
    rows_streamed_this_execution: int
    png_bytes_this_execution: int
    stream_mode: str


@dataclass(frozen=True)
class XYZExportResult:
    """Summary returned after deriving XYZ files from MBTiles."""

    total_tiles: int
    tiles_written_this_execution: int
    bytes_written_this_execution: int


def _normalised_groups(args: argparse.Namespace) -> list[str]:
    return [str(group).strip().upper() for group in args.groups]


def _normalised_value_zooms(args: argparse.Namespace) -> list[int] | None:
    zooms = getattr(args, "value_output_zooms", None)
    return None if zooms is None else [int(zoom) for zoom in zooms]


def build_track_value_config(args: argparse.Namespace) -> dict[str, Any]:
    """Settings that determine which vessel segments and values are produced."""
    groups = _normalised_groups(args)
    uses_vessel_metadata = any(group != "ALL" for group in groups)
    return {
        "algorithm_version": OUTPUT_ALGORITHM_VERSION,
        "ais_folder": normalised_path(args.ais_folder),
        "vessel_csv": normalised_path(args.vessel_csv) if uses_vessel_metadata else None,
        "groups": groups,
        "metric": str(args.metric),
        "ship_id_column": str(args.ship_id_column),
        "lat_column": str(args.lat_column),
        "lon_column": str(args.lon_column),
        "speed_column": str(args.speed_column),
        "timestamp_column": str(args.timestamp_column),
        "vessel_ship_id_column": (
            str(args.vessel_ship_id_column) if uses_vessel_metadata else None
        ),
        "vessel_group_column": (
            str(args.vessel_group_column) if uses_vessel_metadata else None
        ),
        "speed_scale": float(args.speed_scale),
        "min_speed_knots": float(args.min_speed_knots),
        "require_both_endpoint_speeds": bool(args.require_both_endpoint_speeds),
        "max_gap_minutes": int(args.max_gap_minutes),
        "max_implied_speed_knots": float(args.max_implied_speed_knots),
    }


def build_segment_config(args: argparse.Namespace) -> dict[str, Any]:
    """Settings that define reusable staged-point and segment checkpoints."""
    return {
        **build_track_value_config(args),
        "segment_checkpoint_version": SEGMENT_CHECKPOINT_VERSION,
        "segment_checkpoint_schema": str(
            getattr(args, "segment_checkpoint_schema", "compact")
        ).lower(),
        "ship_id_shards": int(args.ship_id_shards),
    }


def build_analytical_config(args: argparse.Namespace) -> dict[str, Any]:
    """Settings that define the pre-rendering density values."""
    return {
        **build_track_value_config(args),
        "min_zoom": int(args.min_zoom),
        "max_zoom": int(args.max_zoom),
        "sample_step_px": float(args.sample_step_px),
        "max_segment_samples": int(args.max_segment_samples),
        "derive_lower_zooms_from_max": bool(
            getattr(args, "derive_lower_zooms_from_max", True)
        ),
        "tile_size": TILE_SIZE,
    }


def build_render_config(args: argparse.Namespace, analytical_hash: str) -> dict[str, Any]:
    """Settings that define rendered PNG content and requested visual products.

    Worker counts, queue sizes and band sizes are performance-only controls and are
    deliberately excluded so they can be benchmarked without invalidating identical
    completed tiles.
    """
    return {
        "render_semantic_version": RENDER_SEMANTIC_VERSION,
        "visual_storage_mode": "mbtiles_primary_xyz_derived_v1",
        "analytical_hash": analytical_hash,
        "blur_radius": int(args.blur_radius),
        "colour_quantile": float(args.colour_quantile),
        "alpha_min": int(args.alpha_min),
        "alpha_max": int(args.alpha_max),
        "tile_size": TILE_SIZE,
        "png_compress_level": int(getattr(args, "png_compress_level", 1)),
        "png_optimize": bool(getattr(args, "png_optimize", False)),
    }


def normalise_geotiff_predictor(value: Any) -> int | None:
    """Return a GDAL predictor value, or None to omit prediction entirely."""
    if value is None:
        return None
    if isinstance(value, bool):
        return 3 if value else None
    text_value = str(value).strip().lower()
    if text_value in {"", "none", "off", "false", "0"}:
        return None
    predictor = int(value)
    if predictor not in {1, 2, 3}:
        raise ValueError("geotiff_predictor must be None/off/0 or one of 1, 2, 3")
    return predictor


def validate_geotiff_block_size(value: Any) -> int:
    block_size = int(value)
    if block_size < 16 or block_size % 16 != 0:
        raise ValueError("geotiff_block_size must be a positive multiple of 16")
    return block_size


def geotiff_output_settings(
    args: argparse.Namespace,
    *,
    output_kind: str,
    block_size: int,
    write_tiles_per_chunk: int | None = None,
) -> dict[str, Any]:
    """Canonical, JSON-safe settings used for output fingerprints and metadata."""
    payload: dict[str, Any] = {
        "writer_version": GEOTIFF_WRITER_VERSION,
        "output_kind": str(output_kind),
        "compression": str(getattr(args, "geotiff_compression", "zstd")).lower(),
        "compression_level": int(getattr(args, "geotiff_compression_level", 1)),
        "predictor": normalise_geotiff_predictor(
            getattr(args, "geotiff_predictor", None)
        ),
        "compression_threads": str(getattr(args, "geotiff_num_threads", "1")),
        "block_size": validate_geotiff_block_size(block_size),
        "tiled": True,
        "bigtiff": "IF_SAFER",
    }
    if output_kind == "composite":
        payload.update(
            {
                "sparse_ok": True,
                "write_strategy": "contiguous_horizontal_strip",
                "write_tiles_per_chunk": max(1, int(write_tiles_per_chunk or 1)),
            }
        )
    else:
        payload["write_strategy"] = "one_analytical_tile_per_tiff"
    return payload


def build_export_config(args: argparse.Namespace, analytical_hash: str) -> dict[str, Any]:
    """Settings that define analytical export files, independently of rendering."""
    write_tiles = bool(getattr(args, "write_value_tiffs", False))
    write_composite = bool(getattr(args, "write_value_composite_geotiff", False))
    tile_settings = (
        geotiff_output_settings(
            args,
            output_kind="tiles",
            block_size=TILE_SIZE,
        )
        if write_tiles
        else None
    )
    composite_settings = (
        geotiff_output_settings(
            args,
            output_kind="composite",
            block_size=int(getattr(args, "geotiff_block_size", 512)),
            write_tiles_per_chunk=int(
                getattr(args, "geotiff_write_tiles_per_chunk", 64)
            ),
        )
        if write_composite
        else None
    )
    return {
        "export_layout_version": EXPORT_LAYOUT_VERSION,
        "analytical_hash": analytical_hash,
        "write_value_pixels": bool(getattr(args, "write_value_pixels", False)),
        "write_value_tiffs": write_tiles,
        "write_value_composite_geotiff": write_composite,
        "value_output_zooms": _normalised_value_zooms(args),
        "value_tiff_tile_limit": getattr(args, "value_tiff_tile_limit", None),
        "value_pixels_schema": str(
            getattr(args, "value_pixels_schema", "compact")
        ).lower(),
        "geotiff_tile_settings": tile_settings,
        "geotiff_composite_settings": composite_settings,
    }


def build_pipeline_configs(args: argparse.Namespace) -> PipelineConfigs:
    analytical = build_analytical_config(args)
    analytical_hash = stable_json_hash(analytical)
    return PipelineConfigs(
        segment=build_segment_config(args),
        analytical=analytical,
        render=build_render_config(args, analytical_hash),
        export=build_export_config(args, analytical_hash),
    )


def _flatten_config(value: Any, prefix: str = "") -> dict[str, Any]:
    if not isinstance(value, dict):
        return {prefix: value}
    flattened: dict[str, Any] = {}
    for key, item in value.items():
        child = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(item, dict):
            flattened.update(_flatten_config(item, child))
        else:
            flattened[child] = item
    return flattened


def config_differences(old: dict[str, Any], new: dict[str, Any]) -> list[str]:
    old_flat = _flatten_config(old)
    new_flat = _flatten_config(new)
    differences: list[str] = []
    for key in sorted(set(old_flat) | set(new_flat)):
        if old_flat.get(key) != new_flat.get(key):
            differences.append(
                f"{key}: saved={old_flat.get(key)!r}, current={new_flat.get(key)!r}"
            )
    return differences


def _legacy_keys_match(
    saved_config: dict[str, Any],
    current_config: dict[str, Any],
    keys: Iterable[str],
) -> bool:
    metadata_keys = {
        "vessel_csv", "vessel_ship_id_column", "vessel_group_column"
    }
    metadata_unused = current_config.get("vessel_csv") is None
    for key in keys:
        if metadata_unused and key in metadata_keys:
            continue
        if saved_config.get(key) != current_config.get(key):
            return False
    return True


LEGACY_TRACK_VALUE_KEYS = (
    "algorithm_version", "ais_folder", "vessel_csv", "groups", "metric",
    "ship_id_column", "lat_column", "lon_column", "speed_column",
    "timestamp_column", "vessel_ship_id_column", "vessel_group_column",
    "speed_scale", "min_speed_knots", "require_both_endpoint_speeds",
    "max_gap_minutes", "max_implied_speed_knots",
)
LEGACY_ANALYTICAL_KEYS = (
    *LEGACY_TRACK_VALUE_KEYS,
    "min_zoom", "max_zoom", "sample_step_px", "max_segment_samples",
    "derive_lower_zooms_from_max", "tile_size",
)
LEGACY_RENDER_KEYS = (
    "blur_radius", "colour_quantile", "alpha_min", "alpha_max", "tile_size",
    "write_xyz", "write_mbtiles",
)
LEGACY_EXPORT_KEYS = (
    "write_value_pixels", "write_value_tiffs",
    "write_value_composite_geotiff", "value_output_zooms",
    "value_tiff_tile_limit",
)


def legacy_stage_aliases(
    saved_config: dict[str, Any],
    legacy_hash: str,
    current: PipelineConfigs,
) -> dict[str, list[str]]:
    """Accept old all-in-one hashes only where the relevant settings still match."""
    aliases = {stage: [] for stage in ("segment", "analytical", "render", "export")}
    analytical_match = _legacy_keys_match(
        saved_config, current.analytical, LEGACY_ANALYTICAL_KEYS
    )
    segment_match = _legacy_keys_match(
        saved_config, current.segment, (*LEGACY_TRACK_VALUE_KEYS, "ship_id_shards")
    )
    if segment_match:
        aliases["segment"].append(legacy_hash)
    if analytical_match:
        aliases["analytical"].append(legacy_hash)
    if analytical_match and _legacy_keys_match(
        saved_config, current.render, LEGACY_RENDER_KEYS
    ):
        aliases["render"].append(legacy_hash)
    if analytical_match and _legacy_keys_match(
        saved_config, current.export, LEGACY_EXPORT_KEYS
    ):
        aliases["export"].append(legacy_hash)
    return aliases


def primary_hash(value: str | StageFingerprint | Iterable[str]) -> str:
    if isinstance(value, StageFingerprint):
        return value.primary
    if isinstance(value, str):
        return value
    values = tuple(str(item) for item in value)
    if not values:
        raise ValueError("At least one configuration hash is required.")
    return values[0]


def accepted_hashes(value: str | StageFingerprint | Iterable[str]) -> set[str]:
    if isinstance(value, StageFingerprint):
        return set(value.accepted)
    if isinstance(value, str):
        return {value}
    return {str(item) for item in value}


def write_completion_marker(
    path: Path,
    marker_type: str,
    config_hash: str | StageFingerprint | Iterable[str],
    **details: Any,
) -> None:
    payload: dict[str, Any] = {
        "marker_type": marker_type,
        "config_hash": primary_hash(config_hash),
        "completed_at": utc_now_iso(),
    }
    payload.update(details)
    atomic_write_json(path, payload)


def read_valid_completion_marker(
    path: Path,
    config_hash: str | StageFingerprint | Iterable[str],
    marker_type: str | None = None,
) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        marker = read_json(path)
    except Exception:
        return None
    if str(marker.get("config_hash", "")) not in accepted_hashes(config_hash):
        return None
    if marker_type is not None and marker.get("marker_type") != marker_type:
        return None
    return marker


def remove_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path, ignore_errors=False)
    elif path.exists():
        path.unlink()


def remove_path_quietly(path: Path) -> None:
    try:
        remove_path(path)
    except FileNotFoundError:
        pass
    except Exception as exc:
        print(f"Warning: could not remove {path}: {exc}")


def safe_close_duckdb(
    con: duckdb.DuckDBPyConnection | None,
    context: str,
) -> Exception | None:
    if con is None:
        return None
    try:
        con.close()
        return None
    except Exception as exc:
        # A close-time temp-file deletion error should not erase already committed
        # output or mask the original processing exception.
        print(f"Warning: DuckDB close failed during {context}: {exc}")
        return exc


def safe_close_sqlite(
    con: sqlite3.Connection | None,
    context: str,
) -> Exception | None:
    if con is None:
        return None

    first_error: Exception | None = None
    try:
        con.commit()
    except Exception as exc:
        first_error = exc
        print(f"Warning: SQLite commit failed during {context}: {exc}")

    try:
        con.close()
    except Exception as exc:
        if first_error is None:
            first_error = exc
        print(f"Warning: SQLite close failed during {context}: {exc}")

    return first_error


def process_is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


class OutputRunLock:
    """Prevent two processes from writing the same named output concurrently."""

    def __init__(self, path: Path, force_remove: bool = False):
        self.path = path
        self.force_remove = bool(force_remove)
        self.token = uuid.uuid4().hex
        self.acquired = False

    def _existing_lock_is_stale(self) -> bool:
        try:
            data = read_json(self.path)
        except Exception:
            return self.force_remove

        same_host = data.get("hostname") == socket.gethostname()
        try:
            pid = int(data.get("pid", -1))
        except Exception:
            pid = -1

        return bool(self.force_remove or (same_host and not process_is_running(pid)))

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "pid": os.getpid(),
            "hostname": socket.gethostname(),
            "token": self.token,
            "created_at": utc_now_iso(),
        }

        for attempt in range(2):
            try:
                fd = os.open(
                    self.path,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                )
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    json.dump(payload, handle, indent=2)
                self.acquired = True
                return
            except FileExistsError:
                if attempt == 0 and self._existing_lock_is_stale():
                    remove_path_quietly(self.path)
                    continue
                try:
                    existing = read_json(self.path)
                except Exception:
                    existing = {"path": str(self.path)}
                raise RuntimeError(
                    "This named output is already locked by another execution. "
                    f"Lock details: {existing}. Use a different output_name for "
                    "parallel vessel-type runs."
                )

    def release(self) -> None:
        if not self.acquired:
            return
        try:
            existing = read_json(self.path)
            if existing.get("token") == self.token:
                self.path.unlink(missing_ok=True)
        except Exception as exc:
            print(f"Warning: could not release output lock {self.path}: {exc}")
        finally:
            self.acquired = False


class RunStateManager:
    """Stage-aware resumable state for analytical, MBTiles, XYZ and export products."""

    STAGES = ("segment", "analytical", "render", "export")

    def __init__(self, state_dir: Path, manifest: dict[str, Any]):
        self.state_dir = state_dir
        self.manifest_path = state_dir / "run_state.json"
        self.run_complete_marker = state_dir / "RUN_COMPLETE.json"
        self.zoom_marker_dir = state_dir / "zoom_markers"
        self.manifest = manifest

    def fingerprint(self, stage: str) -> StageFingerprint:
        if stage not in self.STAGES:
            raise KeyError(f"Unknown pipeline stage fingerprint: {stage}")
        hashes = self.manifest.setdefault("hashes", {})
        aliases = self.manifest.setdefault("hash_aliases", {}).setdefault(stage, [])
        return StageFingerprint(
            primary=str(hashes[stage]),
            aliases=tuple(str(value) for value in aliases),
        )

    @property
    def segment_fingerprint(self) -> StageFingerprint:
        return self.fingerprint("segment")

    @property
    def analytical_fingerprint(self) -> StageFingerprint:
        return self.fingerprint("analytical")

    @property
    def render_fingerprint(self) -> StageFingerprint:
        return self.fingerprint("render")

    @property
    def xyz_fingerprint(self) -> StageFingerprint:
        render = self.render_fingerprint
        return StageFingerprint(
            primary=stable_json_hash(
                {
                    "render_hash": render.primary,
                    "xyz_layout_version": XYZ_EXPORT_LAYOUT_VERSION,
                }
            ),
            aliases=tuple(
                stable_json_hash(
                    {
                        "render_hash": alias,
                        "xyz_layout_version": XYZ_EXPORT_LAYOUT_VERSION,
                    }
                )
                for alias in render.aliases
            ),
        )

    @property
    def export_fingerprint(self) -> StageFingerprint:
        return self.fingerprint("export")

    @property
    def config_hash(self) -> str:
        """Backward-compatible alias for the analytical primary hash."""
        return self.analytical_fingerprint.primary

    @property
    def completion_hash(self) -> str:
        return stable_json_hash(
            {
                "render": self.render_fingerprint.primary,
                "xyz": self.xyz_fingerprint.primary,
                "export": self.export_fingerprint.primary,
            }
        )

    @property
    def run_id(self) -> str:
        return str(self.manifest["run_id"])

    def save(self) -> None:
        self.manifest["updated_at"] = utc_now_iso()
        atomic_write_json(self.manifest_path, self.manifest)

    def zoom_marker_path(self, zoom: int) -> Path:
        """Legacy/summary marker retained for migration and external inspection."""
        return self.zoom_marker_dir / f"zoom_{int(zoom):02d}.complete.json"

    def render_marker_path(self, zoom: int) -> Path:
        """Completion marker for canonical PNG rendering into MBTiles."""
        return self.zoom_marker_dir / f"zoom_{int(zoom):02d}.render.complete.json"

    def xyz_marker_path(self, zoom: int) -> Path:
        return self.zoom_marker_dir / f"zoom_{int(zoom):02d}.xyz.complete.json"

    def xyz_column_marker_dir(self, zoom: int) -> Path:
        return self.state_dir / "xyz_columns" / f"z{int(zoom)}"

    def export_marker_path(self, zoom: int) -> Path:
        return self.zoom_marker_dir / f"zoom_{int(zoom):02d}.export.complete.json"

    def ensure_zoom_entries(self, min_zoom: int, max_zoom: int) -> None:
        zooms = self.manifest.setdefault("zooms", {})
        for zoom in range(int(min_zoom), int(max_zoom) + 1):
            entry = zooms.setdefault(str(zoom), {})
            entry.setdefault("status", "not_started")
            entry.setdefault("render_status", "not_started")
            entry.setdefault("xyz_status", "not_started")
            entry.setdefault("export_status", "not_started")
            entry.setdefault("tiles_written", None)
            entry.setdefault("updated_at", utc_now_iso())
        self.save()

    def _legacy_zoom_marker(self, zoom: int, stage: str) -> dict[str, Any] | None:
        path = self.zoom_marker_path(zoom)
        if not path.exists():
            return None
        try:
            marker = read_json(path)
        except Exception:
            return None
        if str(marker.get("config_hash", "")) not in accepted_hashes(self.fingerprint(stage)):
            return None
        if not str(marker.get("marker_type", "")).startswith("zoom_complete"):
            return None
        return marker

    def is_render_complete(
        self,
        zoom: int,
        args: argparse.Namespace,
        xyz_root: Path | None,
        mbtiles_path: Path,
        mbtiles_conn: sqlite3.Connection | None = None,
    ) -> bool:
        """Validate canonical PNG rendering into MBTiles only."""
        del xyz_root
        if not visual_outputs_requested(args):
            return True
        marker = read_valid_completion_marker(
            self.render_marker_path(zoom),
            self.render_fingerprint,
            marker_type="render_complete",
        )
        if marker is not None and mbtiles_visual_output_exists(
            mbtiles_path=mbtiles_path,
            zoom=zoom,
            expected_tiles=int(marker.get("tiles_written", 0)),
            mbtiles_conn=mbtiles_conn,
        ):
            return True

        legacy = self._legacy_zoom_marker(zoom, "render")
        if legacy is not None and mbtiles_visual_output_exists(
            mbtiles_path=mbtiles_path,
            zoom=zoom,
            expected_tiles=int(legacy.get("tiles_written", 0)),
            mbtiles_conn=mbtiles_conn,
        ):
            self.mark_render_complete(
                zoom,
                tiles_written=int(legacy.get("tiles_written", 0)),
                adopted_from_legacy_marker=True,
            )
            return True
        return False

    def is_xyz_complete(
        self,
        zoom: int,
        args: argparse.Namespace,
        xyz_root: Path | None,
    ) -> bool:
        if not bool(getattr(args, "write_xyz", False)):
            return True
        marker = read_valid_completion_marker(
            self.xyz_marker_path(zoom),
            self.xyz_fingerprint,
            marker_type="xyz_complete",
        )
        if marker is None:
            return False
        return xyz_visual_output_exists(
            xyz_root=xyz_root,
            zoom=zoom,
            expected_tiles=int(marker.get("tiles_written", 0)),
            render_hash=str(marker.get("render_hash", "")),
        )

    def is_export_complete(
        self,
        zoom: int,
        args: argparse.Namespace,
        output_dir: Path,
        output_name: str,
    ) -> bool:
        if not analytical_exports_requested(args, zoom):
            return True
        marker = read_valid_completion_marker(
            self.export_marker_path(zoom),
            self.export_fingerprint,
            marker_type="export_complete",
        )
        if marker is not None and analytical_outputs_exist(
            args=args,
            output_dir=output_dir,
            output_name=output_name,
            zoom=zoom,
        ):
            return True

        legacy = self._legacy_zoom_marker(zoom, "export")
        if legacy is not None and analytical_outputs_exist(
            args=args,
            output_dir=output_dir,
            output_name=output_name,
            zoom=zoom,
        ):
            self.mark_export_complete(
                zoom,
                adopted_from_legacy_marker=True,
            )
            return True
        return False

    def is_zoom_complete(
        self,
        zoom: int,
        args: argparse.Namespace,
        xyz_root: Path | None,
        mbtiles_path: Path,
        output_dir: Path,
        output_name: str,
    ) -> bool:
        render_done = self.is_render_complete(zoom, args, xyz_root, mbtiles_path)
        xyz_done = self.is_xyz_complete(zoom, args, xyz_root)
        export_done = self.is_export_complete(zoom, args, output_dir, output_name)
        complete = bool(render_done and xyz_done and export_done)
        entry = self.manifest.setdefault("zooms", {}).setdefault(str(zoom), {})
        entry["render_status"] = "complete" if render_done else "pending"
        entry["xyz_status"] = "complete" if xyz_done else "pending"
        entry["export_status"] = "complete" if export_done else "pending"
        if complete:
            entry["status"] = "complete"
        elif entry.get("status") == "complete":
            entry["status"] = "pending_outputs"
        entry["updated_at"] = utc_now_iso()
        return complete

    def first_incomplete_zoom(
        self,
        min_zoom: int,
        max_zoom: int,
        args: argparse.Namespace,
        xyz_root: Path | None,
        mbtiles_path: Path,
        output_dir: Path,
        output_name: str,
    ) -> int | None:
        for zoom in range(int(min_zoom), int(max_zoom) + 1):
            if not self.is_zoom_complete(
                zoom, args, xyz_root, mbtiles_path, output_dir, output_name
            ):
                self.save()
                return zoom
        self.save()
        return None

    def mark_zoom_stage(self, zoom: int, status: str, **details: Any) -> None:
        self.run_complete_marker.unlink(missing_ok=True)
        entry = self.manifest.setdefault("zooms", {}).setdefault(str(zoom), {})
        entry.update(details)
        entry["status"] = status
        entry["updated_at"] = utc_now_iso()
        self.manifest["complete"] = False
        self.manifest["last_error"] = None
        self.save()

    def mark_render_complete(self, zoom: int, tiles_written: int, **details: Any) -> None:
        write_completion_marker(
            self.render_marker_path(zoom),
            marker_type="render_complete",
            config_hash=self.render_fingerprint,
            zoom=int(zoom),
            tiles_written=int(tiles_written),
            canonical_storage="mbtiles",
            **details,
        )
        entry = self.manifest.setdefault("zooms", {}).setdefault(str(zoom), {})
        entry.update(details)
        entry.update(
            {
                "render_status": "complete",
                "tiles_written": int(tiles_written),
                "render_completed_at": utc_now_iso(),
                "updated_at": utc_now_iso(),
            }
        )
        self.manifest["last_error"] = None
        self.save()

    def mark_xyz_complete(self, zoom: int, tiles_written: int, **details: Any) -> None:
        write_completion_marker(
            self.xyz_marker_path(zoom),
            marker_type="xyz_complete",
            config_hash=self.xyz_fingerprint,
            zoom=int(zoom),
            tiles_written=int(tiles_written),
            render_hash=self.render_fingerprint.primary,
            xyz_layout_version=XYZ_EXPORT_LAYOUT_VERSION,
            **details,
        )
        entry = self.manifest.setdefault("zooms", {}).setdefault(str(zoom), {})
        entry.update(details)
        entry.update(
            {
                "xyz_status": "complete",
                "xyz_tiles_written": int(tiles_written),
                "xyz_completed_at": utc_now_iso(),
                "updated_at": utc_now_iso(),
            }
        )
        self.manifest["last_error"] = None
        self.save()

    def mark_export_complete(self, zoom: int, **details: Any) -> None:
        write_completion_marker(
            self.export_marker_path(zoom),
            marker_type="export_complete",
            config_hash=self.export_fingerprint,
            zoom=int(zoom),
            **details,
        )
        entry = self.manifest.setdefault("zooms", {}).setdefault(str(zoom), {})
        entry.update(details)
        entry.update(
            {
                "export_status": "complete",
                "export_completed_at": utc_now_iso(),
                "updated_at": utc_now_iso(),
            }
        )
        self.manifest["last_error"] = None
        self.save()

    def mark_zoom_complete(self, zoom: int, tiles_written: int, **details: Any) -> None:
        """Write a compact summary marker after all requested output stages finish."""
        write_completion_marker(
            self.zoom_marker_path(zoom),
            marker_type="zoom_complete_v3",
            config_hash=self.completion_hash,
            zoom=int(zoom),
            tiles_written=int(tiles_written),
            render_hash=self.render_fingerprint.primary,
            xyz_hash=self.xyz_fingerprint.primary,
            export_hash=self.export_fingerprint.primary,
            **details,
        )
        entry = self.manifest.setdefault("zooms", {}).setdefault(str(zoom), {})
        entry.update(details)
        entry.update(
            {
                "status": "complete",
                "render_status": "complete",
                "xyz_status": "complete",
                "export_status": "complete",
                "tiles_written": int(tiles_written),
                "completed_at": utc_now_iso(),
                "updated_at": utc_now_iso(),
            }
        )
        self.manifest["last_error"] = None
        self.save()

    def record_error(self, exc: BaseException) -> None:
        self.run_complete_marker.unlink(missing_ok=True)
        self.manifest["complete"] = False
        self.manifest["last_error"] = {
            "time": utc_now_iso(),
            "type": type(exc).__name__,
            "message": str(exc),
            "traceback": "".join(
                traceback.format_exception(type(exc), exc, exc.__traceback__)
            ),
        }
        self.save()

    def mark_run_complete(self, total_tiles: int) -> None:
        write_completion_marker(
            self.run_complete_marker,
            marker_type="run_complete_v3",
            config_hash=self.completion_hash,
            total_tiles=int(total_tiles),
            render_hash=self.render_fingerprint.primary,
            xyz_hash=self.xyz_fingerprint.primary,
            export_hash=self.export_fingerprint.primary,
        )
        self.manifest["complete"] = True
        self.manifest["completed_at"] = utc_now_iso()
        self.manifest["total_tiles"] = int(total_tiles)
        self.manifest["last_error"] = None
        self.save()


def _json_safe(value: Any) -> Any:
    """Convert common runtime values to JSON-safe Python objects."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if isinstance(value, np.generic):
        return value.item()
    return str(value)


def format_bytes(value: int | float | None) -> str:
    if value is None:
        return "unknown"
    size = float(value)
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    for unit in units:
        if abs(size) < 1024.0 or unit == units[-1]:
            return f"{size:,.2f} {unit}"
        size /= 1024.0
    return f"{size:,.2f} PiB"


class StageProfiler:
    """
    Wall-clock/resource profiler with nested stage and operation events.

    Outputs written beside ``stage_profile.json``:
      - performance_events.jsonl: append-only event stream
      - performance_summary.csv: flat table for analysis
      - performance_system.json: machine/package information

    A context yields a mutable metrics dictionary. Callers can add ``items``,
    ``item_unit``, ``bytes`` and any domain-specific counters before it closes.
    """

    def __init__(
        self,
        path: Path,
        enabled: bool = True,
        resource_sample_seconds: float = 15.0,
        console_heartbeat_seconds: float = 300.0,
    ):
        self.path = path
        self.enabled = bool(enabled)
        self.events_path = path.with_name("performance_events.jsonl")
        self.csv_path = path.with_name("performance_summary.csv")
        self.system_path = path.with_name("performance_system.json")
        self.resources_path = path.with_name("performance_resources.csv")
        self.events: list[dict[str, Any]] = []
        self.started_at = utc_now_iso()
        self._stack: list[tuple[str, float]] = []
        self._lock = threading.RLock()
        self._process = psutil.Process(os.getpid()) if psutil is not None else None
        self.resource_sample_seconds = max(0.0, float(resource_sample_seconds))
        self.console_heartbeat_seconds = max(0.0, float(console_heartbeat_seconds))
        self._resource_stop = threading.Event()
        self._resource_thread: threading.Thread | None = None
        self._last_heartbeat_perf = time.perf_counter()

        if self.enabled and self.path.exists():
            try:
                payload = read_json(self.path)
                self.started_at = str(payload.get("started_at", self.started_at))
                self.events = list(payload.get("events", []))
            except Exception:
                self.events = []

        if self.enabled:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._write_system_info()
            self._write()
            self._start_resource_monitor()

    def _snapshot(self) -> dict[str, Any]:
        snapshot: dict[str, Any] = {
            "perf_counter": time.perf_counter(),
            "process_cpu_seconds": time.process_time(),
        }
        if self._process is None:
            return snapshot
        try:
            memory = self._process.memory_info()
            snapshot["rss_bytes"] = int(memory.rss)
            snapshot["vms_bytes"] = int(memory.vms)
        except Exception:
            pass
        try:
            io_counters = self._process.io_counters()
            snapshot["read_bytes"] = int(getattr(io_counters, "read_bytes", 0))
            snapshot["write_bytes"] = int(getattr(io_counters, "write_bytes", 0))
        except Exception:
            pass
        return snapshot

    def _start_resource_monitor(self) -> None:
        if (
            not self.enabled
            or psutil is None
            or self._process is None
            or self.resource_sample_seconds <= 0
        ):
            return
        try:
            self._process.cpu_percent(interval=None)
            psutil.cpu_percent(interval=None)
        except Exception:
            return
        self._resource_thread = threading.Thread(
            target=self._resource_monitor_loop,
            name="ais-performance-monitor",
            daemon=True,
        )
        self._resource_thread.start()

    def _resource_monitor_loop(self) -> None:
        while not self._resource_stop.wait(self.resource_sample_seconds):
            try:
                self._write_resource_sample()
            except Exception:
                # Diagnostics must never interrupt the analytical workflow.
                pass

    def _write_resource_sample(self) -> None:
        if self._process is None or psutil is None:
            return
        now_perf = time.perf_counter()
        with self._lock:
            if self._stack:
                active_name, active_start = self._stack[-1]
                active_path = " > ".join(name for name, _started in self._stack)
                active_elapsed = max(0.0, now_perf - active_start)
            else:
                active_name = "idle_or_between_stages"
                active_path = active_name
                active_elapsed = 0.0

        process_memory = self._process.memory_info()
        process_io = self._process.io_counters()
        system_memory = psutil.virtual_memory()
        disk = psutil.disk_io_counters()

        # Include spawned shard workers in the resource log. The main-process
        # counters remain separate so thread-heavy DuckDB/Numba stages and
        # process-pool stages can be distinguished during bottleneck analysis.
        child_count = 0
        children_rss = 0
        children_vms = 0
        children_cpu_seconds = 0.0
        children_read_bytes = 0
        children_write_bytes = 0
        try:
            children = self._process.children(recursive=True)
        except Exception:
            children = []
        for child in children:
            try:
                child_memory = child.memory_info()
                child_cpu = child.cpu_times()
                child_io = child.io_counters()
            except Exception:
                continue
            child_count += 1
            children_rss += int(child_memory.rss)
            children_vms += int(child_memory.vms)
            children_cpu_seconds += float(child_cpu.user + child_cpu.system)
            children_read_bytes += int(getattr(child_io, "read_bytes", 0))
            children_write_bytes += int(getattr(child_io, "write_bytes", 0))

        process_read_bytes = int(getattr(process_io, "read_bytes", 0))
        process_write_bytes = int(getattr(process_io, "write_bytes", 0))
        row = {
            "timestamp_utc": utc_now_iso(),
            "active_operation": active_name,
            "active_path": active_path,
            "active_elapsed_seconds": active_elapsed,
            "process_cpu_percent": self._process.cpu_percent(interval=None),
            "system_cpu_percent": psutil.cpu_percent(interval=None),
            "process_rss_bytes": int(process_memory.rss),
            "process_vms_bytes": int(process_memory.vms),
            "child_process_count": int(child_count),
            "children_rss_bytes": int(children_rss),
            "children_vms_bytes": int(children_vms),
            "children_cpu_seconds_total": float(children_cpu_seconds),
            "process_tree_rss_bytes": int(process_memory.rss) + int(children_rss),
            "process_tree_vms_bytes": int(process_memory.vms) + int(children_vms),
            "system_memory_percent": float(system_memory.percent),
            "system_memory_available_bytes": int(system_memory.available),
            "process_read_bytes_total": process_read_bytes,
            "process_write_bytes_total": process_write_bytes,
            "children_read_bytes_total": int(children_read_bytes),
            "children_write_bytes_total": int(children_write_bytes),
            "process_tree_read_bytes_total": process_read_bytes + int(children_read_bytes),
            "process_tree_write_bytes_total": process_write_bytes + int(children_write_bytes),
            "system_disk_read_bytes_total": (
                int(getattr(disk, "read_bytes", 0)) if disk is not None else None
            ),
            "system_disk_write_bytes_total": (
                int(getattr(disk, "write_bytes", 0)) if disk is not None else None
            ),
        }
        fieldnames = list(row)
        write_header = not self.resources_path.exists()
        with self.resources_path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
            writer.writerow(row)
            handle.flush()

        if (
            self.console_heartbeat_seconds > 0
            and now_perf - self._last_heartbeat_perf
            >= self.console_heartbeat_seconds
        ):
            self._last_heartbeat_perf = now_perf
            tqdm.write(
                "[performance heartbeat] "
                f"{active_path} | active {format_duration(active_elapsed)} | "
                f"CPU {float(row['process_cpu_percent']):.1f}% | "
                f"tree RSS {format_bytes(row['process_tree_rss_bytes'])} | "
                f"tree write {format_bytes(row['process_tree_write_bytes_total'])}"
            )

    def close(self) -> None:
        """Stop the background resource monitor and flush one final sample."""
        if self._resource_thread is None:
            return
        self._resource_stop.set()
        self._resource_thread.join(timeout=max(1.0, self.resource_sample_seconds + 1.0))
        try:
            self._write_resource_sample()
        except Exception:
            pass
        self._resource_thread = None

    def _write_system_info(self) -> None:
        if not self.enabled:
            return
        payload = {
            "schema_version": 1,
            "created_at": utc_now_iso(),
            "performance_implementation_version": PERFORMANCE_IMPLEMENTATION_VERSION,
            "python": sys.version,
            "platform": platform.platform(),
            "processor": platform.processor(),
            "logical_cpu_count": os.cpu_count(),
            "packages": {
                "duckdb": getattr(duckdb, "__version__", None),
                "numpy": getattr(np, "__version__", None),
                "pyarrow": getattr(pa, "__version__", None),
                "pillow": getattr(Image, "__version__", None),
                "rasterio": getattr(rasterio, "__version__", None) if rasterio else None,
                "numba_available": njit is not None,
                "scipy_available": scipy_ndimage_convolve is not None,
                "psutil_available": psutil is not None,
            },
        }
        if psutil is not None:
            try:
                vm = psutil.virtual_memory()
                payload["system_memory_bytes"] = int(vm.total)
            except Exception:
                pass
        atomic_write_json(self.system_path, _json_safe(payload))

    def _write(self) -> None:
        if not self.enabled:
            return
        with self._lock:
            total_stage_seconds = sum(
                float(event.get("duration_seconds", 0.0))
                for event in self.events
                if event.get("kind", "stage") == "stage"
            )
            payload = {
                "schema_version": 2,
                "started_at": self.started_at,
                "updated_at": utc_now_iso(),
                "performance_implementation_version": PERFORMANCE_IMPLEMENTATION_VERSION,
                "total_recorded_stage_seconds": total_stage_seconds,
                "event_count": len(self.events),
                "events_jsonl": str(self.events_path),
                "summary_csv": str(self.csv_path),
                "system_info": str(self.system_path),
                "resource_samples_csv": str(self.resources_path),
                "events": self.events,
            }
            atomic_write_json(self.path, _json_safe(payload))
            self._write_csv()

    def _write_csv(self) -> None:
        fieldnames = [
            "kind", "name", "parent", "status", "started_at", "ended_at",
            "duration_seconds", "process_cpu_seconds", "cpu_percent_of_one_core",
            "rss_start_bytes", "rss_end_bytes", "rss_delta_bytes",
            "process_read_bytes", "process_write_bytes", "items", "item_unit",
            "items_per_second", "bytes", "bytes_per_second", "details_json",
        ]
        temp_path = self.csv_path.parent / (
            f".tmp_{os.getpid()}_{uuid.uuid4().hex[:12]}.csv"
        )
        with temp_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for event in self.events:
                row = {key: event.get(key) for key in fieldnames}
                row["details_json"] = json.dumps(
                    _json_safe(event.get("details", {})),
                    sort_keys=True,
                    separators=(",", ":"),
                )
                writer.writerow(row)
        os.replace(temp_path, self.csv_path)

    def _append_jsonl(self, event: dict[str, Any]) -> None:
        with self.events_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(_json_safe(event), sort_keys=True) + "\n")

    @contextmanager
    def _timed(self, kind: str, name: str, **details: Any):
        if not self.enabled:
            metrics: dict[str, Any] = {}
            yield metrics
            return

        started_at = utc_now_iso()
        before = self._snapshot()
        with self._lock:
            parent = self._stack[-1][0] if self._stack else None
            self._stack.append((name, float(before["perf_counter"])))
        metrics: dict[str, Any] = {}

        label = "stage" if kind == "stage" else "operation"
        print(f"\n[{label} start] {name}")
        if details:
            print("  " + ", ".join(f"{key}={value}" for key, value in details.items()))

        status = "complete"
        error: str | None = None
        try:
            yield metrics
        except BaseException as exc:
            status = "failed"
            error = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            after = self._snapshot()
            with self._lock:
                if self._stack and self._stack[-1][0] == name:
                    self._stack.pop()
            duration = max(0.0, float(after["perf_counter"] - before["perf_counter"]))
            cpu_seconds = max(
                0.0,
                float(after["process_cpu_seconds"] - before["process_cpu_seconds"]),
            )
            merged_details = dict(details)
            merged_details.update(metrics)

            event: dict[str, Any] = {
                "kind": kind,
                "name": name,
                "parent": parent,
                "status": status,
                "started_at": started_at,
                "ended_at": utc_now_iso(),
                "duration_seconds": duration,
                "process_cpu_seconds": cpu_seconds,
                "cpu_percent_of_one_core": (
                    100.0 * cpu_seconds / duration if duration > 0 else None
                ),
                "details": _json_safe(merged_details),
            }

            for key in ("rss_bytes", "vms_bytes"):
                if key in before:
                    event[f"{key[:-6]}_start_bytes"] = int(before[key])
                if key in after:
                    event[f"{key[:-6]}_end_bytes"] = int(after[key])
                if key in before and key in after:
                    event[f"{key[:-6]}_delta_bytes"] = int(after[key] - before[key])

            if "read_bytes" in before and "read_bytes" in after:
                event["process_read_bytes"] = max(
                    0, int(after["read_bytes"] - before["read_bytes"])
                )
            if "write_bytes" in before and "write_bytes" in after:
                event["process_write_bytes"] = max(
                    0, int(after["write_bytes"] - before["write_bytes"])
                )

            items = merged_details.get("items")
            if items is not None:
                event["items"] = float(items)
                event["item_unit"] = str(merged_details.get("item_unit", "items"))
                event["items_per_second"] = (
                    float(items) / duration if duration > 0 else None
                )
            byte_count = merged_details.get("bytes")
            if byte_count is not None:
                event["bytes"] = int(byte_count)
                event["bytes_per_second"] = (
                    float(byte_count) / duration if duration > 0 else None
                )
            if error is not None:
                event["error"] = error

            with self._lock:
                self.events.append(event)
                self._append_jsonl(event)
                self._write()

            suffix: list[str] = [format_duration(duration)]
            if event.get("items_per_second") is not None:
                suffix.append(
                    f"{event['items_per_second']:,.0f} {event.get('item_unit', 'items')}/s"
                )
            if event.get("bytes_per_second") is not None:
                suffix.append(f"{format_bytes(event['bytes_per_second'])}/s")
            if event.get("rss_end_bytes") is not None:
                suffix.append(f"RSS {format_bytes(event['rss_end_bytes'])}")
            print(f"[{label} {status}] {name}: " + " | ".join(suffix))

    def stage(self, name: str, **details: Any):
        return self._timed("stage", name, **details)

    def operation(self, name: str, **details: Any):
        return self._timed("operation", name, **details)

    def record_metric(self, name: str, **details: Any) -> None:
        if not self.enabled:
            return
        with self._lock:
            parent = self._stack[-1][0] if self._stack else None
        event = {
            "kind": "metric",
            "name": name,
            "parent": parent,
            "status": "recorded",
            "started_at": utc_now_iso(),
            "ended_at": utc_now_iso(),
            "duration_seconds": 0.0,
            "details": _json_safe(details),
        }
        with self._lock:
            self.events.append(event)
            self._append_jsonl(event)
            self._write()

    def print_summary(self, limit: int = 20) -> None:
        if not self.enabled or not self.events:
            return
        timed_events = [
            event for event in self.events
            if event.get("kind") in {"stage", "operation"}
            and float(event.get("duration_seconds", 0.0)) > 0
        ]
        if not timed_events:
            return
        print("\nPerformance summary (longest individual events):")
        for event in sorted(
            timed_events,
            key=lambda item: float(item.get("duration_seconds", 0.0)),
            reverse=True,
        )[: max(1, int(limit))]:
            rate = ""
            if event.get("items_per_second") is not None:
                rate = (
                    f", {float(event['items_per_second']):,.0f} "
                    f"{event.get('item_unit', 'items')}/s"
                )
            print(
                f"  {event.get('kind', 'event'):9s} {event.get('name')}: "
                f"{format_duration(float(event.get('duration_seconds', 0.0)))}{rate}"
            )
        print(f"Detailed JSON: {self.path}")
        print(f"Event log: {self.events_path}")
        print(f"CSV summary: {self.csv_path}")
        print(f"Resource samples: {self.resources_path}")



def format_duration(seconds: float | None) -> str:
    """Return a compact human-readable duration for console ETA messages."""
    if seconds is None or not math.isfinite(float(seconds)) or float(seconds) < 0:
        return "unknown"

    seconds = int(round(float(seconds)))
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    if hours > 0:
        return f"{hours:d}h {minutes:02d}m {secs:02d}s"
    if minutes > 0:
        return f"{minutes:d}m {secs:02d}s"
    return f"{secs:d}s"

def get_profiler(args: argparse.Namespace | None) -> StageProfiler | None:
    if args is None:
        return None
    profiler = getattr(args, "_profiler", None)
    return profiler if isinstance(profiler, StageProfiler) else None

@contextmanager
def null_profile_context():
    metrics: dict[str, Any] = {}
    yield metrics



def file_size_bytes(path: Path) -> int | None:
    try:
        return int(path.stat().st_size)
    except OSError:
        return None


def parquet_file_row_count(path: Path) -> int:
    """Read a Parquet row count from metadata without scanning column data."""
    try:
        metadata = pq.ParquetFile(path).metadata
        return int(metadata.num_rows if metadata is not None else 0)
    except Exception:
        return 0


def parquet_files_row_count(paths: Iterable[Path]) -> int:
    return sum(parquet_file_row_count(path) for path in paths)


def coordinate_sql_type_for_zoom(zoom: int) -> str:
    world_px = TILE_SIZE * (1 << int(zoom))
    return "INTEGER" if world_px - 1 <= np.iinfo(np.int32).max else "BIGINT"


def coordinate_numpy_dtype_for_world(world_px: int):
    return np.int32 if int(world_px) - 1 <= np.iinfo(np.int32).max else np.int64


def coordinate_arrow_type_for_world(world_px: int):
    return pa.int32() if int(world_px) - 1 <= np.iinfo(np.int32).max else pa.int64()


def write_pixel_part_parquet(
    table: pa.Table,
    output_path: Path,
    args: argparse.Namespace,
) -> None:
    codec = str(getattr(args, "pixel_part_parquet_compression", "zstd")).lower()
    level = getattr(args, "pixel_part_parquet_compression_level", 1)
    kwargs: dict[str, Any] = {
        "compression": codec,
        "use_dictionary": False,
        "write_statistics": False,
    }
    if level is not None and codec in {"zstd", "gzip", "brotli"}:
        kwargs["compression_level"] = int(level)
    try:
        pq.write_table(table, output_path, **kwargs)
    except (TypeError, ValueError) as exc:
        # Older PyArrow releases may support the codec but not an explicit
        # compression_level keyword. Retry only for that compatibility case.
        if "compression_level" not in kwargs or "compression" not in str(exc).lower():
            raise
        output_path.unlink(missing_ok=True)
        kwargs.pop("compression_level", None)
        print(
            "Warning: PyArrow rejected the requested Parquet compression level "
            f"({exc}); retrying with the codec default."
        )
        pq.write_table(table, output_path, **kwargs)


def hardlink_or_copy(source_path: Path, destination_path: Path) -> str:
    """Create a cheap hard link when possible, otherwise copy the file."""
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    destination_path.unlink(missing_ok=True)
    try:
        os.link(source_path, destination_path)
        return "hardlink"
    except OSError:
        shutil.copy2(source_path, destination_path)
        return "copy"


def duckdb_copy_query_to_parquet(
    con: duckdb.DuckDBPyConnection,
    query_sql: str,
    output_path: Path,
    args: argparse.Namespace | None,
) -> str:
    """Write a query result with fast ZSTD and an older-version fallback."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.unlink(missing_ok=True)
    level = max(1, min(22, int(getattr(
        args, "duckdb_parquet_compression_level", 1
    ))))
    query_sql = query_sql.strip().rstrip(";")
    fast_sql = f"""
    COPY ({query_sql})
    TO {sql_str(output_path.as_posix())}
    (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL {level});
    """
    try:
        con.execute(fast_sql)
        return f"zstd_level_{level}"
    except Exception as exc:
        output_path.unlink(missing_ok=True)
        message = str(exc).lower()
        compatibility_error = (
            "compression_level" in message
            or "unrecognized option" in message
            or "unknown option" in message
        )
        if not compatibility_error:
            raise
        print(
            "Warning: DuckDB rejected the requested Parquet compression level "
            f"({exc}); retrying with its default ZSTD level."
        )
        con.execute(
            f"""
            COPY ({query_sql})
            TO {sql_str(output_path.as_posix())}
            (FORMAT PARQUET, COMPRESSION ZSTD);
            """
        )
        return "zstd_default_fallback"


def geotiff_creation_options(
    args: argparse.Namespace,
    *,
    block_size: int | None = None,
) -> dict[str, Any]:
    """Return lossless GTiff creation options using benchmarked defaults."""
    compression = str(getattr(args, "geotiff_compression", "zstd")).lower()
    level = int(getattr(args, "geotiff_compression_level", 1))
    effective_block_size = validate_geotiff_block_size(
        block_size if block_size is not None
        else getattr(args, "geotiff_block_size", 512)
    )
    options: dict[str, Any] = {
        "compress": compression,
        "tiled": True,
        "blockxsize": effective_block_size,
        "blockysize": effective_block_size,
        "bigtiff": "IF_SAFER",
        "num_threads": str(getattr(args, "geotiff_num_threads", "1")),
    }
    predictor = normalise_geotiff_predictor(
        getattr(args, "geotiff_predictor", None)
    )
    if predictor is not None:
        options["predictor"] = predictor
    if compression == "deflate":
        options["zlevel"] = max(1, min(9, level))
    elif compression == "zstd":
        options["zstd_level"] = max(1, min(22, level))
    elif compression == "lzma":
        options["lzma_preset"] = max(0, min(9, level))
    return options


if njit is not None:
    @njit(cache=True, parallel=True)
    def _sample_segments_numba(
        x1: np.ndarray,
        y1: np.ndarray,
        x2: np.ndarray,
        y2: np.ndarray,
        values: np.ndarray,
        world_px: int,
        sample_step_px: float,
        max_segment_samples: int,
    ) -> tuple[np.ndarray, np.ndarray]:
        n = x1.shape[0]
        dxs = np.empty(n, dtype=np.float64)
        dys = np.empty(n, dtype=np.float64)
        weights = np.empty(n, dtype=np.float64)
        steps_arr = np.zeros(n, dtype=np.int32)
        counts = np.zeros(n, dtype=np.int64)
        world_px_float = float(world_px)
        minimum_step = max(float(sample_step_px), 0.1)

        for j in prange(n):
            a = x1[j]
            b = y1[j]
            c = x2[j]
            d = y2[j]
            value = values[j]
            if not math.isfinite(a + b + c + d + value) or value <= 0.0:
                continue

            dx = c - a
            if dx > world_px_float / 2.0:
                c -= world_px_float
            elif dx < -world_px_float / 2.0:
                c += world_px_float

            dx = c - a
            dy = d - b
            pixel_length = max(abs(dx), abs(dy))
            steps = max(1, int(math.ceil(pixel_length / minimum_step)))
            steps = min(steps, int(max_segment_samples))
            dxs[j] = dx
            dys[j] = dy
            steps_arr[j] = steps
            counts[j] = steps + 1
            weights[j] = value / float(steps + 1)

        offsets = np.empty(n + 1, dtype=np.int64)
        offsets[0] = 0
        for j in range(n):
            offsets[j + 1] = offsets[j] + counts[j]

        total = int(offsets[n])
        keys = np.empty(total, dtype=np.int64)
        sampled_values = np.empty(total, dtype=np.float64)

        for j in prange(n):
            steps = int(steps_arr[j])
            if steps <= 0:
                continue
            start = int(offsets[j])
            dx = dxs[j]
            dy = dys[j]
            weight = weights[j]
            for i in range(steps + 1):
                t = float(i) / float(steps)
                gx = int(math.floor((x1[j] + dx * t) % world_px_float))
                gy = int(math.floor(y1[j] + dy * t))
                position = start + i
                if 0 <= gy < world_px:
                    keys[position] = gy * world_px + gx
                    sampled_values[position] = weight
                else:
                    keys[position] = -1
                    sampled_values[position] = 0.0

        return keys, sampled_values
else:  # pragma: no cover - selected only when Numba is unavailable
    _sample_segments_numba = None


def sample_segments_python_arrays(
    x1: np.ndarray,
    y1: np.ndarray,
    x2: np.ndarray,
    y2: np.ndarray,
    values: np.ndarray,
    world_px: int,
    sample_step_px: float,
    max_segment_samples: int,
) -> tuple[np.ndarray, np.ndarray]:
    key_buffer: list[int] = []
    value_buffer: list[float] = []
    for a, b, c, d, value in zip(x1, y1, x2, y2, values):
        add_segment_samples_to_buffers(
            key_buffer=key_buffer,
            value_buffer=value_buffer,
            x1=float(a),
            y1=float(b),
            x2=float(c),
            y2=float(d),
            value=float(value),
            world_px=int(world_px),
            sample_step_px=float(sample_step_px),
            max_segment_samples=int(max_segment_samples),
        )
    return (
        np.asarray(key_buffer, dtype=np.int64),
        np.asarray(value_buffer, dtype=np.float64),
    )


def estimate_segment_sample_counts(
    x1: np.ndarray,
    y1: np.ndarray,
    x2: np.ndarray,
    y2: np.ndarray,
    world_px: int,
    sample_step_px: float,
    max_segment_samples: int,
) -> np.ndarray:
    """Estimate each segment's output size to bound native-kernel memory."""
    world_px_float = float(world_px)
    dx = np.asarray(x2 - x1, dtype=np.float64)
    dx = np.where(
        dx > world_px_float / 2.0,
        dx - world_px_float,
        np.where(dx < -world_px_float / 2.0, dx + world_px_float, dx),
    )
    dy = np.asarray(y2 - y1, dtype=np.float64)
    pixel_length = np.maximum(np.abs(dx), np.abs(dy))
    steps = np.ceil(
        pixel_length / max(float(sample_step_px), 0.1)
    ).astype(np.int64, copy=False)
    np.clip(steps, 1, int(max_segment_samples), out=steps)
    return steps + 1


def iter_sample_budget_slices(
    sample_counts: np.ndarray,
    max_output_samples: int,
) -> Iterator[slice]:
    """Yield contiguous segment slices whose estimated samples fit a budget."""
    if sample_counts.size == 0:
        return
    budget = max(1, int(max_output_samples))
    cumulative = np.cumsum(sample_counts, dtype=np.int64)
    start = 0
    while start < sample_counts.size:
        preceding = int(cumulative[start - 1]) if start > 0 else 0
        target = preceding + budget
        end = int(np.searchsorted(cumulative, target, side="right"))
        if end <= start:
            end = start + 1
        yield slice(start, min(end, int(sample_counts.size)))
        start = end


def compact_pixel_samples(
    keys: np.ndarray,
    values: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """Sort integer keys once, then sum adjacent duplicates with ``reduceat``."""
    if keys.size == 0:
        return np.empty(0, dtype=np.int64), np.empty(0, dtype=np.float64)
    order = np.argsort(keys, kind="quicksort")
    sorted_keys = keys[order]
    sorted_values = values[order]
    starts_mask = np.empty(sorted_keys.size, dtype=bool)
    starts_mask[0] = True
    starts_mask[1:] = sorted_keys[1:] != sorted_keys[:-1]
    starts = np.flatnonzero(starts_mask)
    unique_keys = sorted_keys[starts]
    summed_values = np.add.reduceat(sorted_values, starts)
    positive = np.isfinite(summed_values) & (summed_values > 0)
    return unique_keys[positive], summed_values[positive]


def flush_sample_chunks_to_parquet(
    key_chunks: list[np.ndarray],
    value_chunks: list[np.ndarray],
    output_dir: Path,
    part_number: int,
    world_px: int,
    args: argparse.Namespace,
) -> tuple[int, dict[str, Any]]:
    if not key_chunks:
        return part_number, {
            "input_samples": 0,
            "output_pixels": 0,
            "concat_seconds": 0.0,
            "compact_seconds": 0.0,
            "parquet_write_seconds": 0.0,
            "bytes_written": 0,
        }

    started = time.perf_counter()
    keys = key_chunks[0] if len(key_chunks) == 1 else np.concatenate(key_chunks)
    values = value_chunks[0] if len(value_chunks) == 1 else np.concatenate(value_chunks)
    key_chunks.clear()
    value_chunks.clear()
    concat_seconds = time.perf_counter() - started
    input_samples = int(keys.size)

    started = time.perf_counter()
    unique_keys, summed_values = compact_pixel_samples(keys, values)
    compact_seconds = time.perf_counter() - started
    del keys, values

    if unique_keys.size == 0:
        return part_number, {
            "input_samples": input_samples,
            "output_pixels": 0,
            "concat_seconds": concat_seconds,
            "compact_seconds": compact_seconds,
            "parquet_write_seconds": 0.0,
            "bytes_written": 0,
        }

    gy64 = unique_keys // int(world_px)
    gx64 = unique_keys - gy64 * int(world_px)
    coord_dtype = coordinate_numpy_dtype_for_world(world_px)
    coord_arrow_type = coordinate_arrow_type_for_world(world_px)
    gx = gx64.astype(coord_dtype, copy=False)
    gy = gy64.astype(coord_dtype, copy=False)

    table = pa.table(
        {
            "gx": pa.array(gx, type=coord_arrow_type),
            "gy": pa.array(gy, type=coord_arrow_type),
            "value": pa.array(summed_values, type=pa.float64()),
        }
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"pixels_part_{part_number:06d}.parquet"
    started = time.perf_counter()
    write_pixel_part_parquet(table, output_path, args)
    parquet_write_seconds = time.perf_counter() - started
    bytes_written = file_size_bytes(output_path) or 0
    return part_number + 1, {
        "input_samples": input_samples,
        "output_pixels": int(unique_keys.size),
        "concat_seconds": concat_seconds,
        "compact_seconds": compact_seconds,
        "parquet_write_seconds": parquet_write_seconds,
        "bytes_written": bytes_written,
    }



def estimate_segment_rows_from_metadata(
    segment_path: Path,
    config_hash: StageFingerprint,
) -> int | None:
    """
    Estimate the number of segment rows for ETA purposes.

    The preferred source is the segment completion marker. If unavailable, fall
    back to summing Parquet file metadata row counts, which is usually much
    cheaper than scanning the actual row data.
    """
    marker = read_valid_completion_marker(
        segment_path / STAGE_MARKER_FILENAME,
        config_hash,
        marker_type="segments_complete",
    )
    if marker is not None:
        try:
            total = int(marker.get("total_segments", 0))
            if total > 0:
                return total
        except Exception:
            pass

    total_rows = 0
    files = sorted(segment_path.glob("segments_shard_*.parquet"))
    if not files:
        return None

    try:
        for path in files:
            total_rows += int(pq.ParquetFile(path).metadata.num_rows)
    except Exception:
        return None

    return total_rows if total_rows > 0 else None


class ProgressEtaReporter:
    """Console and JSON ETA reporter for long, streaming stages."""

    def __init__(
        self,
        *,
        path: Path | None,
        stage: str,
        zoom: int,
        total_segments: int | None,
        update_seconds: float,
        enabled: bool,
    ):
        self.path = path
        self.stage = stage
        self.zoom = int(zoom)
        self.total_segments = int(total_segments) if total_segments else None
        self.update_seconds = max(1.0, float(update_seconds or 15.0))
        self.enabled = bool(enabled and path is not None)
        self.started_perf = time.perf_counter()
        self.started_at = utc_now_iso()
        self.last_write_perf = 0.0

    def snapshot(
        self,
        *,
        segments_seen: int,
        batches_seen: int,
        samples_buffered: int,
        part_number: int,
        final: bool = False,
    ) -> dict[str, Any]:
        elapsed = max(0.0, time.perf_counter() - self.started_perf)
        rate_segments_per_second = (
            float(segments_seen) / elapsed if elapsed > 0 and segments_seen > 0 else None
        )

        percent_complete = None
        estimated_remaining_seconds = None
        estimated_total_seconds = None
        segments_remaining = None

        if self.total_segments and self.total_segments > 0:
            percent_complete = min(100.0, 100.0 * float(segments_seen) / float(self.total_segments))
            segments_remaining = max(0, int(self.total_segments) - int(segments_seen))
            if rate_segments_per_second and rate_segments_per_second > 0:
                estimated_remaining_seconds = float(segments_remaining) / rate_segments_per_second
                estimated_total_seconds = elapsed + estimated_remaining_seconds

        return {
            "stage": self.stage,
            "zoom": self.zoom,
            "status": "complete" if final else "running",
            "started_at": self.started_at,
            "updated_at": utc_now_iso(),
            "segments_seen": int(segments_seen),
            "total_segments_estimated": self.total_segments,
            "segments_remaining_estimated": segments_remaining,
            "percent_complete": percent_complete,
            "batches_seen": int(batches_seen),
            "samples_buffered": int(samples_buffered),
            "pixel_part_files_written_so_far": int(part_number),
            "elapsed_seconds": elapsed,
            "elapsed_hms": format_duration(elapsed),
            "rate_segments_per_second": rate_segments_per_second,
            "estimated_remaining_seconds": estimated_remaining_seconds,
            "estimated_remaining_hms": format_duration(estimated_remaining_seconds),
            "estimated_total_seconds": estimated_total_seconds,
            "estimated_total_hms": format_duration(estimated_total_seconds),
            "note": (
                "ETA is based on segment-processing throughput. It is an estimate; "
                "later stages such as pixel aggregation, rendering, MBTiles writing "
                "and GeoTIFF export have their own runtimes."
            ),
        }

    def maybe_write(
        self,
        *,
        segments_seen: int,
        batches_seen: int,
        samples_buffered: int,
        part_number: int,
        final: bool = False,
    ) -> dict[str, Any]:
        payload = self.snapshot(
            segments_seen=segments_seen,
            batches_seen=batches_seen,
            samples_buffered=samples_buffered,
            part_number=part_number,
            final=final,
        )

        now = time.perf_counter()
        should_write = final or (now - self.last_write_perf >= self.update_seconds)
        if self.enabled and should_write:
            atomic_write_json(self.path, payload)
            self.last_write_perf = now
        return payload

    @staticmethod
    def postfix_from_payload(payload: dict[str, Any]) -> str:
        pct = payload.get("percent_complete")
        rate = payload.get("rate_segments_per_second")
        eta = payload.get("estimated_remaining_hms")

        pieces: list[str] = []
        if pct is not None:
            pieces.append(f"{float(pct):5.1f}%")
        if rate is not None:
            pieces.append(f"{float(rate):,.0f} seg/s")
        if eta:
            pieces.append(f"ETA {eta}")
        return " | ".join(pieces)


def _stage_hashes(configs: PipelineConfigs) -> dict[str, str]:
    return {
        stage: stable_json_hash(config)
        for stage, config in configs.as_dict().items()
    }


def create_new_run_state(
    state_dir: Path,
    output_name: str,
    configs: PipelineConfigs,
) -> RunStateManager:
    now = utc_now_iso()
    manifest = {
        "schema_version": RUN_STATE_SCHEMA_VERSION,
        "run_id": (
            f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_"
            f"{uuid.uuid4().hex[:10]}"
        ),
        "output_name": output_name,
        "configs": configs.as_dict(),
        "hashes": _stage_hashes(configs),
        "hash_aliases": {
            stage: [] for stage in ("segment", "analytical", "render", "export")
        },
        "created_at": now,
        "updated_at": now,
        "complete": False,
        "last_error": None,
        "zooms": {},
        "duckdb": {},
    }
    manager = RunStateManager(state_dir, manifest)
    manager.save()
    return manager


def _merge_aliases(*groups: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(str(value) for group in groups for value in group))


def _stage_configs_compatible(stage: str, old: dict[str, Any], new: dict[str, Any]) -> bool:
    if stage == "segment":
        # Compact and diagnostic segment files both contain the five columns needed
        # by rasterisation. A schema change alone must not invalidate a usable shard.
        keys = (*LEGACY_TRACK_VALUE_KEYS, "ship_id_shards")
        return _legacy_keys_match(old, new, keys)
    return old == new


def load_run_state(
    state_dir: Path,
    current_configs: PipelineConfigs,
    allow_setting_mismatch: bool,
) -> RunStateManager:
    manifest_path = state_dir / "run_state.json"
    manifest = read_json(manifest_path)
    schema_version = int(manifest.get("schema_version", 1))
    current_dict = current_configs.as_dict()
    current_hashes = _stage_hashes(current_configs)

    if schema_version == 1:
        saved_config = dict(manifest.get("config", {}))
        legacy_hash = str(manifest.get("config_hash", ""))
        analytical_match = _legacy_keys_match(
            saved_config, current_configs.analytical, LEGACY_ANALYTICAL_KEYS
        )
        if not analytical_match and not allow_setting_mismatch:
            relevant_saved = {
                key: saved_config.get(key) for key in LEGACY_ANALYTICAL_KEYS
            }
            relevant_current = {
                key: current_configs.analytical.get(key)
                for key in LEGACY_ANALYTICAL_KEYS
            }
            differences = config_differences(relevant_saved, relevant_current)
            raise RuntimeError(
                "The analytical settings do not match the existing run:\n  - "
                + "\n  - ".join(differences)
                + "\nRendering and export settings may now change independently, but "
                "analytical changes require start-again mode or explicit "
                "allow_resume_setting_mismatch (not recommended)."
            )
        if not analytical_match:
            print(
                "WARNING: analytical settings changed while mismatch override is enabled; "
                "existing analytical checkpoints will not be trusted."
            )
            aliases = {
                stage: [] for stage in ("segment", "analytical", "render", "export")
            }
        else:
            aliases = legacy_stage_aliases(
                saved_config, legacy_hash, current_configs
            )

        manifest["legacy_run_state_v1"] = {
            "config": saved_config,
            "config_hash": legacy_hash,
            "migrated_at": utc_now_iso(),
        }
        manifest["schema_version"] = RUN_STATE_SCHEMA_VERSION
        manifest["configs"] = current_dict
        manifest["hashes"] = current_hashes
        manifest["hash_aliases"] = aliases
        # Retain the original keys as provenance only; new code uses stage configs.
        manifest.pop("config", None)
        manifest.pop("config_hash", None)
        manifest["complete"] = False
        print("Migrated run_state.json to stage-specific configuration fingerprints.")

    elif schema_version == RUN_STATE_SCHEMA_VERSION:
        saved_configs = {
            stage: dict(manifest.get("configs", {}).get(stage, {}))
            for stage in ("segment", "analytical", "render", "export")
        }
        saved_hashes = {
            stage: str(manifest.get("hashes", {}).get(stage, ""))
            for stage in ("segment", "analytical", "render", "export")
        }
        aliases = {
            stage: list(manifest.get("hash_aliases", {}).get(stage, []))
            for stage in ("segment", "analytical", "render", "export")
        }

        if saved_configs["analytical"] != current_configs.analytical:
            differences = config_differences(
                saved_configs["analytical"], current_configs.analytical
            )
            message = (
                "The analytical settings do not match the existing run:\n  - "
                + "\n  - ".join(differences)
            )
            if not allow_setting_mismatch:
                raise RuntimeError(
                    message
                    + "\nRendering and export settings may change independently. "
                    "Analytical changes require start-again mode or explicit "
                    "allow_resume_setting_mismatch (not recommended)."
                )
            print("WARNING: " + message.replace("\n", "\nWARNING: "))
            aliases["analytical"] = []
            aliases["segment"] = []
        else:
            aliases["analytical"] = _merge_aliases(
                aliases["analytical"], [saved_hashes["analytical"]]
            )

        if _stage_configs_compatible(
            "segment", saved_configs["segment"], current_configs.segment
        ):
            aliases["segment"] = _merge_aliases(
                aliases["segment"], [saved_hashes["segment"]]
            )
        else:
            aliases["segment"] = []

        for stage in ("render", "export"):
            if saved_configs[stage] == current_dict[stage]:
                aliases[stage] = _merge_aliases(
                    aliases[stage], [saved_hashes[stage]]
                )
            else:
                # These stages may change without invalidating analytical work.
                aliases[stage] = []
                manifest["complete"] = False
                print(
                    f"{stage.capitalize()} settings changed; only that output stage "
                    "will be regenerated where required."
                )

        manifest["configs"] = current_dict
        manifest["hashes"] = current_hashes
        manifest["hash_aliases"] = aliases
    else:
        raise RuntimeError(
            f"Unsupported run-state schema in {manifest_path}: {schema_version!r}"
        )

    manager = RunStateManager(state_dir, manifest)
    manager.run_complete_marker.unlink(missing_ok=True)
    manager.save()
    return manager


def has_any_named_output(
    work_dir: Path,
    state_dir: Path,
    xyz_root: Path | None,
    mbtiles_path: Path,
    output_dir: Path,
    output_name: str,
) -> bool:
    # Check both compact and legacy names so existing runs remain resumable and
    # start-again mode can find every file belonging to the named output.
    paths = [
        work_dir,
        state_dir,
        compact_work_dir(output_dir, output_name),
        output_dir / f"_work_{output_name}",
        compact_state_dir(output_dir, output_name),
        output_dir / f"{output_name}_run_state",
        mbtiles_path,
        *all_analytical_roots(output_dir, output_name),
    ]
    if xyz_root is not None:
        paths.append(xyz_root)
    return any(path.exists() for path in paths)


def named_output_paths(
    work_dir: Path,
    state_dir: Path,
    xyz_root: Path | None,
    mbtiles_path: Path,
    output_dir: Path,
    output_name: str,
) -> list[Path]:
    paths = [
        work_dir,
        state_dir,
        compact_work_dir(output_dir, output_name),
        output_dir / f"_work_{output_name}",
        compact_state_dir(output_dir, output_name),
        output_dir / f"{output_name}_run_state",
        mbtiles_path,
        *all_analytical_roots(output_dir, output_name),
    ]
    if xyz_root is not None:
        paths.append(xyz_root)
    # Deduplicate paths because the resolved work/state path may already be in the
    # candidate set. Preserve order for predictable deletion/logging.
    return list(dict.fromkeys(paths))



@dataclass(frozen=True)
class PipelinePaths:
    output_dir: Path
    output_name: str
    work_dir: Path
    state_dir: Path
    xyz_named_path: Path
    xyz_root: Path | None
    mbtiles_path: Path
    tile_archive_path: Path
    lock_path: Path


def infer_dataset_label(ais_folder: str | Path) -> str:
    """Derive a stable dataset label from the AIS input directory name."""
    path = Path(ais_folder)
    label = safe_slug(path.name)
    return label if label not in {"", "data", "dataset", "parquet"} else safe_slug(path.parent.name)


def generated_output_name(
    args: argparse.Namespace,
    group_slug: str,
    dataset_label: str,
) -> str:
    return (
        f"{safe_slug(dataset_label)}_{group_slug}_{args.metric}_"
        f"gt_{safe_slug(str(args.min_speed_knots))}kt_"
        f"z{args.min_zoom}_{args.max_zoom}"
    )


def output_name_has_artifacts(output_dir: Path, output_name: str) -> bool:
    work_dir = resolve_work_dir(output_dir, output_name)
    state_dir = resolve_state_dir(output_dir, output_name)
    xyz_path = output_dir / f"{output_name}_xyz"
    mbtiles_path = output_dir / f"{output_name}.mbtiles"
    return has_any_named_output(
        work_dir=work_dir,
        state_dir=state_dir,
        xyz_root=xyz_path,
        mbtiles_path=mbtiles_path,
        output_dir=output_dir,
        output_name=output_name,
    )


def resolve_output_name(
    args: argparse.Namespace,
    output_dir: Path,
    group_slug: str,
) -> str:
    if args.output_name:
        return str(args.output_name)

    dataset_label = str(
        getattr(args, "dataset_label", None) or infer_dataset_label(args.ais_folder)
    )
    current_name = generated_output_name(args, group_slug, dataset_label)
    # Earlier versions hard-coded ais_2025 even when the input folder contained
    # another year. Prefer that old name only when it already owns resumable files.
    legacy_name = generated_output_name(args, group_slug, "ais_2025")
    if bool(getattr(args, "resume_existing", True)):
        if output_name_has_artifacts(output_dir, current_name):
            return current_name
        if legacy_name != current_name and output_name_has_artifacts(
            output_dir, legacy_name
        ):
            print(
                "Existing output uses the historical hard-coded ais_2025 name; "
                f"resuming it as {legacy_name!r}. Set output_name explicitly for "
                "future runs to avoid ambiguity."
            )
            return legacy_name
    return current_name


def build_pipeline_paths(
    args: argparse.Namespace,
    output_dir: Path,
    output_name: str,
) -> PipelinePaths:
    xyz_named_path = output_dir / f"{output_name}_xyz"
    work_dir = resolve_work_dir(output_dir, output_name)
    state_dir = resolve_state_dir(output_dir, output_name)
    mbtiles_path = output_dir / f"{output_name}.mbtiles"
    tile_archive_path = (
        mbtiles_path
        if bool(getattr(args, "write_mbtiles", False))
        else state_dir / "visual_tile_archive.mbtiles"
    )
    return PipelinePaths(
        output_dir=output_dir,
        output_name=output_name,
        work_dir=work_dir,
        state_dir=state_dir,
        xyz_named_path=xyz_named_path,
        xyz_root=xyz_named_path if args.write_xyz else None,
        mbtiles_path=mbtiles_path,
        tile_archive_path=tile_archive_path,
        lock_path=resolve_lock_path(output_dir, output_name),
    )

def count_xyz_tiles_for_zoom(xyz_root: Path | None, zoom: int) -> int:
    if xyz_root is None:
        return 0
    zoom_dir = xyz_root / str(int(zoom))
    if not zoom_dir.exists():
        return 0
    return sum(1 for _ in zoom_dir.rglob("*.png"))


def sqlite_zoom_tile_count(path: Path, zoom: int) -> int:
    if not path.exists():
        return 0
    con = sqlite3.connect(path.as_posix())
    try:
        row = con.execute(
            "SELECT COUNT(*) FROM tiles WHERE zoom_level = ?", [int(zoom)]
        ).fetchone()
        return int(row[0] if row else 0)
    except sqlite3.Error:
        return 0
    finally:
        con.close()


def visual_outputs_requested(args: argparse.Namespace) -> bool:
    return bool(getattr(args, "write_xyz", False) or getattr(args, "write_mbtiles", False))


def analytical_exports_requested(args: argparse.Namespace, zoom: int) -> bool:
    return bool(
        should_export_value_zoom(args, zoom)
        and (
            getattr(args, "write_value_pixels", False)
            or getattr(args, "write_value_tiffs", False)
            or getattr(args, "write_value_composite_geotiff", False)
        )
    )


def mbtiles_visual_output_exists(
    mbtiles_path: Path,
    zoom: int,
    expected_tiles: int,
    mbtiles_conn: sqlite3.Connection | None = None,
) -> bool:
    """Validate the canonical rendered tile archive for one zoom.

    Use an already-open connection when available. The production MBTiles
    connection uses SQLite exclusive locking for fast bulk writes, so opening a
    second validation connection while rendering may otherwise report a false
    zero count because the database is locked.
    """
    expected_tiles = max(0, int(expected_tiles))
    if expected_tiles == 0:
        return True
    if mbtiles_conn is not None:
        try:
            row = mbtiles_conn.execute(
                "SELECT COUNT(*) FROM tiles WHERE zoom_level = ?",
                [int(zoom)],
            ).fetchone()
            return int(row[0] if row else 0) == expected_tiles
        except sqlite3.Error:
            return False
    return sqlite_zoom_tile_count(mbtiles_path, zoom) == expected_tiles


def xyz_visual_output_exists(
    xyz_root: Path | None,
    zoom: int,
    expected_tiles: int,
    render_hash: str | None = None,
) -> bool:
    """Validate XYZ from its small final sentinel instead of recounting all PNGs."""
    expected_tiles = max(0, int(expected_tiles))
    if expected_tiles == 0:
        return True
    if xyz_root is None:
        return False
    sentinel = xyz_root / str(int(zoom)) / "_XYZ_COMPLETE.json"
    if not sentinel.exists():
        return False
    try:
        payload = read_json(sentinel)
        if int(payload.get("zoom", -1)) != int(zoom):
            return False
        if int(payload.get("tiles_written", -1)) != expected_tiles:
            return False
        if int(payload.get("layout_version", -1)) != XYZ_EXPORT_LAYOUT_VERSION:
            return False
        if render_hash and str(payload.get("render_hash", "")) != str(render_hash):
            return False
        return True
    except Exception:
        return False


def visual_outputs_exist(
    args: argparse.Namespace,
    xyz_root: Path | None,
    mbtiles_path: Path,
    zoom: int,
    expected_tiles: int,
) -> bool:
    """Backward-compatible combined visual validation helper."""
    if not mbtiles_visual_output_exists(mbtiles_path, zoom, expected_tiles):
        return False
    if getattr(args, "write_xyz", False):
        return xyz_visual_output_exists(xyz_root, zoom, expected_tiles)
    return True


def _metadata_matches(path: Path, zoom: int, metric: str) -> bool:
    if not path.exists():
        return False
    try:
        metadata = read_json(path)
        return (
            int(metadata.get("zoom", -1)) == int(zoom)
            and str(metadata.get("metric")) == str(metric)
        )
    except Exception:
        return False


def _value_pixels_schema_matches(
    data_path: Path,
    metadata_path: Path,
    expected_schema: str,
) -> bool:
    expected_schema = str(expected_schema).lower()
    try:
        metadata = read_json(metadata_path)
        saved_schema = metadata.get("value_pixels_schema")
        if saved_schema is not None:
            return str(saved_schema).lower() == expected_schema
        names = set(pq.ParquetFile(data_path).schema_arrow.names)
    except Exception:
        return False
    compact = {"gx", "gy", "value"}
    if expected_schema == "compact":
        return names == compact
    return compact.issubset(names) and {
        "zoom", "tile_x", "tile_y", "pixel_x", "pixel_y"
    }.issubset(names)


def analytical_outputs_exist(
    args: argparse.Namespace,
    output_dir: Path,
    output_name: str,
    zoom: int,
) -> bool:
    """Validate only the analytical outputs requested for this zoom."""
    if not analytical_exports_requested(args, zoom):
        return True
    metric = str(args.metric)

    if getattr(args, "write_value_pixels", False):
        data_path, metadata_path = external_value_pixel_paths(
            output_dir, output_name, zoom, metric
        )
        if (file_size_bytes(data_path) or 0) <= 0 or not _metadata_matches(
            metadata_path, zoom, metric
        ):
            return False
        if not _value_pixels_schema_matches(
            data_path,
            metadata_path,
            str(getattr(args, "value_pixels_schema", "compact")),
        ):
            return False

    if getattr(args, "write_value_tiffs", False):
        root = value_tiffs_root(output_dir, output_name)
        metadata_candidates = [
            root / f"z{int(zoom)}_metadata.json",
            root / (
                f"{output_name}_z{int(zoom)}_{safe_slug(metric)}_"
                "geotiff_metadata.json"
            ),
        ]
        if not any(_metadata_matches(path, zoom, metric) for path in metadata_candidates):
            return False
        if not any((file_size_bytes(path) or 0) > 0 for path in (root / f"z{int(zoom)}").glob("*/*.tif")):
            return False

    if getattr(args, "write_value_composite_geotiff", False):
        root = value_composite_root(output_dir, output_name)
        tif_candidates = [
            root / f"z{int(zoom)}.tif",
            root / f"{output_name}_z{int(zoom)}_{safe_slug(metric)}_values.tif",
        ]
        metadata_candidates = [
            root / f"z{int(zoom)}_metadata.json",
            root / (
                f"{output_name}_z{int(zoom)}_{safe_slug(metric)}_"
                "composite_metadata.json"
            ),
        ]
        if not any((file_size_bytes(path) or 0) > 0 for path in tif_candidates):
            return False
        if not any(_metadata_matches(path, zoom, metric) for path in metadata_candidates):
            return False

    return True


def adopt_legacy_completed_zooms(
    manager: RunStateManager,
    args: argparse.Namespace,
    first_incomplete_zoom: int,
    xyz_root: Path | None,
    mbtiles_path: Path,
    output_dir: Path,
    output_name: str,
) -> None:
    first_incomplete_zoom = int(first_incomplete_zoom)
    if not (int(args.min_zoom) <= first_incomplete_zoom <= int(args.max_zoom)):
        raise ValueError(
            "legacy_first_incomplete_zoom must be within the requested zoom range."
        )

    print(
        "Adopting a legacy output with no run manifest. "
        f"Zooms {args.min_zoom}-{first_incomplete_zoom - 1} will have their "
        "existing outputs recorded; missing analytical exports remain pending."
    )
    for zoom in range(int(args.min_zoom), first_incomplete_zoom):
        print(f"  Validating legacy zoom {zoom}...")
        xyz_count = count_xyz_tiles_for_zoom(xyz_root, zoom)
        mbtiles_count = sqlite_zoom_tile_count(mbtiles_path, zoom)
        if visual_outputs_requested(args) and mbtiles_count <= 0:
            raise RuntimeError(
                f"Cannot adopt legacy zoom {zoom}: no canonical MBTiles rows were found."
            )
        if args.write_xyz and xyz_count != mbtiles_count:
            raise RuntimeError(
                f"Cannot adopt legacy zoom {zoom}: XYZ has {xyz_count:,} tiles but "
                f"MBTiles has {mbtiles_count:,}."
            )
        tile_count = mbtiles_count
        manager.mark_render_complete(
            zoom=zoom,
            tiles_written=tile_count,
            adopted_from_legacy=True,
        )
        if args.write_xyz:
            zoom_dir = xyz_root / str(int(zoom)) if xyz_root is not None else None
            if zoom_dir is not None:
                atomic_write_json(
                    zoom_dir / "_XYZ_COMPLETE.json",
                    {
                        "zoom": int(zoom),
                        "tiles_written": int(tile_count),
                        "layout_version": XYZ_EXPORT_LAYOUT_VERSION,
                        "render_hash": manager.render_fingerprint.primary,
                        "source_mbtiles": str(mbtiles_path),
                        "completed_at": utc_now_iso(),
                    },
                )
            manager.mark_xyz_complete(
                zoom=zoom,
                tiles_written=tile_count,
                adopted_from_legacy=True,
            )

        export_done = not analytical_exports_requested(args, zoom)
        if export_done:
            manager.mark_export_complete(zoom, no_outputs_requested=True)
        elif analytical_outputs_exist(args, output_dir, output_name, zoom):
            manager.mark_export_complete(zoom, adopted_from_legacy=True)
            export_done = True

        if export_done:
            manager.mark_zoom_complete(
                zoom=zoom,
                tiles_written=tile_count,
                adopted_from_legacy=True,
            )
        else:
            manager.mark_zoom_stage(
                zoom,
                "analytical_exports_pending",
                adopted_from_legacy=True,
                tiles_written=tile_count,
            )
        print(f"  Legacy zoom {zoom} output adopted ({tile_count:,} tiles).")

    manager.mark_zoom_stage(
        first_incomplete_zoom,
        "not_started",
        adopted_from_legacy=True,
    )


def prepare_duckdb_execution_root(
    args: argparse.Namespace,
    manager: RunStateManager,
    work_dir: Path,
    output_name: str,
) -> tuple[Path, Path]:
    """
    Create a persistent run root plus a unique execution directory beneath it.

    The run root is saved in run_state.json and therefore remains associated
    with the resumable run. Actual DuckDB temp files are isolated per process
    execution so stale handles cannot collide with a resumed or parallel run.
    """
    duckdb_state = manager.manifest.setdefault("duckdb", {})
    saved_run_root = duckdb_state.get("run_root")

    if saved_run_root:
        run_root = Path(saved_run_root)
    else:
        if getattr(args, "duckdb_temp_dir", None):
            base_root = Path(args.duckdb_temp_dir)
        else:
            base_root = work_dir / "duckdb_temp_runs"
        run_root = base_root / f"run_{short_output_key(output_name)}" / manager.run_id
        duckdb_state["base_root"] = str(base_root)
        duckdb_state["run_root"] = str(run_root)

    execution_id = (
        f"execution_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
        f"_pid{os.getpid()}_{uuid.uuid4().hex[:8]}"
    )
    execution_root = run_root / "executions" / execution_id
    execution_root.mkdir(parents=True, exist_ok=True)

    executions = duckdb_state.setdefault("executions", [])
    executions.append(
        {
            "execution_id": execution_id,
            "path": str(execution_root),
            "pid": os.getpid(),
            "hostname": socket.gethostname(),
            "started_at": utc_now_iso(),
        }
    )
    duckdb_state["last_execution_root"] = str(execution_root)
    manager.save()

    atomic_write_json(
        run_root / "resume_context.json",
        {
            "run_id": manager.run_id,
            "output_name": output_name,
            "state_file": str(manager.manifest_path),
            "work_dir": str(work_dir),
            "last_execution_root": str(execution_root),
            "updated_at": utc_now_iso(),
        },
    )

    args.duckdb_temp_dir = str(execution_root)
    return run_root, execution_root


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


def add_segment_samples_to_buffers(
    key_buffer: list[int],
    value_buffer: list[float],
    x1: float,
    y1: float,
    x2: float,
    y2: float,
    value: float,
    world_px: int,
    sample_step_px: float,
    max_segment_samples: int,
) -> None:
    """
    Append sampled line pixels to simple Python lists.

    This is faster than updating a Python dict for every sample. The samples are
    compacted with NumPy in flush_sample_chunks_to_parquet(), so duplicate keys
    are summed in C-backed code rather than one dictionary update at a time.
    """
    if not np.isfinite(x1 + y1 + x2 + y2 + value):
        return
    if value <= 0:
        return

    dx = x2 - x1
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

    # Bind frequently used names locally to reduce Python loop overhead.
    append_key = key_buffer.append
    append_value = value_buffer.append
    floor = math.floor
    world_px_float = float(world_px)

    for i in range(steps + 1):
        t = i / steps
        gx = int(floor((x1 + dx * t) % world_px_float))
        gy = int(floor(y1 + dy * t))
        if 0 <= gy < world_px:
            append_key(gy * world_px + gx)
            append_value(weight)


def resolve_duckdb_temp_directory(
    args: argparse.Namespace,
    work_dir: Path,
    temp_subdir: str,
) -> Path:
    """Return and create the dedicated spill directory for one connection."""
    duckdb_temp_dir = getattr(args, "duckdb_temp_dir", None)
    if duckdb_temp_dir:
        temp_dir = Path(duckdb_temp_dir) / temp_subdir
    else:
        temp_dir = work_dir / "duckdb_temp" / temp_subdir
    temp_dir.mkdir(parents=True, exist_ok=True)
    return temp_dir


def normalise_path_for_comparison(value: str | Path) -> str:
    """Normalise a path only for comparing DuckDB's reported setting."""
    return os.path.normcase(os.path.abspath(os.path.normpath(str(value))))


def apply_duckdb_settings(
    con: duckdb.DuckDBPyConnection,
    args: argparse.Namespace,
    work_dir: Path,
    temp_subdir: str,
) -> Path:
    """
    Apply DuckDB settings consistently after the database has been opened.

    ``temp_directory`` is a database start-up setting for this workflow. It is
    supplied by ``connect_duckdb`` while DuckDB opens the database, before WAL
    recovery or any query can use temporary storage. This function verifies the
    value instead of unconditionally trying to switch it after opening.
    """
    threads = max(1, int(args.threads))
    con.execute(f"SET threads TO {threads}")

    preserve = bool(getattr(args, "duckdb_preserve_insertion_order", False))
    con.execute(f"SET preserve_insertion_order={'true' if preserve else 'false'}")

    if args.duckdb_memory_limit:
        con.execute(f"SET memory_limit = {sql_str(str(args.duckdb_memory_limit))}")

    temp_dir = resolve_duckdb_temp_directory(args, work_dir, temp_subdir)
    current_temp_dir = con.execute(
        "SELECT current_setting('temp_directory')"
    ).fetchone()[0]

    if normalise_path_for_comparison(current_temp_dir) != normalise_path_for_comparison(temp_dir):
        try:
            con.execute(f"SET temp_directory = {sql_str(temp_dir.as_posix())}")
        except Exception as exc:
            raise RuntimeError(
                "DuckDB opened the database before the requested temporary "
                "directory was applied. Open internal DuckDB connections through "
                "connect_duckdb() so temp_directory is provided at connection "
                "creation time."
            ) from exc

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


def connect_duckdb(
    database: str | Path | None,
    args: argparse.Namespace,
    work_dir: Path,
    temp_subdir: str,
) -> duckdb.DuckDBPyConnection:
    """
    Open DuckDB with its spill directory configured before database start-up.

    This is required when reopening a persistent database: opening the file can
    perform WAL recovery or other storage work before the first explicit SQL
    statement. Setting ``temp_directory`` afterwards can therefore raise
    "Cannot switch temporary directory after the current one has been used".
    """
    temp_dir = resolve_duckdb_temp_directory(args, work_dir, temp_subdir)
    database_value = ":memory:" if database is None else Path(database).as_posix()

    try:
        con = duckdb.connect(
            database=database_value,
            config={"temp_directory": temp_dir.as_posix()},
        )
    except TypeError as exc:
        raise RuntimeError(
            "This script requires a DuckDB Python version whose connect() "
            "function supports the config argument. Upgrade DuckDB, then retry."
        ) from exc

    try:
        apply_duckdb_settings(
            con=con,
            args=args,
            work_dir=work_dir,
            temp_subdir=temp_subdir,
        )
    except Exception:
        safe_close_duckdb(con, context=f"DuckDB setup ({temp_subdir})")
        raise

    return con

def create_selected_vessels_table(
    con: duckdb.DuckDBPyConnection,
    args: argparse.Namespace,
) -> list[str]:
    groups = [
        group.strip().upper()
        for group in args.groups
        if group.strip().upper() != "ALL"
    ]
    con.execute("DROP TABLE IF EXISTS selected_vessels")
    if not groups:
        print("Groups = ALL. Vessel metadata join is skipped during AIS staging.")
        return []

    vessel_ship_col = sql_identifier(args.vessel_ship_id_column)
    vessel_group_col = sql_identifier(args.vessel_group_column)
    group_values = ",".join(sql_str(group) for group in groups)
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
        SELECT vessel_ship_id
        FROM vessel_metadata
        WHERE vessel_ship_id IS NOT NULL
          AND comfleet_groupedtype IN ({group_values})
        GROUP BY vessel_ship_id
        """
    )

    selected_vessel_count = int(
        con.execute("SELECT COUNT(*) FROM selected_vessels").fetchone()[0]
    )
    print(
        f"Vessel metadata prefilter selected {selected_vessel_count:,} unique "
        f"SHIP_ID values for groups: {groups}"
    )
    if selected_vessel_count == 0:
        raise RuntimeError(
            "The vessel metadata prefilter selected zero vessels. "
            "Check the COMFLEET_GROUPEDTYPE spelling."
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

    ais_join_sql = (
        "INNER JOIN selected_vessels AS v "
        f"ON CAST(a.{ship_col} AS VARCHAR) = v.vessel_ship_id"
        if groups else ""
    )

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
            CAST(hash(ship_id) % {shard_count} AS INTEGER) AS ship_id_shard
        FROM (
            SELECT
                CAST(a.{ship_col} AS VARCHAR) AS ship_id,
                CAST(a.{lat_col} AS DOUBLE) AS lat,
                CAST(a.{lon_col} AS DOUBLE) AS lon,
                CAST(a.{speed_col} AS DOUBLE) / {float(args.speed_scale)} AS speed_kts,
                TRY_CAST(a.{ts_col} AS TIMESTAMP) AS ts
            FROM read_parquet({ais_files_sql}, union_by_name=true) AS a
            {ais_join_sql}
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

    staged_point_count = parquet_files_row_count(staged_files)
    print(
        f"Staged {staged_point_count:,} AIS points into "
        f"{len(staged_files):,} Parquet files (metadata count)."
    )

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
    segment_out.unlink(missing_ok=True)

    if args.require_both_endpoint_speeds:
        speed_filter_sql = (
            f"AND speed1_kts > {float(args.min_speed_knots)} "
            f"AND speed2_kts > {float(args.min_speed_knots)}"
        )
    else:
        speed_filter_sql = f"AND avg_speed_kts > {float(args.min_speed_knots)}"

    if args.metric == "vessel_hours":
        value_sql = "dt_seconds / 3600.0"
    elif args.metric == "track_km":
        value_sql = "distance_km"
    else:
        raise ValueError("Metric must be either vessel_hours or track_km")

    schema_mode = str(
        getattr(args, "segment_checkpoint_schema", "compact")
    ).lower()
    if schema_mode == "compact":
        output_columns = f"""
            lon1,
            lat1,
            lon2,
            lat2,
            {value_sql} AS value
        """
    elif schema_mode == "diagnostic":
        output_columns = f"""
            ship_id,
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
        """
    else:
        raise ValueError(
            "segment_checkpoint_schema must be 'compact' or 'diagnostic'"
        )

    con = connect_duckdb(
        database=None,
        args=args,
        work_dir=work_dir,
        temp_subdir=f"segments_shard_{shard:04d}",
    )
    sql = f"""
    COPY (
        WITH clean_points AS (
            SELECT ship_id, lat, lon, speed_kts, ts
            FROM read_parquet({sql_str(shard_glob)})
            WHERE ship_id IS NOT NULL
              AND ts IS NOT NULL
              AND lat IS NOT NULL
              AND lon IS NOT NULL
              AND speed_kts IS NOT NULL
        ),
        ordered_points AS (
            SELECT
                ship_id,
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
            WHERE lon1 IS NOT NULL
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
                                + COS(RADIANS(lat1))
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
        SELECT {output_columns}
        FROM measured_segments
        WHERE dt_seconds BETWEEN 1 AND {int(args.max_gap_minutes) * 60}
          AND distance_km > 0
          AND distance_km / NULLIF(dt_seconds / 3600.0, 0) / 1.852
              <= {float(args.max_implied_speed_knots)}
          {speed_filter_sql}
    )
    TO {sql_str(segment_out.as_posix())}
    (FORMAT PARQUET, COMPRESSION ZSTD);
    """

    try:
        con.execute(sql)
    finally:
        safe_close_duckdb(con, context=f"segment shard {shard:04d}")

    segment_count = parquet_file_row_count(segment_out)
    if segment_count == 0:
        segment_out.unlink(missing_ok=True)
    else:
        print(
            f"Shard {shard:04d}: built {segment_count:,} segments "
            f"({schema_mode} checkpoint)."
        )
    return int(segment_count)


def _build_segment_shard_worker(
    worker_args: argparse.Namespace,
    work_dir: str,
    staged_dir: str,
    segments_dir: str,
    shard: int,
) -> tuple[int, int, float]:
    started = time.perf_counter()
    count = build_segments_for_one_shard(
        args=worker_args,
        work_dir=Path(work_dir),
        staged_dir=Path(staged_dir),
        segments_dir=Path(segments_dir),
        shard=int(shard),
    )
    return int(shard), int(count), float(time.perf_counter() - started)



def build_segments_parquet(
    args: argparse.Namespace,
    work_dir: Path,
    config_hash: StageFingerprint,
) -> Path:
    """Build/reuse sharded track segments, optionally with a small process pool."""
    shard_count = int(args.ship_id_shards)
    if shard_count < 1:
        raise ValueError("ship_id_shards must be at least 1.")

    profiler = get_profiler(args)
    staged_dir = work_dir / "staged_points_by_ship_id_shard"
    staged_marker = staged_dir / STAGE_MARKER_FILENAME
    segments_dir = work_dir / "segments_sharded"
    segments_marker = segments_dir / STAGE_MARKER_FILENAME

    completed_segments = read_valid_completion_marker(
        segments_marker, config_hash, marker_type="segments_complete"
    )
    segment_files = list(segments_dir.glob("segments_shard_*.parquet"))
    expected_segment_files = (
        int(completed_segments.get("parquet_file_count", -1))
        if completed_segments is not None else -1
    )
    if (
        completed_segments is not None
        and segment_files
        and len(segment_files) == expected_segment_files
    ):
        print(
            "Reusing completed track-segment checkpoint: "
            f"{segments_dir} ({len(segment_files):,} Parquet files)."
        )
        return segments_dir

    staged_complete = read_valid_completion_marker(
        staged_marker, config_hash, marker_type="staged_points_complete"
    )
    staged_files = list(staged_dir.glob("ship_id_shard=*/*.parquet"))
    expected_staged_files = (
        int(staged_complete.get("parquet_file_count", -1))
        if staged_complete is not None else -1
    )
    if (
        staged_complete is not None
        and staged_files
        and len(staged_files) == expected_staged_files
    ):
        print(
            "Reusing completed staged AIS checkpoint: "
            f"{staged_dir} ({len(staged_files):,} Parquet files)."
        )
    else:
        if staged_dir.exists():
            print("Discarding incomplete staged AIS checkpoint and rebuilding it.")
            shutil.rmtree(staged_dir, ignore_errors=True)

        ais_files = find_parquet_files(Path(args.ais_folder))
        if not ais_files:
            raise FileNotFoundError(
                f"No Parquet or GeoParquet files found in: {args.ais_folder}"
            )

        db_path = work_dir / "stage_points.duckdb"
        remove_path_quietly(db_path)
        remove_path_quietly(Path(str(db_path) + ".wal"))
        con: duckdb.DuckDBPyConnection | None = None
        try:
            con = connect_duckdb(
                database=db_path,
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
            safe_close_duckdb(con, context="AIS staging")

        staged_files = list(staged_dir.glob("ship_id_shard=*/*.parquet"))
        if not staged_files:
            raise RuntimeError("No staged AIS point files were produced.")
        write_completion_marker(
            staged_marker,
            marker_type="staged_points_complete",
            config_hash=config_hash,
            parquet_file_count=len(staged_files),
            point_count=parquet_files_row_count(staged_files),
            staged_schema_version=2,
        )

    segments_dir.mkdir(parents=True, exist_ok=True)
    expected_segment_paths = {
        segments_dir / f"segments_shard_{shard:04d}.parquet"
        for shard in range(shard_count)
    }
    for unexpected_file in segments_dir.glob("segments_shard_*.parquet"):
        if unexpected_file not in expected_segment_paths:
            print(f"Removing unexpected segment shard file: {unexpected_file}")
            remove_path_quietly(unexpected_file)

    print("\nBuilding or resuming track segments shard by shard...")
    total_segments = 0
    pending_shards: list[int] = []
    for shard in range(shard_count):
        segment_out = segments_dir / f"segments_shard_{shard:04d}.parquet"
        shard_marker = segments_dir / f"_shard_{shard:04d}.complete.json"
        completed_shard = read_valid_completion_marker(
            shard_marker, config_hash, marker_type="segment_shard_complete"
        )
        if completed_shard is not None:
            saved_count = int(completed_shard.get("segment_count", 0))
            if saved_count == 0 or segment_out.exists():
                total_segments += saved_count
                print(
                    f"Shard {shard:04d}: reusing completed checkpoint "
                    f"({saved_count:,} segments)."
                )
                continue
        remove_path_quietly(shard_marker)
        remove_path_quietly(segment_out)
        pending_shards.append(shard)

    workers = max(1, int(getattr(args, "segment_shard_workers", 1)))
    workers = min(workers, max(1, len(pending_shards)))
    requested_worker_threads = getattr(args, "segment_worker_duckdb_threads", None)
    worker_threads = (
        max(1, int(requested_worker_threads))
        if requested_worker_threads is not None
        else max(1, int(args.threads) // workers)
    )
    print(
        f"Segment shard workers: {workers}; DuckDB threads per worker: {worker_threads}"
    )

    def record_result(shard: int, segment_count: int, elapsed: float) -> None:
        nonlocal total_segments
        segment_out = segments_dir / f"segments_shard_{shard:04d}.parquet"
        shard_marker = segments_dir / f"_shard_{shard:04d}.complete.json"
        total_segments += int(segment_count)
        write_completion_marker(
            shard_marker,
            marker_type="segment_shard_complete",
            config_hash=config_hash,
            shard=int(shard),
            segment_count=int(segment_count),
            output_file=str(segment_out) if segment_count > 0 else None,
            elapsed_seconds=float(elapsed),
            segment_checkpoint_schema=str(
                getattr(args, "segment_checkpoint_schema", "compact")
            ).lower(),
        )
        if profiler is not None:
            profiler.record_metric(
                "segment_shard_result",
                shard=int(shard),
                segment_count=int(segment_count),
                elapsed_seconds=float(elapsed),
                segments_per_second=(
                    float(segment_count) / elapsed if elapsed > 0 else None
                ),
            )

    if pending_shards and workers == 1:
        worker_args = copy.copy(args)
        worker_args.threads = worker_threads
        if hasattr(worker_args, "_profiler"):
            delattr(worker_args, "_profiler")
        for shard in pending_shards:
            started = time.perf_counter()
            segment_count = build_segments_for_one_shard(
                args=worker_args,
                work_dir=work_dir,
                staged_dir=staged_dir,
                segments_dir=segments_dir,
                shard=shard,
            )
            record_result(shard, segment_count, time.perf_counter() - started)
    elif pending_shards:
        worker_args = copy.copy(args)
        worker_args.threads = worker_threads
        if hasattr(worker_args, "_profiler"):
            delattr(worker_args, "_profiler")
        with ProcessPoolExecutor(
            max_workers=workers,
            mp_context=multiprocessing_get_context("spawn"),
        ) as pool:
            futures = {
                pool.submit(
                    _build_segment_shard_worker,
                    worker_args,
                    str(work_dir),
                    str(staged_dir),
                    str(segments_dir),
                    shard,
                ): shard
                for shard in pending_shards
            }
            for future in as_completed(futures):
                shard, segment_count, elapsed = future.result()
                record_result(shard, segment_count, elapsed)
                print(
                    f"Shard {shard:04d}: worker complete in "
                    f"{format_duration(elapsed)} ({segment_count:,} segments)."
                )

    segment_files = list(segments_dir.glob("segments_shard_*.parquet"))
    if total_segments == 0 or not segment_files:
        raise RuntimeError(
            "No valid segments were produced. Check the group filter, speed threshold, "
            "timestamp parsing, and join key."
        )

    write_completion_marker(
        segments_marker,
        marker_type="segments_complete",
        config_hash=config_hash,
        total_segments=int(total_segments),
        parquet_file_count=len(segment_files),
        segment_checkpoint_schema=str(
            getattr(args, "segment_checkpoint_schema", "compact")
        ).lower(),
        segment_checkpoint_version=SEGMENT_CHECKPOINT_VERSION,
    )
    print(
        f"\nBuilt/reused {total_segments:,} track segments across "
        f"{len(segment_files):,} shard files."
    )
    return segments_dir


def build_pixel_parts_for_zoom(
    segment_path: Path,
    zoom: int,
    stage_dir: Path,
    args: argparse.Namespace,
    config_hash: StageFingerprint,
    segment_config_hash: StageFingerprint | None = None,
    progress_path: Path | None = None,
) -> bool:
    """Build sparse pixel parts using native sampling and larger NumPy buffers."""
    profiler = get_profiler(args)
    marker_path = stage_dir / STAGE_MARKER_FILENAME
    completed = read_valid_completion_marker(
        marker_path, config_hash, marker_type="pixel_parts_complete"
    )
    existing_parts = list(stage_dir.glob("pixels_part_*.parquet"))
    if completed is not None:
        has_parts = bool(completed.get("has_parts", False))
        expected_parts = int(completed.get("part_file_count", -1))
        if len(existing_parts) == expected_parts and (
            (has_parts and expected_parts > 0)
            or (not has_parts and expected_parts == 0)
        ):
            print(
                f"Zoom {zoom}: reusing completed pixel-part checkpoint "
                f"({len(existing_parts):,} files)."
            )
            return has_parts

    if stage_dir.exists():
        print(f"Zoom {zoom}: discarding incomplete pixel-part checkpoint.")
        shutil.rmtree(stage_dir, ignore_errors=True)
    stage_dir.mkdir(parents=True, exist_ok=True)

    segment_files = sorted(segment_path.glob("segments_shard_*.parquet"))
    if not segment_files:
        raise RuntimeError(f"No segment Parquet files found in {segment_path}")

    total_segments_estimated = estimate_segment_rows_from_metadata(
        segment_path=segment_path,
        config_hash=segment_config_hash or config_hash,
    )
    if progress_path is None:
        progress_path = stage_dir / "current_stage_progress.json"
    reporter = ProgressEtaReporter(
        path=progress_path,
        stage="build_pixel_parts_for_zoom",
        zoom=int(zoom),
        total_segments=total_segments_estimated,
        update_seconds=float(getattr(args, "progress_update_seconds", 15.0)),
        enabled=bool(getattr(args, "write_progress_json", True)),
    )

    dataset = ds.dataset([path.as_posix() for path in segment_files], format="parquet")
    scanner_kwargs: dict[str, Any] = {
        "columns": ["lon1", "lat1", "lon2", "lat2", "value"],
        "batch_size": int(args.segment_batch_size),
        "use_threads": True,
    }
    scanner_kwargs["batch_readahead"] = int(getattr(args, "arrow_batch_readahead", 16))
    scanner_kwargs["fragment_readahead"] = int(
        getattr(args, "arrow_fragment_readahead", 8)
    )
    try:
        scanner = dataset.scanner(**scanner_kwargs)
    except TypeError:
        scanner_kwargs.pop("batch_readahead", None)
        scanner_kwargs.pop("fragment_readahead", None)
        scanner = dataset.scanner(**scanner_kwargs)

    world_px = TILE_SIZE * (1 << int(zoom))
    sample_flush_threshold = max(1, int(args.pixel_flush_threshold))
    use_numba = bool(getattr(args, "use_numba_rasterizer", True)) and (
        _sample_segments_numba is not None
    )
    if bool(getattr(args, "use_numba_rasterizer", True)) and not use_numba:
        print("Warning: Numba is unavailable; using the slower Python rasteriser.")
    if use_numba and numba_set_num_threads is not None:
        requested_numba_threads = int(getattr(args, "numba_threads", 0) or 0)
        if requested_numba_threads > 0:
            numba_set_num_threads(requested_numba_threads)

    if use_numba:
        warm_x = np.array([0.0], dtype=np.float64)
        warm_y = np.array([0.0], dtype=np.float64)
        warm_x2 = np.array([1.0], dtype=np.float64)
        warm_value = np.array([1.0], dtype=np.float64)
        if profiler is not None:
            context = profiler.operation("numba_rasterizer_compile_warmup", zoom=int(zoom))
        else:
            context = null_profile_context()
        with context as metrics:
            warm_keys, _warm_values = _sample_segments_numba(
                warm_x, warm_y, warm_x2, warm_y, warm_value,
                int(world_px), float(args.sample_step_px), int(args.max_segment_samples)
            )
            metrics["items"] = int(warm_keys.size)
            metrics["item_unit"] = "samples"
        print(
            "Rasteriser: Numba native kernel"
            + (
                f" ({numba_get_num_threads()} threads)"
                if numba_get_num_threads is not None else ""
            )
        )
    else:
        print("Rasteriser: Python fallback")

    print(f"Aggregating zoom {zoom} into sparse pixels with native/vectorised compaction...")
    if total_segments_estimated:
        approx_batches = math.ceil(
            total_segments_estimated / max(1, int(args.segment_batch_size))
        )
        print(
            f"  Estimated segments: {total_segments_estimated:,} "
            f"(~{approx_batches:,} batches at {int(args.segment_batch_size):,})"
        )
    print(f"  Sample flush threshold: {sample_flush_threshold:,}")
    print(f"  Progress JSON: {progress_path}")

    key_chunks: list[np.ndarray] = []
    value_chunks: list[np.ndarray] = []
    samples_in_buffer = 0
    samples_generated_total = 0
    compacted_pixels_total = 0
    part_number = 0
    segments_seen = 0
    batches_seen = 0
    timings: defaultdict[str, float] = defaultdict(float)
    bytes_written = 0

    initial_payload = reporter.maybe_write(
        segments_seen=0, batches_seen=0, samples_buffered=0,
        part_number=0, final=False
    )
    pbar_total = total_segments_estimated if total_segments_estimated else None

    operation_context = (
        profiler.operation(
            "rasterise_segment_batches",
            zoom=int(zoom),
            estimated_segments=total_segments_estimated,
            segment_batch_size=int(args.segment_batch_size),
            sample_flush_threshold=sample_flush_threshold,
            implementation="numba" if use_numba else "python",
        )
        if profiler is not None
        else null_profile_context()
    )

    with operation_context as operation_metrics:
        with tqdm(
            total=pbar_total,
            desc=f"z{zoom}",
            unit="seg",
            unit_scale=True,
            dynamic_ncols=True,
            mininterval=1.0,
        ) as pbar:
            postfix = ProgressEtaReporter.postfix_from_payload(initial_payload)
            if postfix:
                pbar.set_postfix_str(postfix)

            batch_iterator = iter(scanner.to_batches())
            while True:
                started = time.perf_counter()
                try:
                    batch = next(batch_iterator)
                except StopIteration:
                    timings["arrow_fetch_seconds"] += time.perf_counter() - started
                    break
                timings["arrow_fetch_seconds"] += time.perf_counter() - started
                batches_seen += 1

                started = time.perf_counter()
                names = batch.schema.names
                index = {name: names.index(name) for name in names}
                lon1 = np.asarray(
                    batch.column(index["lon1"]).to_numpy(zero_copy_only=False),
                    dtype=np.float64,
                )
                lat1 = np.asarray(
                    batch.column(index["lat1"]).to_numpy(zero_copy_only=False),
                    dtype=np.float64,
                )
                lon2 = np.asarray(
                    batch.column(index["lon2"]).to_numpy(zero_copy_only=False),
                    dtype=np.float64,
                )
                lat2 = np.asarray(
                    batch.column(index["lat2"]).to_numpy(zero_copy_only=False),
                    dtype=np.float64,
                )
                values = np.asarray(
                    batch.column(index["value"]).to_numpy(zero_copy_only=False),
                    dtype=np.float64,
                )
                timings["arrow_to_numpy_seconds"] += time.perf_counter() - started

                started = time.perf_counter()
                x1, y1 = lonlat_arrays_to_global_pixels(lon1, lat1, int(zoom))
                x2, y2 = lonlat_arrays_to_global_pixels(lon2, lat2, int(zoom))
                valid = (
                    np.isfinite(x1) & np.isfinite(y1)
                    & np.isfinite(x2) & np.isfinite(y2)
                    & np.isfinite(values) & (values > 0)
                )
                batch_valid_segments = int(valid.sum())
                timings["coordinate_transform_seconds"] += time.perf_counter() - started

                if batch_valid_segments > 0:
                    started = time.perf_counter()
                    valid_x1 = np.ascontiguousarray(x1[valid], dtype=np.float64)
                    valid_y1 = np.ascontiguousarray(y1[valid], dtype=np.float64)
                    valid_x2 = np.ascontiguousarray(x2[valid], dtype=np.float64)
                    valid_y2 = np.ascontiguousarray(y2[valid], dtype=np.float64)
                    valid_values = np.ascontiguousarray(values[valid], dtype=np.float64)
                    sample_counts = estimate_segment_sample_counts(
                        valid_x1, valid_y1, valid_x2, valid_y2,
                        int(world_px), float(args.sample_step_px),
                        int(args.max_segment_samples),
                    )
                    timings["sample_count_estimate_seconds"] += (
                        time.perf_counter() - started
                    )

                    for sample_slice in iter_sample_budget_slices(
                        sample_counts, sample_flush_threshold
                    ):
                        started = time.perf_counter()
                        if use_numba:
                            batch_keys, batch_values = _sample_segments_numba(
                                valid_x1[sample_slice],
                                valid_y1[sample_slice],
                                valid_x2[sample_slice],
                                valid_y2[sample_slice],
                                valid_values[sample_slice],
                                int(world_px),
                                float(args.sample_step_px),
                                int(args.max_segment_samples),
                            )
                            if batch_keys.size:
                                valid_samples = batch_keys >= 0
                                if not valid_samples.all():
                                    batch_keys = batch_keys[valid_samples]
                                    batch_values = batch_values[valid_samples]
                        else:
                            batch_keys, batch_values = sample_segments_python_arrays(
                                valid_x1[sample_slice],
                                valid_y1[sample_slice],
                                valid_x2[sample_slice],
                                valid_y2[sample_slice],
                                valid_values[sample_slice],
                                int(world_px),
                                float(args.sample_step_px),
                                int(args.max_segment_samples),
                            )
                        timings["sampling_kernel_seconds"] += (
                            time.perf_counter() - started
                        )

                        if batch_keys.size:
                            key_chunks.append(batch_keys)
                            value_chunks.append(batch_values)
                            generated = int(batch_keys.size)
                            samples_in_buffer += generated
                            samples_generated_total += generated

                        if samples_in_buffer >= sample_flush_threshold:
                            next_part, flush_stats = flush_sample_chunks_to_parquet(
                                key_chunks=key_chunks,
                                value_chunks=value_chunks,
                                output_dir=stage_dir,
                                part_number=part_number,
                                world_px=world_px,
                                args=args,
                            )
                            part_number = next_part
                            samples_in_buffer = 0
                            compacted_pixels_total += int(
                                flush_stats["output_pixels"]
                            )
                            bytes_written += int(flush_stats["bytes_written"])
                            for name in (
                                "concat_seconds",
                                "compact_seconds",
                                "parquet_write_seconds",
                            ):
                                timings[name] += float(flush_stats[name])

                segments_seen += batch_valid_segments
                pbar.update(batch_valid_segments)
                payload = reporter.maybe_write(
                    segments_seen=segments_seen,
                    batches_seen=batches_seen,
                    samples_buffered=samples_generated_total,
                    part_number=part_number,
                    final=False,
                )
                postfix = ProgressEtaReporter.postfix_from_payload(payload)
                if postfix:
                    pbar.set_postfix_str(postfix)

        if key_chunks:
            next_part, flush_stats = flush_sample_chunks_to_parquet(
                key_chunks=key_chunks,
                value_chunks=value_chunks,
                output_dir=stage_dir,
                part_number=part_number,
                world_px=world_px,
                args=args,
            )
            part_number = next_part
            compacted_pixels_total += int(flush_stats["output_pixels"])
            bytes_written += int(flush_stats["bytes_written"])
            for name in ("concat_seconds", "compact_seconds", "parquet_write_seconds"):
                timings[name] += float(flush_stats[name])

        operation_metrics.update(
            {
                "items": int(segments_seen),
                "item_unit": "segments",
                "batches": int(batches_seen),
                "samples_generated": int(samples_generated_total),
                "compacted_pixels_written": int(compacted_pixels_total),
                "part_files": int(part_number),
                "bytes": int(bytes_written),
                "phase_seconds": dict(timings),
            }
        )

    parts = list(stage_dir.glob("pixels_part_*.parquet"))
    has_parts = bool(parts)
    final_payload = reporter.maybe_write(
        segments_seen=segments_seen,
        batches_seen=batches_seen,
        samples_buffered=samples_generated_total,
        part_number=part_number,
        final=True,
    )

    print(
        f"Zoom {zoom}: processed {segments_seen:,} segments into {len(parts):,} "
        f"pixel part files after generating {samples_generated_total:,} samples."
    )
    print("Rasterisation phase breakdown:")
    for name, seconds in sorted(timings.items(), key=lambda item: item[1], reverse=True):
        print(f"  {name}: {format_duration(seconds)}")
    print(f"  intermediate Parquet bytes: {format_bytes(bytes_written)}")

    if profiler is not None:
        profiler.record_metric(
            "rasterisation_phase_breakdown",
            zoom=int(zoom),
            segments_seen=int(segments_seen),
            samples_generated=int(samples_generated_total),
            compacted_pixels_written=int(compacted_pixels_total),
            part_file_count=len(parts),
            intermediate_bytes=int(bytes_written),
            phase_seconds=dict(timings),
        )

    write_completion_marker(
        marker_path,
        marker_type="pixel_parts_complete",
        config_hash=config_hash,
        zoom=int(zoom),
        segments_seen=int(segments_seen),
        total_segments_estimated=(
            int(total_segments_estimated) if total_segments_estimated is not None else None
        ),
        samples_buffered=int(samples_generated_total),
        part_file_count=len(parts),
        has_parts=has_parts,
        elapsed_seconds=float(final_payload.get("elapsed_seconds", 0.0)),
        rate_segments_per_second=final_payload.get("rate_segments_per_second"),
        implementation="numba" if use_numba else "python",
        phase_seconds=dict(timings),
        intermediate_bytes=int(bytes_written),
    )
    return has_parts



def aggregate_pixel_parts(
    con: duckdb.DuckDBPyConnection,
    stage_dir: Path,
    zoom: int,
    profiler: StageProfiler | None = None,
) -> tuple[str, int]:
    table_name = f"pixels_z{zoom}"
    glob_path = (stage_dir / "*.parquet").as_posix()
    coord_type = coordinate_sql_type_for_zoom(zoom)
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    print(f"Combining duplicate pixels for zoom {zoom} without a redundant final sort...")

    context = (
        profiler.operation("duckdb_aggregate_pixel_parts", zoom=int(zoom))
        if profiler is not None else null_profile_context()
    )
    with context as metrics:
        con.execute(
            f"""
            CREATE TABLE {table_name} AS
            SELECT
                CAST(gx AS {coord_type}) AS gx,
                CAST(gy AS {coord_type}) AS gy,
                CAST(SUM(value) AS DOUBLE) AS value
            FROM read_parquet({sql_str(glob_path)})
            GROUP BY 1, 2
            HAVING SUM(value) > 0
            """
        )
        pixel_count = int(
            con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        )
        metrics["items"] = pixel_count
        metrics["item_unit"] = "pixels"
        metrics["source_part_files"] = len(list(stage_dir.glob("*.parquet")))

    print(f"Zoom {zoom}: {pixel_count:,} non-zero pixels after aggregation.")
    return table_name, pixel_count





def aggregated_checkpoint_paths(work_dir: Path, zoom: int) -> tuple[Path, Path]:
    root = work_dir / "aggregated_pixel_checkpoints"
    parquet_path = root / f"pixels_z{int(zoom)}.parquet"
    marker_path = root / f"pixels_z{int(zoom)}.complete.json"
    return parquet_path, marker_path


def write_aggregated_pixel_checkpoint(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    zoom: int,
    work_dir: Path,
    config_hash: StageFingerprint,
    pixel_count: int,
    args: argparse.Namespace | None = None,
    profiler: StageProfiler | None = None,
) -> Path:
    parquet_path, marker_path = aggregated_checkpoint_paths(work_dir, zoom)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    remove_path_quietly(parquet_path)
    remove_path_quietly(marker_path)

    context = (
        profiler.operation(
            "duckdb_write_aggregated_checkpoint",
            zoom=int(zoom),
            pixel_count=int(pixel_count),
            sorted_output=False,
        )
        if profiler is not None else null_profile_context()
    )
    with context as metrics:
        compression_mode = duckdb_copy_query_to_parquet(
            con=con,
            query_sql=f"""
                SELECT gx, gy, CAST(value AS DOUBLE) AS value
                FROM {table_name}
                WHERE value > 0
            """,
            output_path=parquet_path,
            args=args,
        )
        size = file_size_bytes(parquet_path) or 0
        metrics.update(
            {
                "items": int(pixel_count),
                "item_unit": "pixels",
                "bytes": int(size),
                "path": str(parquet_path),
                "compression_mode": compression_mode,
            }
        )

    write_completion_marker(
        marker_path,
        marker_type="aggregated_pixels_complete",
        config_hash=config_hash,
        zoom=int(zoom),
        pixel_count=int(pixel_count),
        parquet_path=str(parquet_path),
        byte_size=file_size_bytes(parquet_path),
        sorted_output=False,
        compression_mode=compression_mode,
    )
    print(
        f"Zoom {zoom}: wrote resumable aggregated-pixel checkpoint: {parquet_path} "
        f"({format_bytes(file_size_bytes(parquet_path))})"
    )
    return parquet_path



def load_pixel_table_from_parquet(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    parquet_path: Path,
    zoom: int,
    profiler: StageProfiler | None = None,
) -> int:
    coord_type = coordinate_sql_type_for_zoom(zoom)
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    context = (
        profiler.operation(
            "duckdb_load_pixel_checkpoint",
            zoom=int(zoom),
            source=str(parquet_path),
        )
        if profiler is not None else null_profile_context()
    )
    with context as metrics:
        con.execute(
            f"""
            CREATE TABLE {table_name} AS
            SELECT
                CAST(gx AS {coord_type}) AS gx,
                CAST(gy AS {coord_type}) AS gy,
                CAST(value AS DOUBLE) AS value
            FROM read_parquet({sql_str(parquet_path.as_posix())})
            WHERE value > 0
            """
        )
        pixel_count = int(
            con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        )
        metrics["items"] = pixel_count
        metrics["item_unit"] = "pixels"
        metrics["bytes"] = file_size_bytes(parquet_path) or 0
    return pixel_count




def derive_pixel_table_from_higher_zoom(
    con: duckdb.DuckDBPyConnection,
    source_table: str,
    source_zoom: int,
    target_zoom: int,
    profiler: StageProfiler | None = None,
) -> tuple[str, int]:
    """Derive one lower analytical level without imposing a costly row order."""
    source_zoom = int(source_zoom)
    target_zoom = int(target_zoom)
    if target_zoom > source_zoom:
        raise ValueError("target_zoom must be less than or equal to source_zoom")

    table_name = f"pixels_z{target_zoom}"
    coord_type = coordinate_sql_type_for_zoom(target_zoom)
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    scale = 1 << (source_zoom - target_zoom)
    print(
        f"Deriving zoom {target_zoom} from zoom {source_zoom} by summing "
        f"{scale} x {scale} pixel blocks."
    )

    context = (
        profiler.operation(
            "duckdb_derive_pyramid_level",
            source_zoom=source_zoom,
            target_zoom=target_zoom,
            scale=scale,
        )
        if profiler is not None else null_profile_context()
    )
    with context as metrics:
        if target_zoom == source_zoom:
            con.execute(
                f"""
                CREATE TABLE {table_name} AS
                SELECT
                    CAST(gx AS {coord_type}) AS gx,
                    CAST(gy AS {coord_type}) AS gy,
                    CAST(value AS DOUBLE) AS value
                FROM {source_table}
                WHERE value > 0
                """
            )
        else:
            con.execute(
                f"""
                CREATE TABLE {table_name} AS
                SELECT
                    CAST(FLOOR(gx / {scale}) AS {coord_type}) AS gx,
                    CAST(FLOOR(gy / {scale}) AS {coord_type}) AS gy,
                    CAST(SUM(value) AS DOUBLE) AS value
                FROM {source_table}
                WHERE value > 0
                GROUP BY 1, 2
                HAVING SUM(value) > 0
                """
            )
        pixel_count = int(
            con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        )
        metrics["items"] = pixel_count
        metrics["item_unit"] = "pixels"

    print(f"Zoom {target_zoom}: derived {pixel_count:,} non-zero pixels.")
    return table_name, pixel_count



def external_value_pixel_paths(
    output_dir: Path,
    output_name: str,
    zoom: int,
    metric: str,
) -> tuple[Path, Path]:
    """Return resumable value-pixel paths, preferring existing legacy files."""
    root = value_pixels_root(output_dir, output_name)
    short_data = root / f"z{int(zoom)}.parquet"
    short_meta = root / f"z{int(zoom)}_metadata.json"
    legacy_stem = f"{output_name}_z{int(zoom)}_{safe_slug(metric)}"
    legacy_data = root / f"{legacy_stem}_pixels.parquet"
    legacy_meta = root / f"{legacy_stem}_metadata.json"

    # Existing large Parquet exports are valuable checkpoints. Reuse their old
    # filename rather than creating a second multi-GB copy merely to shorten it.
    if short_data.exists():
        return short_data, short_meta if short_meta.exists() else short_meta
    if legacy_data.exists():
        return legacy_data, legacy_meta if legacy_meta.exists() else short_meta
    if short_meta.exists():
        return short_data, short_meta
    if legacy_meta.exists():
        return legacy_data, legacy_meta
    return short_data, short_meta


def try_load_existing_aggregated_pixels(
    con: duckdb.DuckDBPyConnection,
    zoom: int,
    args: argparse.Namespace,
    work_dir: Path,
    output_dir: Path,
    output_name: str,
    config_hash: StageFingerprint,
) -> tuple[str, int, str] | None:
    """Load the newest trustworthy pixel checkpoint without rebuilding segments."""
    table_name = f"pixels_z{int(zoom)}"
    checkpoint_path, marker_path = aggregated_checkpoint_paths(work_dir, zoom)
    marker = read_valid_completion_marker(
        marker_path,
        config_hash,
        marker_type="aggregated_pixels_complete",
    )

    if marker is not None and checkpoint_path.exists():
        try:
            pixel_count = load_pixel_table_from_parquet(
                con, table_name, checkpoint_path, zoom, get_profiler(args)
            )
            if pixel_count > 0:
                print(
                    f"Zoom {zoom}: loaded {pixel_count:,} pixels from the "
                    "internal aggregated checkpoint."
                )
                return table_name, pixel_count, "internal_aggregated_checkpoint"
        except Exception as exc:
            print(
                f"Warning: zoom {zoom} aggregated checkpoint is unreadable and "
                f"will be rebuilt: {exc}"
            )
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
            remove_path_quietly(marker_path)

    # The analytical value-pixel export is written before visual rendering. It
    # is therefore an excellent recovery point for the user's legacy zoom-11 run.
    value_path, metadata_path = external_value_pixel_paths(
        output_dir, output_name, zoom, args.metric
    )
    if value_path.exists() and metadata_path.exists():
        try:
            metadata = read_json(metadata_path)
            if int(metadata.get("zoom", -1)) != int(zoom):
                raise ValueError("metadata zoom does not match")
            if str(metadata.get("metric")) != str(args.metric):
                raise ValueError("metadata metric does not match")
            pixel_count = load_pixel_table_from_parquet(
                con, table_name, value_path, zoom, get_profiler(args)
            )
        except Exception as exc:
            print(
                f"Warning: zoom {zoom} analytical value-pixel export cannot be "
                f"used as a checkpoint: {exc}"
            )
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
        else:
            if pixel_count > 0:
                print(
                    f"Zoom {zoom}: recovered {pixel_count:,} aggregated pixels "
                    f"from the analytical value-pixel export: {value_path}"
                )
                if getattr(args, "checkpoint_aggregated_pixels", True):
                    # Failure to write the requested internal checkpoint should
                    # stop the run rather than trigger an unnecessary AIS rebuild.
                    write_aggregated_pixel_checkpoint(
                        con=con,
                        table_name=table_name,
                        zoom=zoom,
                        work_dir=work_dir,
                        config_hash=config_hash,
                        pixel_count=pixel_count,
                        args=args,
                        profiler=get_profiler(args),
                    )
                return table_name, pixel_count, "analytical_value_pixels"

    return None

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
    **extra: Any,
) -> None:
    metadata = {
        "output_name": output_name,
        "zoom": int(zoom),
        "tile_size": TILE_SIZE,
        "crs": "EPSG:3857",
        "metric": args.metric,
        "metric_description": (
            "Accumulated vessel track kilometres per Web Mercator pixel"
            if args.metric == "track_km"
            else "Accumulated vessel-hours per Web Mercator pixel"
        ),
        "value_column": "value",
        "pixel_count": int(pixel_count),
        "speed_filter_knots": float(args.min_speed_knots),
        "speed_scale": float(args.speed_scale),
        "max_gap_minutes": int(args.max_gap_minutes),
        "max_implied_speed_knots": float(args.max_implied_speed_knots),
        "resolution_m_at_equator": web_mercator_resolution_m(zoom),
        "performance_implementation_version": PERFORMANCE_IMPLEMENTATION_VERSION,
        "note": (
            "These values are pre-colour-rendering analytical density values. "
            "They are not the RGBA values stored in the visual MBTiles/XYZ PNG output."
        ),
    }
    metadata.update(_json_safe(extra))
    metadata["path_layout_version"] = PATH_LAYOUT_VERSION
    atomic_write_json(metadata_path, metadata)



def export_value_pixels_parquet(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    zoom: int,
    args: argparse.Namespace,
    output_dir: Path,
    output_name: str,
    pixel_count: int,
    work_dir: Path,
    profiler: StageProfiler | None = None,
) -> Path:
    """
    Export analytical pixels in compact or enriched form.

    Compact mode is the speed-oriented default. It reuses the already-written
    checkpoint containing ``gx, gy, value`` via an NTFS/POSIX hard link whenever
    possible, avoiding another billion-row scan, coordinate calculation and sort.
    """
    value_root = value_pixels_root(output_dir, output_name)
    value_root.mkdir(parents=True, exist_ok=True)
    out_path, metadata_path = external_value_pixel_paths(
        output_dir, output_name, zoom, args.metric
    )
    schema_mode = str(getattr(args, "value_pixels_schema", "compact")).lower()
    if out_path.exists() and metadata_path.exists():
        try:
            existing_meta = read_json(metadata_path)
            existing_ok = (
                int(existing_meta.get("zoom", -1)) == int(zoom)
                and str(existing_meta.get("metric")) == str(args.metric)
                and str(existing_meta.get("value_pixels_schema", schema_mode)).lower() == schema_mode
                and (file_size_bytes(out_path) or 0) > 0
            )
        except Exception:
            existing_ok = False
        if existing_ok:
            print(
                f"Zoom {zoom}: reusing existing analytical value pixels: {out_path}"
            )
            return out_path

    out_path.unlink(missing_ok=True)
    if schema_mode not in {"compact", "enriched"}:
        raise ValueError("value_pixels_schema must be 'compact' or 'enriched'")

    link_mode: str | None = None
    checkpoint_path, _marker_path = aggregated_checkpoint_paths(work_dir, zoom)
    context = (
        profiler.operation(
            "export_value_pixels_parquet",
            zoom=int(zoom),
            pixel_count=int(pixel_count),
            schema=schema_mode,
        )
        if profiler is not None else null_profile_context()
    )
    with context as metrics:
        if schema_mode == "compact" and checkpoint_path.exists():
            link_mode = hardlink_or_copy(checkpoint_path, out_path)
        elif schema_mode == "compact":
            link_mode = duckdb_copy_query_to_parquet(
                con=con,
                query_sql=f"""
                    SELECT gx, gy, CAST(value AS DOUBLE) AS value
                    FROM {table_name}
                    WHERE value > 0
                """,
                output_path=out_path,
                args=args,
            )
        else:
            world_px = TILE_SIZE * (1 << int(zoom))
            res = web_mercator_resolution_m(zoom)
            half = WEB_MERCATOR_HALF_WORLD_M
            link_mode = "enriched_" + duckdb_copy_query_to_parquet(
                con=con,
                query_sql=f"""
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
                """,
                output_path=out_path,
                args=args,
            )

        size = file_size_bytes(out_path) or 0
        metrics.update(
            {
                "items": int(pixel_count),
                "item_unit": "pixels",
                "bytes": int(size),
                "link_or_copy_mode": link_mode,
                "path": str(out_path),
            }
        )

    columns = (
        ["gx", "gy", "value"]
        if schema_mode == "compact"
        else [
            "zoom", "gx", "gy", "tile_x", "tile_y", "pixel_x", "pixel_y",
            "x_mercator_m", "y_mercator_m", "lon_center", "lat_center",
            "value", safe_slug(args.metric),
        ]
    )
    write_value_metadata(
        metadata_path,
        args,
        zoom,
        output_name,
        pixel_count,
        value_pixels_schema=schema_mode,
        columns=columns,
        checkpoint_reuse_mode=link_mode,
        byte_size=file_size_bytes(out_path),
    )
    print(
        f"Zoom {zoom}: wrote analytical value pixels ({schema_mode}, {link_mode}): "
        f"{out_path} ({format_bytes(file_size_bytes(out_path))})"
    )
    print(f"Zoom {zoom}: wrote analytical value metadata: {metadata_path}")
    return out_path



def export_value_tiff_tiles(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    zoom: int,
    args: argparse.Namespace,
    output_dir: Path,
    output_name: str,
    pixel_count: int,
) -> int:
    """Stream and vectorise individual analytical Float32 GeoTIFF tile output."""
    if rasterio is None or from_origin is None:
        raise RuntimeError("Float32 GeoTIFF output requires rasterio.")
    profiler = get_profiler(args)
    tiff_output_root = value_tiffs_root(output_dir, output_name)
    tiff_root = tiff_output_root / f"z{int(zoom)}"
    short_metadata_path = tiff_output_root / f"z{int(zoom)}_metadata.json"
    legacy_metadata_path = tiff_output_root / (
        f"{output_name}_z{int(zoom)}_{safe_slug(args.metric)}_geotiff_metadata.json"
    )
    metadata_path = (
        legacy_metadata_path if legacy_metadata_path.exists() else short_metadata_path
    )
    tiff_output_root.mkdir(parents=True, exist_ok=True)

    tile_geotiff_settings = geotiff_output_settings(
        args,
        output_kind="tiles",
        block_size=TILE_SIZE,
    )
    tile_geotiff_settings_hash = stable_json_hash(tile_geotiff_settings)
    metadata_settings_ok = False
    if metadata_path.exists():
        try:
            existing_tile_metadata = read_json(metadata_path)
            metadata_settings_ok = (
                str(existing_tile_metadata.get("geotiff_settings_hash", ""))
                == tile_geotiff_settings_hash
            )
        except Exception:
            metadata_settings_ok = False

    ntiles = 1 << int(zoom)
    coord_type = coordinate_sql_type_for_zoom(zoom)
    tile_type = "INTEGER" if ntiles - 1 <= np.iinfo(np.int32).max else "BIGINT"
    rows_per_batch = int(getattr(args, "render_row_batch_size", 500_000))
    sql = f"""
    SELECT
        CAST(FLOOR(gx / {TILE_SIZE}) AS {tile_type}) AS tile_x,
        CAST(FLOOR(gy / {TILE_SIZE}) AS {tile_type}) AS tile_y,
        CAST(gx AS {coord_type}) AS gx,
        CAST(gy AS {coord_type}) AS gy,
        CAST(value AS FLOAT) AS value
    FROM {table_name}
    WHERE value > 0
    ORDER BY tile_y, tile_x
    """
    res = web_mercator_resolution_m(zoom)
    half = WEB_MERCATOR_HALF_WORLD_M
    limit = getattr(args, "value_tiff_tile_limit", None)

    # A previous run may have finished every GeoTIFF tile and then failed only
    # while writing the over-long metadata filename. Verify the existing tile
    # count before deleting anything; if it matches the analytical table, create
    # the new short metadata file and reuse the completed tiles.
    if tiff_root.exists():
        existing_tifs = [p for p in tiff_root.glob("*/*.tif") if p.is_file()]
        if existing_tifs and metadata_settings_ok:
            expected_tiles = int(
                con.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM (
                        SELECT
                            FLOOR(gx / {TILE_SIZE}) AS tile_x,
                            FLOOR(gy / {TILE_SIZE}) AS tile_y
                        FROM {table_name}
                        WHERE value > 0
                        GROUP BY 1, 2
                    )
                    """
                ).fetchone()[0]
            )
            if limit is not None:
                expected_tiles = min(expected_tiles, int(limit))
            existing_complete = (
                expected_tiles > 0
                and len(existing_tifs) == expected_tiles
                and all((file_size_bytes(p) or 0) > 0 for p in existing_tifs)
            )
            if existing_complete:
                existing_bytes = sum(file_size_bytes(p) or 0 for p in existing_tifs)
                write_value_metadata(
                    metadata_path, args, zoom, output_name, pixel_count,
                    tiles_written=int(expected_tiles),
                    byte_size=int(existing_bytes),
                    recovered_existing_tiles=True,
                    phase_seconds={},
                    geotiff_settings=tile_geotiff_settings,
                    geotiff_settings_hash=tile_geotiff_settings_hash,
                )
                print(
                    f"Zoom {zoom}: reused {expected_tiles:,} existing analytical "
                    f"GeoTIFF tiles and wrote metadata: {metadata_path}"
                )
                return int(expected_tiles)

        # Existing tiles are incomplete or unverified; rebuild this zoom cleanly.
        shutil.rmtree(tiff_root)
    tiff_root.mkdir(parents=True, exist_ok=True)

    counters: defaultdict[str, float] = defaultdict(float)
    tiles_written = 0
    output_bytes = 0
    created_tile_dirs: set[int] = set()
    progress = tqdm(total=int(pixel_count), desc=f"value GeoTIFF z{zoom}", unit="rows")
    context = (
        profiler.operation("write_value_geotiff_tiles", zoom=int(zoom), pixel_count=int(pixel_count))
        if profiler is not None else null_profile_context()
    )
    with context as metrics:
        try:
            for tile_x, tile_y, gxs, gys, values in iter_ordered_tile_groups(
                con, sql, rows_per_batch, ntiles, counters, progress
            ):
                if limit is not None and tiles_written >= int(limit):
                    break
                started = time.perf_counter()
                px = (gxs - int(tile_x) * TILE_SIZE).astype(np.intp, copy=False)
                py = (gys - int(tile_y) * TILE_SIZE).astype(np.intp, copy=False)
                arr = np.zeros((TILE_SIZE, TILE_SIZE), dtype=np.float32)
                arr[py, px] = values.astype(np.float32, copy=False)
                counters["numpy_pixel_placement_seconds"] += time.perf_counter() - started

                x_left = -half + (tile_x * TILE_SIZE) * res
                y_top = half - (tile_y * TILE_SIZE) * res
                transform = from_origin(x_left, y_top, res, res)
                tile_dir = tiff_root / str(tile_x)
                if int(tile_x) not in created_tile_dirs:
                    tile_dir.mkdir(parents=True, exist_ok=True)
                    created_tile_dirs.add(int(tile_x))
                tif_path = tile_dir / f"{tile_y}.tif"
                started = time.perf_counter()
                with rasterio.open(
                    tif_path, "w", driver="GTiff", height=TILE_SIZE, width=TILE_SIZE,
                    count=1, dtype="float32", crs="EPSG:3857", transform=transform,
                    **geotiff_creation_options(args, block_size=TILE_SIZE),
                ) as dst:
                    dst.write(arr, 1)
                    dst.set_band_description(1, args.metric)
                counters["raster_write_seconds"] += time.perf_counter() - started
                output_bytes += file_size_bytes(tif_path) or 0
                tiles_written += 1
        finally:
            progress.close()
        metrics.update(
            {
                "items": int(counters.get("rows_streamed", 0)),
                "item_unit": "rows",
                "tiles_written": int(tiles_written),
                "bytes": int(output_bytes),
                "phase_seconds": dict(counters),
            }
        )

    write_value_metadata(
        metadata_path, args, zoom, output_name, pixel_count,
        tiles_written=int(tiles_written), byte_size=int(output_bytes),
        phase_seconds=dict(counters),
        geotiff_settings=tile_geotiff_settings,
        geotiff_settings_hash=tile_geotiff_settings_hash,
    )
    print(
        f"Zoom {zoom}: wrote {tiles_written:,} analytical GeoTIFF tiles: {tiff_root}"
    )
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
    """Write a composite Float32 GeoTIFF with vectorised tile-strip placement."""
    if rasterio is None or from_origin is None or Window is None:
        raise RuntimeError("Composite Float32 GeoTIFF output requires rasterio.")
    profiler = get_profiler(args)

    bounds_context = (
        profiler.operation("query_composite_geotiff_bounds", zoom=int(zoom))
        if profiler is not None else null_profile_context()
    )
    with bounds_context:
        bounds = con.execute(
            f"""
            SELECT
                MIN(CAST(FLOOR(gx / {TILE_SIZE}) AS BIGINT)),
                MAX(CAST(FLOOR(gx / {TILE_SIZE}) AS BIGINT)),
                MIN(CAST(FLOOR(gy / {TILE_SIZE}) AS BIGINT)),
                MAX(CAST(FLOOR(gy / {TILE_SIZE}) AS BIGINT))
            FROM {table_name}
            WHERE value > 0
            """
        ).fetchone()
    if bounds is None or bounds[0] is None:
        print(f"Zoom {zoom}: no analytical values found for composite GeoTIFF.")
        return None

    min_tile_x, max_tile_x, min_tile_y, max_tile_y = [int(value) for value in bounds]
    width = (max_tile_x - min_tile_x + 1) * TILE_SIZE
    height = (max_tile_y - min_tile_y + 1) * TILE_SIZE
    res = web_mercator_resolution_m(zoom)
    half = WEB_MERCATOR_HALF_WORLD_M
    transform = from_origin(
        -half + (min_tile_x * TILE_SIZE) * res,
        half - (min_tile_y * TILE_SIZE) * res,
        res,
        res,
    )

    composite_root = value_composite_root(output_dir, output_name)
    composite_root.mkdir(parents=True, exist_ok=True)
    short_tif_path = composite_root / f"z{int(zoom)}.tif"
    short_metadata_path = composite_root / f"z{int(zoom)}_metadata.json"
    legacy_tif_path = composite_root / (
        f"{output_name}_z{int(zoom)}_{safe_slug(args.metric)}_values.tif"
    )
    legacy_metadata_path = composite_root / (
        f"{output_name}_z{int(zoom)}_{safe_slug(args.metric)}_composite_metadata.json"
    )
    tif_path = legacy_tif_path if legacy_tif_path.exists() else short_tif_path
    metadata_path = (
        legacy_metadata_path if legacy_metadata_path.exists() else short_metadata_path
    )

    write_tiles_per_chunk = max(
        1, int(getattr(args, "geotiff_write_tiles_per_chunk", 64))
    )
    composite_block_size = validate_geotiff_block_size(
        getattr(args, "geotiff_block_size", 512)
    )
    composite_geotiff_settings = geotiff_output_settings(
        args,
        output_kind="composite",
        block_size=composite_block_size,
        write_tiles_per_chunk=write_tiles_per_chunk,
    )
    composite_geotiff_settings_hash = stable_json_hash(composite_geotiff_settings)

    if tif_path.exists() and metadata_path.exists():
        try:
            existing_meta = read_json(metadata_path)
            existing_ok = (
                int(existing_meta.get("zoom", -1)) == int(zoom)
                and str(existing_meta.get("metric")) == str(args.metric)
                and str(existing_meta.get("geotiff_settings_hash", ""))
                    == composite_geotiff_settings_hash
                and (file_size_bytes(tif_path) or 0) > 0
            )
        except Exception:
            existing_ok = False
        if existing_ok:
            print(
                f"Zoom {zoom}: reusing existing composite analytical GeoTIFF: {tif_path}"
            )
            return tif_path

    tif_path.unlink(missing_ok=True)
    metadata_path.unlink(missing_ok=True)

    limit = getattr(args, "value_tiff_tile_limit", None)
    rows_per_batch = int(getattr(args, "render_row_batch_size", 500_000))
    ntiles = 1 << int(zoom)
    coord_type = coordinate_sql_type_for_zoom(zoom)
    tile_type = "INTEGER" if ntiles - 1 <= np.iinfo(np.int32).max else "BIGINT"
    sql = f"""
    SELECT
        CAST(FLOOR(gx / {TILE_SIZE}) AS {tile_type}) AS tile_x,
        CAST(FLOOR(gy / {TILE_SIZE}) AS {tile_type}) AS tile_y,
        CAST(gx AS {coord_type}) AS gx,
        CAST(gy AS {coord_type}) AS gy,
        CAST(value AS FLOAT) AS value
    FROM {table_name}
    WHERE value > 0
    ORDER BY tile_y, tile_x
    """

    print(f"\nWriting vectorised composite analytical Float32 GeoTIFF for zoom {zoom}...")
    print(f"  output: {tif_path}")
    print(f"  tile range x: {min_tile_x} to {max_tile_x}")
    print(f"  tile range y: {min_tile_y} to {max_tile_y}")
    print(f"  raster size: {width:,} x {height:,} pixels")
    print(f"  non-zero rows: {pixel_count:,}")
    print(f"  Arrow batch size: {rows_per_batch:,}")
    print(f"  write geometry: contiguous horizontal strips, up to {write_tiles_per_chunk} tiles")
    print(
        f"  compression: {getattr(args, 'geotiff_compression', 'zstd')} "
        f"level {getattr(args, 'geotiff_compression_level', 1)}, "
        f"predictor={normalise_geotiff_predictor(getattr(args, 'geotiff_predictor', None))}, "
        f"threads={getattr(args, 'geotiff_num_threads', '1')}, "
        f"block={composite_block_size}x{composite_block_size}"
    )

    counters: defaultdict[str, float] = defaultdict(float)
    tiles_written = 0
    raster_write_calls = 0
    rows_seen = 0
    strip = np.zeros(
        (TILE_SIZE, TILE_SIZE * write_tiles_per_chunk), dtype=np.float32
    )
    strip_y: int | None = None
    strip_start_x: int | None = None
    strip_count = 0

    progress = tqdm(
        total=int(pixel_count), desc=f"composite GeoTIFF z{zoom}", unit="rows",
        unit_scale=True, dynamic_ncols=True,
    )
    context = (
        profiler.operation(
            "write_composite_geotiff",
            zoom=int(zoom),
            pixel_count=int(pixel_count),
            width=int(width),
            height=int(height),
        )
        if profiler is not None else null_profile_context()
    )

    with context as metrics:
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
            sparse_ok=True,
            **geotiff_creation_options(args, block_size=composite_block_size),
        ) as dst:
            dst.set_band_description(1, args.metric)
            dst.update_tags(
                metric=args.metric,
                zoom=str(int(zoom)),
                performance_implementation_version=PERFORMANCE_IMPLEMENTATION_VERSION,
                geotiff_writer_version=str(GEOTIFF_WRITER_VERSION),
                geotiff_write_strategy="contiguous_horizontal_strip",
                geotiff_write_tiles_per_chunk=str(write_tiles_per_chunk),
            )

            def flush_strip() -> None:
                nonlocal strip_y, strip_start_x, strip_count
                nonlocal tiles_written, raster_write_calls
                if strip_y is None or strip_start_x is None or strip_count <= 0:
                    return
                started = time.perf_counter()
                width_pixels = strip_count * TILE_SIZE
                window = Window(
                    col_off=(strip_start_x - min_tile_x) * TILE_SIZE,
                    row_off=(strip_y - min_tile_y) * TILE_SIZE,
                    width=width_pixels,
                    height=TILE_SIZE,
                )
                dst.write(strip[:, :width_pixels], 1, window=window)
                counters["raster_write_seconds"] += time.perf_counter() - started
                counters["raster_write_calls"] += 1
                tiles_written += strip_count
                raster_write_calls += 1
                strip[:, :width_pixels].fill(0.0)
                strip_y = None
                strip_start_x = None
                strip_count = 0

            try:
                for tile_x, tile_y, gxs, gys, values in iter_ordered_tile_groups(
                    con, sql, rows_per_batch, ntiles, counters, progress
                ):
                    if limit is not None and tiles_written + strip_count >= int(limit):
                        break
                    contiguous = (
                        strip_y == tile_y
                        and strip_start_x is not None
                        and tile_x == strip_start_x + strip_count
                        and strip_count < write_tiles_per_chunk
                    )
                    if strip_count > 0 and not contiguous:
                        flush_strip()
                    if strip_count == 0:
                        strip_y = int(tile_y)
                        strip_start_x = int(tile_x)

                    started = time.perf_counter()
                    px = (gxs - int(tile_x) * TILE_SIZE).astype(np.intp, copy=False)
                    py = (gys - int(tile_y) * TILE_SIZE).astype(np.intp, copy=False)
                    col_offset = strip_count * TILE_SIZE
                    strip[py, col_offset + px] = values.astype(np.float32, copy=False)
                    counters["numpy_pixel_placement_seconds"] += time.perf_counter() - started
                    strip_count += 1
                    rows_seen += int(values.size)

                    if strip_count >= write_tiles_per_chunk:
                        flush_strip()
                flush_strip()
            finally:
                progress.close()

        output_size = file_size_bytes(tif_path) or 0
        metrics.update(
            {
                "items": int(rows_seen),
                "item_unit": "rows",
                "bytes": int(output_size),
                "tiles_written": int(tiles_written),
                "raster_write_calls": int(raster_write_calls),
                "phase_seconds": dict(counters),
            }
        )

    metadata = {
        "output_name": output_name,
        "zoom": int(zoom),
        "tile_size": TILE_SIZE,
        "crs": "EPSG:3857",
        "metric": args.metric,
        "pixel_count": int(pixel_count),
        "min_tile_x": min_tile_x,
        "max_tile_x": max_tile_x,
        "min_tile_y": min_tile_y,
        "max_tile_y": max_tile_y,
        "width_pixels": int(width),
        "height_pixels": int(height),
        "resolution_m_at_equator": float(res),
        "nodata": 0.0,
        "rows_streamed": int(rows_seen),
        "tiles_written": int(tiles_written),
        "raster_write_calls": int(raster_write_calls),
        "byte_size": file_size_bytes(tif_path),
        "phase_seconds": dict(counters),
        "compression": getattr(args, "geotiff_compression", "zstd"),
        "compression_level": getattr(args, "geotiff_compression_level", 1),
        "compression_predictor": normalise_geotiff_predictor(
            getattr(args, "geotiff_predictor", None)
        ),
        "compression_threads": getattr(args, "geotiff_num_threads", "1"),
        "block_size": int(composite_block_size),
        "write_strategy": "contiguous_horizontal_strip",
        "write_tiles_per_chunk": int(write_tiles_per_chunk),
        "geotiff_settings": composite_geotiff_settings,
        "geotiff_settings_hash": composite_geotiff_settings_hash,
        "performance_implementation_version": PERFORMANCE_IMPLEMENTATION_VERSION,
    }
    metadata["path_layout_version"] = PATH_LAYOUT_VERSION
    atomic_write_json(metadata_path, _json_safe(metadata))
    print(
        f"Zoom {zoom}: streamed {rows_seen:,} rows, wrote {tiles_written:,} tiles "
        f"in {raster_write_calls:,} GDAL calls: {tif_path} "
        f"({format_bytes(file_size_bytes(tif_path))})"
    )
    print("Composite GeoTIFF phase breakdown:")
    for name, seconds in sorted(counters.items(), key=lambda item: item[1], reverse=True):
        if name.endswith("_seconds"):
            print(f"  {name}: {format_duration(seconds)}")
    return tif_path



def export_value_outputs(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    zoom: int,
    args: argparse.Namespace,
    output_dir: Path,
    output_name: str,
    pixel_count: int,
    work_dir: Path,
) -> None:
    if not should_export_value_zoom(args, zoom):
        return
    profiler = get_profiler(args)
    if getattr(args, "write_value_pixels", False):
        export_value_pixels_parquet(
            con=con,
            table_name=table_name,
            zoom=zoom,
            args=args,
            output_dir=output_dir,
            output_name=output_name,
            pixel_count=pixel_count,
            work_dir=work_dir,
            profiler=profiler,
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
    """Colourise only occupied pixels; transparent background pixels are untouched."""
    rgba = np.zeros((TILE_SIZE, TILE_SIZE, 4), dtype=np.uint8)
    if vmax <= 0 or not np.isfinite(vmax):
        return rgba

    flat_density = np.asarray(density, dtype=np.float32).reshape(-1)
    occupied = np.flatnonzero(np.isfinite(flat_density) & (flat_density > 0))
    if occupied.size == 0:
        return rgba

    values = flat_density[occupied]
    norm = np.log1p(values) / math.log1p(vmax)
    norm = np.clip(norm, 0.0, 1.0).astype(np.float32, copy=False)
    flat_rgba = rgba.reshape(-1, 4)
    flat_rgba[occupied, 0] = np.interp(
        norm, COLOUR_STOPS, COLOUR_REDS
    ).astype(np.uint8)
    flat_rgba[occupied, 1] = np.interp(
        norm, COLOUR_STOPS, COLOUR_GREENS
    ).astype(np.uint8)
    flat_rgba[occupied, 2] = np.interp(
        norm, COLOUR_STOPS, COLOUR_BLUES
    ).astype(np.uint8)
    alpha = alpha_min + (alpha_max - alpha_min) * np.power(norm, 0.75)
    flat_rgba[occupied, 3] = np.clip(alpha, 0, alpha_max).astype(np.uint8)
    return rgba


def png_bytes_from_rgba(
    rgba: np.ndarray,
    compress_level: int = 1,
    optimize: bool = False,
) -> bytes:
    image = Image.fromarray(rgba)
    buffer = io.BytesIO()
    image.save(
        buffer,
        format="PNG",
        optimize=bool(optimize),
        compress_level=max(0, min(9, int(compress_level))),
    )
    return buffer.getvalue()



def init_mbtiles(
    path: Path,
    name: str,
    min_zoom: int,
    max_zoom: int,
    cache_mb: int = 256,
) -> sqlite3.Connection:
    if path.exists():
        path.unlink()
    conn = sqlite3.connect(path.as_posix(), timeout=60.0)
    conn.execute("PRAGMA page_size=65536")
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA locking_mode=EXCLUSIVE")
    conn.execute(f"PRAGMA cache_size=-{max(1, int(cache_mb)) * 1024}")
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
        "CREATE UNIQUE INDEX tile_index ON tiles (zoom_level, tile_column, tile_row)"
    )
    metadata = {
        "name": name,
        "type": "overlay",
        "version": "1.0",
        "description": "AIS pre-aggregated track-density raster heatmap",
        "format": "png",
        "minzoom": str(min_zoom),
        "maxzoom": str(max_zoom),
        "bounds": "-180.0,-85.05112878,180.0,85.05112878",
        "center": "0.0,0.0,2",
    }
    conn.executemany("INSERT INTO metadata (name, value) VALUES (?, ?)", metadata.items())
    conn.commit()
    return conn




def open_or_init_mbtiles(
    path: Path,
    name: str,
    min_zoom: int,
    max_zoom: int,
    resume_existing: bool,
    cache_mb: int = 256,
) -> sqlite3.Connection:
    if not path.exists() or not resume_existing:
        return init_mbtiles(path, name, min_zoom, max_zoom, cache_mb=cache_mb)
    conn = sqlite3.connect(path.as_posix(), timeout=60.0)
    required_tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    if not {"metadata", "tiles"}.issubset(required_tables):
        conn.close()
        raise RuntimeError(f"Existing MBTiles file has an invalid schema: {path}")
    saved_name_row = conn.execute(
        "SELECT value FROM metadata WHERE name='name' LIMIT 1"
    ).fetchone()
    if saved_name_row is not None and str(saved_name_row[0]) != str(name):
        conn.close()
        raise RuntimeError(
            f"Existing MBTiles name {saved_name_row[0]!r} does not match "
            f"requested output name {name!r}."
        )
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA locking_mode=EXCLUSIVE")
    conn.execute(f"PRAGMA cache_size=-{max(1, int(cache_mb)) * 1024}")
    print(f"Reusing existing MBTiles database: {path}")
    return conn



def xyz_progress_path(state_dir: Path, zoom: int) -> Path:
    return state_dir / "xyz_progress" / f"zoom_{int(zoom):02d}.json"


def clear_incomplete_xyz_output(
    zoom: int,
    xyz_root: Path | None,
    state_dir: Path | None = None,
) -> None:
    """Remove only the derived XYZ output and its extraction checkpoint."""
    if xyz_root is not None:
        zoom_dir = xyz_root / str(int(zoom))
        if zoom_dir.exists():
            shutil.rmtree(zoom_dir, ignore_errors=True)
    if state_dir is not None:
        remove_path_quietly(xyz_progress_path(state_dir, zoom))


def clear_incomplete_mbtiles_zoom(
    zoom: int,
    mbtiles_conn: sqlite3.Connection | None,
) -> None:
    """Remove one incomplete canonical MBTiles zoom."""
    if mbtiles_conn is not None:
        mbtiles_conn.execute(
            "DELETE FROM tiles WHERE zoom_level = ?",
            [int(zoom)],
        )
        mbtiles_conn.commit()


def clear_incomplete_zoom_outputs(
    zoom: int,
    xyz_root: Path | None,
    mbtiles_conn: sqlite3.Connection | None,
    state_dir: Path | None = None,
) -> None:
    """Backward-compatible helper that invalidates both visual products."""
    clear_incomplete_xyz_output(zoom, xyz_root, state_dir=state_dir)
    clear_incomplete_mbtiles_zoom(zoom, mbtiles_conn)


def total_completed_tiles(
    manager: RunStateManager,
    min_zoom: int,
    max_zoom: int,
) -> int:
    total = 0
    for zoom in range(int(min_zoom), int(max_zoom) + 1):
        marker = read_valid_completion_marker(
            manager.render_marker_path(zoom),
            manager.render_fingerprint,
            marker_type="render_complete",
        )
        if marker is not None:
            total += int(marker.get("tiles_written", 0))
    return total

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


def iter_ordered_tile_groups(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    rows_per_batch: int,
    ntiles: int,
    counters: MutableMapping[str, float] | None = None,
    progress: tqdm | None = None,
) -> Iterator[tuple[int, int, np.ndarray, np.ndarray, np.ndarray]]:
    """Yield contiguous tile groups from a tile-ordered DuckDB Arrow stream."""
    if counters is None:
        counters = defaultdict(float)

    started = time.perf_counter()
    reader = con.execute(sql).fetch_record_batch(rows_per_batch=int(rows_per_batch))
    counters["duckdb_query_start_seconds"] += time.perf_counter() - started

    pending_key: int | None = None
    pending_gx: list[np.ndarray] = []
    pending_gy: list[np.ndarray] = []
    pending_values: list[np.ndarray] = []

    while True:
        started = time.perf_counter()
        try:
            batch = reader.read_next_batch()
        except StopIteration:
            counters["duckdb_batch_fetch_seconds"] += time.perf_counter() - started
            break
        counters["duckdb_batch_fetch_seconds"] += time.perf_counter() - started
        if batch is None or batch.num_rows == 0:
            break

        started = time.perf_counter()
        names = batch.schema.names
        idx = {name: names.index(name) for name in names}
        tile_dtype = (
            np.int32 if int(ntiles) - 1 <= np.iinfo(np.int32).max else np.int64
        )
        coord_dtype = coordinate_numpy_dtype_for_world(int(ntiles) * TILE_SIZE)
        tile_xs = np.asarray(
            batch.column(idx["tile_x"]).to_numpy(zero_copy_only=False),
            dtype=tile_dtype,
        )
        tile_ys = np.asarray(
            batch.column(idx["tile_y"]).to_numpy(zero_copy_only=False),
            dtype=tile_dtype,
        )
        gxs = np.asarray(
            batch.column(idx["gx"]).to_numpy(zero_copy_only=False),
            dtype=coord_dtype,
        )
        gys = np.asarray(
            batch.column(idx["gy"]).to_numpy(zero_copy_only=False),
            dtype=coord_dtype,
        )
        values = np.asarray(
            batch.column(idx["value"]).to_numpy(zero_copy_only=False),
            dtype=np.float32,
        )
        tile_keys = (
            tile_ys.astype(np.int64, copy=False) * int(ntiles)
            + tile_xs.astype(np.int64, copy=False)
        )
        starts = np.concatenate(
            (
                np.array([0], dtype=np.int64),
                np.flatnonzero(tile_keys[1:] != tile_keys[:-1]).astype(np.int64) + 1,
            )
        )
        ends = np.concatenate((starts[1:], np.array([batch.num_rows], dtype=np.int64)))
        counters["arrow_to_numpy_and_group_seconds"] += time.perf_counter() - started
        counters["rows_streamed"] += int(batch.num_rows)
        counters["batches_streamed"] += 1
        if progress is not None:
            progress.update(batch.num_rows)

        for start, end in zip(starts, ends):
            start_i = int(start)
            end_i = int(end)
            key = int(tile_keys[start_i])
            if pending_key is not None and key != pending_key:
                out_gx = pending_gx[0] if len(pending_gx) == 1 else np.concatenate(pending_gx)
                out_gy = pending_gy[0] if len(pending_gy) == 1 else np.concatenate(pending_gy)
                out_values = (
                    pending_values[0]
                    if len(pending_values) == 1
                    else np.concatenate(pending_values)
                )
                yield (
                    int(pending_key % int(ntiles)),
                    int(pending_key // int(ntiles)),
                    out_gx,
                    out_gy,
                    out_values,
                )
                pending_gx = []
                pending_gy = []
                pending_values = []

            pending_key = key
            pending_gx.append(gxs[start_i:end_i])
            pending_gy.append(gys[start_i:end_i])
            pending_values.append(values[start_i:end_i])

    if pending_key is not None:
        out_gx = pending_gx[0] if len(pending_gx) == 1 else np.concatenate(pending_gx)
        out_gy = pending_gy[0] if len(pending_gy) == 1 else np.concatenate(pending_gy)
        out_values = (
            pending_values[0]
            if len(pending_values) == 1
            else np.concatenate(pending_values)
        )
        yield (
            int(pending_key % int(ntiles)),
            int(pending_key // int(ntiles)),
            out_gx,
            out_gy,
            out_values,
        )



def tile_expanded_pixel_sql(table_name: str, zoom: int, radius: int) -> str:
    """
    Stream pixels by destination tile while duplicating only edge pixels.

    The previous 3 x 3 cross join generated nine candidates for every source
    pixel and filtered most of them afterwards. Conditional UNNEST lists emit one
    candidate for interior pixels, two along an edge and four at a corner.
    """
    zoom = int(zoom)
    radius = int(radius)
    if radius < 0 or radius >= TILE_SIZE // 2:
        raise ValueError(f"blur_radius must be between 0 and {TILE_SIZE // 2 - 1}")
    ntiles = 1 << zoom
    world_px = TILE_SIZE * ntiles
    coord_type = coordinate_sql_type_for_zoom(zoom)
    tile_type = "INTEGER" if ntiles - 1 <= np.iinfo(np.int32).max else "BIGINT"
    if radius == 0:
        return f"""
        SELECT
            CAST(FLOOR(gx / {TILE_SIZE}) AS {tile_type}) AS tile_x,
            CAST(FLOOR(gy / {TILE_SIZE}) AS {tile_type}) AS tile_y,
            CAST(gx AS {coord_type}) AS gx,
            CAST(gy AS {coord_type}) AS gy,
            CAST(value AS FLOAT) AS value
        FROM {table_name}
        WHERE value > 0
        ORDER BY tile_y, tile_x
        """

    return f"""
    WITH base AS (
        SELECT
            CAST(gx AS {coord_type}) AS gx,
            CAST(gy AS {coord_type}) AS gy,
            CAST(value AS FLOAT) AS value,
            CAST(FLOOR(gx / {TILE_SIZE}) AS BIGINT) AS base_tile_x,
            CAST(FLOOR(gy / {TILE_SIZE}) AS BIGINT) AS base_tile_y,
            CAST(gx % {TILE_SIZE} AS INTEGER) AS local_x,
            CAST(gy % {TILE_SIZE} AS INTEGER) AS local_y
        FROM {table_name}
        WHERE value > 0
    ),
    expanded AS (
        SELECT
            base.gx,
            base.gy,
            base.value,
            base.base_tile_x + oxs.ox AS raw_tile_x,
            base.base_tile_y + oys.oy AS raw_tile_y
        FROM base
        CROSS JOIN UNNEST(
            CASE
                WHEN local_x < {radius} THEN [0, -1]
                WHEN local_x >= {TILE_SIZE - radius} THEN [0, 1]
                ELSE [0]
            END
        ) AS oxs(ox)
        CROSS JOIN UNNEST(
            CASE
                WHEN local_y < {radius} THEN [0, -1]
                WHEN local_y >= {TILE_SIZE - radius} THEN [0, 1]
                ELSE [0]
            END
        ) AS oys(oy)
    )
    SELECT
        CAST(((raw_tile_x + {ntiles}) % {ntiles}) AS {tile_type}) AS tile_x,
        CAST(raw_tile_y AS {tile_type}) AS tile_y,
        CAST(
            gx + CASE
                WHEN raw_tile_x < 0 THEN {world_px}
                WHEN raw_tile_x >= {ntiles} THEN -{world_px}
                ELSE 0
            END AS {coord_type}
        ) AS gx,
        CAST(gy AS {coord_type}) AS gy,
        value
    FROM expanded
    WHERE raw_tile_y BETWEEN 0 AND {ntiles - 1}
    ORDER BY tile_y, tile_x
    """


def legacy_tile_expanded_pixel_sql(table_name: str, zoom: int, radius: int) -> str:
    """Compatibility fallback using the previous VALUES cross-join SQL.

    This form is less efficient because every source pixel is expanded to all
    neighbouring tile candidates before filtering. It is retained so older
    DuckDB versions can still render if conditional list UNNEST is unsupported.
    """
    zoom = int(zoom)
    radius = int(radius)
    ntiles = 1 << zoom
    world_px = TILE_SIZE * ntiles
    coord_type = coordinate_sql_type_for_zoom(zoom)
    tile_type = "INTEGER" if ntiles - 1 <= np.iinfo(np.int32).max else "BIGINT"
    offset_span = max(0, int(math.ceil(max(radius, 0) / TILE_SIZE)))
    offsets = ",".join(f"({i})" for i in range(-offset_span, offset_span + 1))

    if radius <= 0:
        return f"""
        SELECT
            CAST(FLOOR(gx / {TILE_SIZE}) AS {tile_type}) AS tile_x,
            CAST(FLOOR(gy / {TILE_SIZE}) AS {tile_type}) AS tile_y,
            CAST(gx AS {coord_type}) AS gx,
            CAST(gy AS {coord_type}) AS gy,
            CAST(value AS FLOAT) AS value
        FROM {table_name}
        WHERE value > 0
        ORDER BY tile_y, tile_x
        """

    return f"""
    WITH base AS (
        SELECT
            CAST(gx AS {coord_type}) AS gx,
            CAST(gy AS {coord_type}) AS gy,
            CAST(value AS FLOAT) AS value,
            CAST(FLOOR(gx / {TILE_SIZE}) AS BIGINT) AS base_tile_x,
            CAST(FLOOR(gy / {TILE_SIZE}) AS BIGINT) AS base_tile_y
        FROM {table_name}
        WHERE value > 0
    ),
    expanded AS (
        SELECT
            base.gx,
            base.gy,
            base.value,
            base.base_tile_x + oxs.ox AS raw_tile_x,
            base.base_tile_y + oys.oy AS raw_tile_y
        FROM base
        CROSS JOIN (VALUES {offsets}) AS oxs(ox)
        CROSS JOIN (VALUES {offsets}) AS oys(oy)
    ),
    candidates AS (
        SELECT
            CAST(((raw_tile_x + {ntiles}) % {ntiles}) AS {tile_type}) AS tile_x,
            CAST(raw_tile_y AS {tile_type}) AS tile_y,
            CAST(
                gx + CASE
                    WHEN raw_tile_x < 0 THEN {world_px}
                    WHEN raw_tile_x >= {ntiles} THEN -{world_px}
                    ELSE 0
                END AS {coord_type}
            ) AS gx,
            CAST(gy AS {coord_type}) AS gy,
            value
        FROM expanded
        WHERE raw_tile_y BETWEEN 0 AND {ntiles - 1}
    )
    SELECT tile_x, tile_y, gx, gy, value
    FROM candidates
    WHERE gx BETWEEN tile_x * {TILE_SIZE} - {radius}
                 AND (tile_x + 1) * {TILE_SIZE} - 1 + {radius}
      AND gy BETWEEN tile_y * {TILE_SIZE} - {radius}
                 AND (tile_y + 1) * {TILE_SIZE} - 1 + {radius}
    ORDER BY tile_y, tile_x
    """


@contextmanager
def temporary_duckdb_threads(
    con: duckdb.DuckDBPyConnection,
    requested_threads: int | None,
) -> Iterator[None]:
    """Temporarily apply a stage-specific DuckDB thread count and restore it."""
    if requested_threads is None or int(requested_threads) <= 0:
        yield
        return
    requested = max(1, int(requested_threads))
    try:
        current = int(con.execute("SELECT current_setting('threads')").fetchone()[0])
    except Exception:
        current = requested
    changed = current != requested
    if changed:
        con.execute(f"SET threads TO {requested}")
    try:
        yield
    finally:
        if changed:
            con.execute(f"SET threads TO {current}")


def render_cache_paths(work_dir: Path, zoom: int, band_rows: int) -> tuple[Path, Path]:
    root = work_dir / f"render_source_z{int(zoom)}_b{int(band_rows)}"
    return root, root / STAGE_MARKER_FILENAME


def render_cache_config_hash(
    analytical_hash: str | StageFingerprint | Iterable[str],
    zoom: int,
    band_rows: int,
) -> str:
    return stable_json_hash(
        {
            "render_source_cache_version": RENDER_SOURCE_CACHE_VERSION,
            "analytical_hash": primary_hash(analytical_hash),
            "zoom": int(zoom),
            "tile_size": TILE_SIZE,
            "partition_band_rows": int(band_rows),
            "schema": ["base_tile_x", "base_tile_y", "local_x", "local_y", "value"],
        }
    )


def build_or_reuse_render_cache(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    zoom: int,
    pixel_count: int,
    args: argparse.Namespace,
    work_dir: Path,
    analytical_hash: str | StageFingerprint | Iterable[str],
    profiler: StageProfiler | None = None,
) -> RenderSourceCache:
    """Write a compact base-pixel cache partitioned by tile-row band.

    No global sort is performed. Each analytical pixel is written exactly once as
    a compact tile/local-pixel record. Rendering later reads only one destination
    band plus its one-tile blur halo, so z11 sort memory remains bounded.
    """
    band_rows = max(
        1, int(getattr(args, "render_cache_partition_band_rows", 64))
    )
    root, marker_path = render_cache_paths(work_dir, zoom, band_rows)
    cache_hash = render_cache_config_hash(analytical_hash, zoom, band_rows)
    marker = read_valid_completion_marker(
        marker_path,
        cache_hash,
        marker_type="render_source_cache_complete",
    )
    existing_files = sorted(root.glob("render_band=*/*.parquet")) if root.exists() else []
    if marker is not None:
        expected_files = int(marker.get("parquet_file_count", -1))
        if expected_files == len(existing_files) and expected_files > 0:
            print(
                f"Zoom {zoom}: reusing banded render cache "
                f"({expected_files:,} files, {band_rows} tile rows/cache partition, "
                f"{format_bytes(int(marker.get('byte_size', 0)))})."
            )
            return RenderSourceCache(
                root=root,
                cache_hash=cache_hash,
                band_tile_rows=band_rows,
                min_base_tile_y=int(marker["min_base_tile_y"]),
                max_base_tile_y=int(marker["max_base_tile_y"]),
                parquet_file_count=expected_files,
                byte_size=int(marker.get("byte_size", 0)),
            )

    if root.exists():
        shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True, exist_ok=True)

    bounds = con.execute(
        f"""
        SELECT
            MIN(CAST(FLOOR(gy / {TILE_SIZE}) AS BIGINT)),
            MAX(CAST(FLOOR(gy / {TILE_SIZE}) AS BIGINT))
        FROM {table_name}
        WHERE value > 0
        """
    ).fetchone()
    if bounds is None or bounds[0] is None:
        raise RuntimeError(f"Zoom {zoom}: cannot build a render cache for an empty table.")
    min_tile_y, max_tile_y = int(bounds[0]), int(bounds[1])

    compression = str(getattr(args, "render_tile_parts_compression", "zstd")).upper()
    level = max(
        1,
        min(22, int(getattr(args, "render_tile_parts_compression_level", 1))),
    )
    select_sql = f"""
        SELECT
            CAST(FLOOR(gx / {TILE_SIZE}) AS INTEGER) AS base_tile_x,
            CAST(FLOOR(gy / {TILE_SIZE}) AS INTEGER) AS base_tile_y,
            CAST(gx % {TILE_SIZE} AS USMALLINT) AS local_x,
            CAST(gy % {TILE_SIZE} AS USMALLINT) AS local_y,
            CAST("value" AS FLOAT) AS "value",
            CAST(FLOOR(FLOOR(gy / {TILE_SIZE}) / {band_rows}) AS INTEGER)
                AS render_band
        FROM {table_name}
        WHERE "value" > 0
    """
    context = (
        profiler.operation(
            "build_banded_render_source_cache",
            zoom=int(zoom),
            pixel_count=int(pixel_count),
            band_rows=int(band_rows),
        )
        if profiler is not None
        else null_profile_context()
    )
    with context as metrics:
        fast_sql = f"""
        COPY ({select_sql})
        TO {sql_str(root.as_posix())}
        (
            FORMAT PARQUET,
            COMPRESSION {compression},
            COMPRESSION_LEVEL {level},
            PARTITION_BY (render_band)
        );
        """
        try:
            con.execute(fast_sql)
            compression_mode = f"{compression.lower()}_level_{level}"
        except Exception as exc:
            message = str(exc).lower()
            compatibility_error = (
                "compression_level" in message
                or "unrecognized option" in message
                or "unknown option" in message
            )
            if not compatibility_error:
                raise
            shutil.rmtree(root, ignore_errors=True)
            root.mkdir(parents=True, exist_ok=True)
            con.execute(
                f"""
                COPY ({select_sql})
                TO {sql_str(root.as_posix())}
                (FORMAT PARQUET, COMPRESSION {compression}, PARTITION_BY (render_band));
                """
            )
            compression_mode = f"{compression.lower()}_default_fallback"

        files = sorted(root.glob("render_band=*/*.parquet"))
        if not files:
            raise RuntimeError(f"Zoom {zoom}: banded render cache produced no files.")
        row_count = parquet_files_row_count(files)
        byte_size = sum(file_size_bytes(path) or 0 for path in files)
        metrics.update(
            {
                "items": int(row_count),
                "item_unit": "pixels",
                "bytes": int(byte_size),
                "parquet_file_count": len(files),
                "compression_mode": compression_mode,
            }
        )

    if int(row_count) != int(pixel_count):
        raise RuntimeError(
            f"Zoom {zoom}: render cache row count {row_count:,} does not match "
            f"analytical pixel count {pixel_count:,}."
        )
    write_completion_marker(
        marker_path,
        marker_type="render_source_cache_complete",
        config_hash=cache_hash,
        render_source_cache_version=RENDER_SOURCE_CACHE_VERSION,
        zoom=int(zoom),
        pixel_count=int(pixel_count),
        min_base_tile_y=int(min_tile_y),
        max_base_tile_y=int(max_tile_y),
        cache_partition_tile_rows=int(band_rows),
        band_tile_rows=int(band_rows),
        parquet_file_count=len(files),
        byte_size=int(byte_size),
        compression_mode=compression_mode,
    )
    print(
        f"Zoom {zoom}: built banded render cache: {root} "
        f"({len(files):,} files, {format_bytes(byte_size)}, "
        f"{band_rows} tile rows/cache partition)."
    )
    return RenderSourceCache(
        root=root,
        cache_hash=cache_hash,
        band_tile_rows=band_rows,
        min_base_tile_y=min_tile_y,
        max_base_tile_y=max_tile_y,
        parquet_file_count=len(files),
        byte_size=byte_size,
    )


def render_cache_band_files(cache_root: Path, band_ids: Iterable[int]) -> list[Path]:
    files: list[Path] = []
    for band_id in sorted(set(int(value) for value in band_ids if int(value) >= 0)):
        band_dir = cache_root / f"render_band={band_id}"
        if band_dir.exists():
            files.extend(sorted(band_dir.glob("*.parquet")))
    return files


def banded_tile_expanded_pixel_sql(
    source_files: list[Path],
    zoom: int,
    radius: int,
    target_tile_y_min: int,
    target_tile_y_max: int,
    use_conditional_unnest: bool = True,
) -> str:
    """Return a locally bounded tile-order query for one destination Y band."""
    if not source_files:
        raise ValueError("source_files must not be empty")
    zoom = int(zoom)
    radius = int(radius)
    ntiles = 1 << zoom
    world_px = TILE_SIZE * ntiles
    coord_type = coordinate_sql_type_for_zoom(zoom)
    tile_type = "INTEGER" if ntiles - 1 <= np.iinfo(np.int32).max else "BIGINT"
    source_sql = "[" + ",".join(sql_str(path.as_posix()) for path in source_files) + "]"
    source_tile_y_min = max(0, int(target_tile_y_min) - (1 if radius > 0 else 0))
    source_tile_y_max = min(
        ntiles - 1,
        int(target_tile_y_max) + (1 if radius > 0 else 0),
    )

    base_sql = f"""
        SELECT
            CAST(base_tile_x AS BIGINT) AS base_tile_x,
            CAST(base_tile_y AS BIGINT) AS base_tile_y,
            CAST(local_x AS INTEGER) AS local_x,
            CAST(local_y AS INTEGER) AS local_y,
            CAST(base_tile_x * {TILE_SIZE} + local_x AS {coord_type}) AS gx,
            CAST(base_tile_y * {TILE_SIZE} + local_y AS {coord_type}) AS gy,
            CAST("value" AS FLOAT) AS "value"
        FROM read_parquet({source_sql}, union_by_name=true)
        WHERE base_tile_y BETWEEN {source_tile_y_min} AND {source_tile_y_max}
          AND "value" > 0
    """
    if radius <= 0:
        return f"""
        WITH base AS ({base_sql})
        SELECT
            CAST(base_tile_x AS {tile_type}) AS tile_x,
            CAST(base_tile_y AS {tile_type}) AS tile_y,
            gx,
            gy,
            value
        FROM base
        WHERE base_tile_y BETWEEN {int(target_tile_y_min)} AND {int(target_tile_y_max)}
        ORDER BY tile_y, tile_x
        """

    if use_conditional_unnest:
        expanded_sql = f"""
        SELECT
            base.gx,
            base.gy,
            base.value,
            base.base_tile_x + oxs.ox AS raw_tile_x,
            base.base_tile_y + oys.oy AS raw_tile_y
        FROM base
        CROSS JOIN UNNEST(
            CASE
                WHEN local_x < {radius} THEN [0, -1]
                WHEN local_x >= {TILE_SIZE - radius} THEN [0, 1]
                ELSE [0]
            END
        ) AS oxs(ox)
        CROSS JOIN UNNEST(
            CASE
                WHEN local_y < {radius} THEN [0, -1]
                WHEN local_y >= {TILE_SIZE - radius} THEN [0, 1]
                ELSE [0]
            END
        ) AS oys(oy)
        """
    else:
        expanded_sql = f"""
        SELECT
            base.gx,
            base.gy,
            base.value,
            base.base_tile_x + oxs.ox AS raw_tile_x,
            base.base_tile_y + oys.oy AS raw_tile_y
        FROM base
        CROSS JOIN (VALUES (-1), (0), (1)) AS oxs(ox)
        CROSS JOIN (VALUES (-1), (0), (1)) AS oys(oy)
        WHERE (
            oxs.ox = 0
            OR (oxs.ox = -1 AND local_x < {radius})
            OR (oxs.ox = 1 AND local_x >= {TILE_SIZE - radius})
        )
          AND (
            oys.oy = 0
            OR (oys.oy = -1 AND local_y < {radius})
            OR (oys.oy = 1 AND local_y >= {TILE_SIZE - radius})
          )
        """

    return f"""
    WITH base AS ({base_sql}),
    expanded AS ({expanded_sql})
    SELECT
        CAST(((raw_tile_x + {ntiles}) % {ntiles}) AS {tile_type}) AS tile_x,
        CAST(raw_tile_y AS {tile_type}) AS tile_y,
        CAST(
            gx + CASE
                WHEN raw_tile_x < 0 THEN {world_px}
                WHEN raw_tile_x >= {ntiles} THEN -{world_px}
                ELSE 0
            END AS {coord_type}
        ) AS gx,
        CAST(gy AS {coord_type}) AS gy,
        value
    FROM expanded
    WHERE raw_tile_y BETWEEN {int(target_tile_y_min)} AND {int(target_tile_y_max)}
    ORDER BY tile_y, tile_x
    """


def render_band_marker_root(state_dir: Path, zoom: int) -> Path:
    return state_dir / "render_bands" / f"zoom_{int(zoom):02d}"


def render_band_marker_path(state_dir: Path, zoom: int, band_id: int) -> Path:
    return render_band_marker_root(state_dir, zoom) / f"band_{int(band_id):05d}.json"


def render_band_execution_hash(
    render_hash: str | StageFingerprint | Iterable[str],
    zoom: int,
    radius: int,
    target_band_rows: int,
) -> str:
    """Fingerprint output/checkpoint boundaries, excluding cache-only layout."""
    return stable_json_hash(
        {
            "version": RENDER_BAND_EXECUTION_VERSION,
            "render_hash": primary_hash(render_hash),
            "zoom": int(zoom),
            "radius": int(radius),
            "target_band_rows": int(target_band_rows),
            "tile_expansion_semantics": "edge_only_exact_v1",
        }
    )


_TILE_RENDER_TLS = threading.local()


def _thread_render_buffers(radius: int, render_mode: str) -> dict[str, Any]:
    key = (int(radius), str(render_mode))
    state = getattr(_TILE_RENDER_TLS, "state", None)
    if state is not None and state.get("key") == key:
        return state
    radius = int(radius)
    state = {"key": key}
    if radius == 0:
        state["density"] = np.zeros((TILE_SIZE, TILE_SIZE), dtype=np.float32)
    elif render_mode == "vectorized_scipy_blur":
        padded = TILE_SIZE + 2 * radius
        state["impulse"] = np.zeros((padded, padded), dtype=np.float32)
        state["blurred"] = np.empty((padded, padded), dtype=np.float32)
    else:
        state["density"] = np.zeros((TILE_SIZE, TILE_SIZE), dtype=np.float32)
    _TILE_RENDER_TLS.state = state
    return state


def render_sparse_tile_to_png(
    tile_x: int,
    tile_y: int,
    gxs: np.ndarray,
    gys: np.ndarray,
    values: np.ndarray,
    radius: int,
    render_mode: str,
    kernel: np.ndarray | None,
    vmax: float,
    alpha_min: int,
    alpha_max: int,
    png_level: int,
    png_optimize: bool,
) -> tuple[int, int, bytes | None, int, dict[str, float]]:
    """Whole-tile worker: placement, blur, colourisation, and PNG encoding."""
    timings: defaultdict[str, float] = defaultdict(float)
    state = _thread_render_buffers(radius, render_mode)
    radius = int(radius)
    input_rows = int(values.size)

    started = time.perf_counter()
    if radius == 0:
        density = state["density"]
        local_x = (gxs - int(tile_x) * TILE_SIZE).astype(np.intp, copy=False)
        local_y = (gys - int(tile_y) * TILE_SIZE).astype(np.intp, copy=False)
        valid = (
            (local_x >= 0) & (local_x < TILE_SIZE)
            & (local_y >= 0) & (local_y < TILE_SIZE)
        )
        local_x = local_x[valid]
        local_y = local_y[valid]
        local_values = values[valid].astype(np.float32, copy=False)
        density[local_y, local_x] = local_values
        touched_x, touched_y = local_x, local_y
    elif render_mode == "vectorized_scipy_blur":
        impulse = state["impulse"]
        local_x = (
            gxs - int(tile_x) * TILE_SIZE + radius
        ).astype(np.intp, copy=False)
        local_y = (
            gys - int(tile_y) * TILE_SIZE + radius
        ).astype(np.intp, copy=False)
        valid = (
            (local_x >= 0) & (local_x < impulse.shape[1])
            & (local_y >= 0) & (local_y < impulse.shape[0])
        )
        local_x = local_x[valid]
        local_y = local_y[valid]
        local_values = values[valid].astype(np.float32, copy=False)
        impulse[local_y, local_x] = local_values
        touched_x, touched_y = local_x, local_y
        density = None
    else:
        density = state["density"]
        density.fill(0.0)
        if kernel is None:
            raise RuntimeError("Legacy blur rendering requires a kernel.")
        for gx, gy, value in zip(gxs, gys, values):
            splat_value(
                density,
                int(gx) - int(tile_x) * TILE_SIZE,
                int(gy) - int(tile_y) * TILE_SIZE,
                float(value),
                kernel,
                radius,
            )
        touched_x = touched_y = None
    timings["tile_pixel_placement_cpu_seconds_sum"] += time.perf_counter() - started

    if radius > 0 and render_mode == "vectorized_scipy_blur":
        if scipy_ndimage_convolve is None or kernel is None:
            raise RuntimeError("SciPy convolution is unavailable for vectorized blur.")
        started = time.perf_counter()
        scipy_ndimage_convolve(
            state["impulse"],
            kernel,
            output=state["blurred"],
            mode="constant",
            cval=0.0,
        )
        density = state["blurred"][
            radius : radius + TILE_SIZE,
            radius : radius + TILE_SIZE,
        ]
        state["impulse"][touched_y, touched_x] = 0.0
        timings["convolution_cpu_seconds_sum"] += time.perf_counter() - started

    started = time.perf_counter()
    rgba = colourise_density(
        density=density,
        vmax=float(vmax),
        alpha_min=int(alpha_min),
        alpha_max=int(alpha_max),
    )
    timings["colourise_cpu_seconds_sum"] += time.perf_counter() - started

    if radius == 0:
        state["density"][touched_y, touched_x] = 0.0

    if rgba[:, :, 3].max() == 0:
        return int(tile_x), int(tile_y), None, input_rows, dict(timings)

    started = time.perf_counter()
    png_data = png_bytes_from_rgba(
        rgba,
        compress_level=int(png_level),
        optimize=bool(png_optimize),
    )
    timings["png_encode_cpu_seconds_sum"] += time.perf_counter() - started
    return int(tile_x), int(tile_y), png_data, input_rows, dict(timings)


def _accumulate_worker_result(
    result: tuple[int, int, bytes | None, int, dict[str, float]],
    *,
    zoom: int,
    ntiles: int,
    mbtiles_rows: list[tuple[int, int, int, bytes]],
    counters: MutableMapping[str, float],
) -> tuple[int, int, int]:
    tile_x, tile_y, png_data, input_rows, timings = result
    for name, seconds in timings.items():
        counters[name] += float(seconds)
    if png_data is None:
        return 0, 0, int(input_rows)
    tms_y = int(ntiles) - 1 - int(tile_y)
    mbtiles_rows.append((int(zoom), int(tile_x), int(tms_y), png_data))
    return 1, len(png_data), int(input_rows)


def _delete_mbtiles_xyz_y_band(
    mbtiles_conn: sqlite3.Connection,
    zoom: int,
    xyz_y_min: int,
    xyz_y_max: int,
    ntiles: int,
) -> None:
    tms_min = int(ntiles) - 1 - int(xyz_y_max)
    tms_max = int(ntiles) - 1 - int(xyz_y_min)
    mbtiles_conn.execute(
        "DELETE FROM tiles WHERE zoom_level = ? AND tile_row BETWEEN ? AND ?",
        [int(zoom), int(tms_min), int(tms_max)],
    )
    mbtiles_conn.commit()


def _render_tile_group_stream(
    tile_groups: Iterator[tuple[int, int, np.ndarray, np.ndarray, np.ndarray]],
    *,
    zoom: int,
    radius: int,
    render_mode: str,
    kernel: np.ndarray | None,
    vmax: float,
    args: argparse.Namespace,
    mbtiles_conn: sqlite3.Connection,
    counters: MutableMapping[str, float],
    progress: tqdm | None,
) -> tuple[int, int, int, int]:
    """Render a bounded stream and commit encoded PNGs to MBTiles."""
    ntiles = 1 << int(zoom)
    workers = max(1, int(getattr(args, "tile_render_workers", 8)))
    queue_limit = max(
        workers,
        int(getattr(args, "tile_render_queue", max(8, workers * 4))),
    )
    batch_size = max(1, int(getattr(args, "mbtiles_insert_batch_size", 1000)))
    png_level = int(getattr(args, "png_compress_level", 1))
    png_optimize = bool(getattr(args, "png_optimize", False))
    mbtiles_rows: list[tuple[int, int, int, bytes]] = []
    pending: set[Any] = set()
    tiles_written = 0
    png_bytes = 0
    rows_seen = 0
    tiles_submitted = 0

    def flush_mbtiles_rows() -> None:
        if not mbtiles_rows:
            return
        started = time.perf_counter()
        mbtiles_conn.executemany(
            """
            INSERT OR REPLACE INTO tiles
                (zoom_level, tile_column, tile_row, tile_data)
            VALUES (?, ?, ?, ?)
            """,
            mbtiles_rows,
        )
        mbtiles_conn.commit()
        counters["mbtiles_batch_write_seconds"] += time.perf_counter() - started
        counters["mbtiles_batches"] += 1
        mbtiles_rows.clear()

    def consume(future_or_result: Any, from_future: bool) -> None:
        nonlocal tiles_written, png_bytes, rows_seen
        result = future_or_result.result() if from_future else future_or_result
        wrote, byte_count, input_rows = _accumulate_worker_result(
            result,
            zoom=zoom,
            ntiles=ntiles,
            mbtiles_rows=mbtiles_rows,
            counters=counters,
        )
        tiles_written += wrote
        png_bytes += byte_count
        rows_seen += input_rows
        if progress is not None:
            progress.update(1)
        if len(mbtiles_rows) >= batch_size:
            flush_mbtiles_rows()

    executor = (
        ThreadPoolExecutor(
            max_workers=workers,
            thread_name_prefix=f"tile-render-z{int(zoom)}",
        )
        if workers > 1
        else None
    )
    try:
        for tile_x, tile_y, gxs, gys, values in tile_groups:
            args_tuple = (
                int(tile_x),
                int(tile_y),
                gxs,
                gys,
                values,
                int(radius),
                render_mode,
                kernel,
                float(vmax),
                int(args.alpha_min),
                int(args.alpha_max),
                png_level,
                png_optimize,
            )
            if executor is None:
                consume(render_sparse_tile_to_png(*args_tuple), False)
            else:
                pending.add(executor.submit(render_sparse_tile_to_png, *args_tuple))
                if len(pending) >= queue_limit:
                    done, _ = wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        pending.remove(future)
                        consume(future, True)
                else:
                    done = {future for future in pending if future.done()}
                    for future in done:
                        pending.remove(future)
                        consume(future, True)
            tiles_submitted += 1

        while pending:
            done, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                pending.remove(future)
                consume(future, True)
        flush_mbtiles_rows()
    finally:
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=False)
    return tiles_written, png_bytes, rows_seen, tiles_submitted


def _render_mode_and_kernel(args: argparse.Namespace) -> tuple[str, np.ndarray | None]:
    radius = int(args.blur_radius)
    if radius == 0:
        return "vectorized_zero_blur", None
    vector_requested = bool(getattr(args, "use_vectorized_tile_blur", True))
    use_vectorized = bool(vector_requested and scipy_ndimage_convolve is not None)
    if vector_requested and not use_vectorized:
        print(
            "Warning: SciPy is unavailable; using the slower per-pixel "
            "splat fallback for blurred rendering."
        )
    mode = "vectorized_scipy_blur" if use_vectorized else "legacy_splat_blur"
    return mode, gaussian_kernel(radius)


def render_zoom(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    zoom: int,
    args: argparse.Namespace,
    mbtiles_conn: sqlite3.Connection,
    pixel_count: int,
    work_dir: Path,
    state_dir: Path,
    analytical_hash: str | StageFingerprint | Iterable[str],
    render_hash: str | StageFingerprint | Iterable[str],
) -> RenderZoomResult:
    """Render a zoom into canonical MBTiles using bounded whole-tile workers."""
    if mbtiles_conn is None:
        raise RuntimeError("Visual rendering requires an open canonical MBTiles database.")
    profiler = get_profiler(args)
    zoom = int(zoom)
    radius = int(args.blur_radius)
    if radius < 0 or radius >= TILE_SIZE // 2:
        raise ValueError(
            f"blur_radius must be between 0 and {TILE_SIZE // 2 - 1}"
        )
    ntiles = 1 << zoom
    render_mode, kernel = _render_mode_and_kernel(args)

    with (
        profiler.operation("query_zoom_colour_scale", zoom=zoom)
        if profiler is not None
        else null_profile_context()
    ):
        vmax = get_zoom_vmax(con, table_name, float(args.colour_quantile))
    print(f"Zoom {zoom}: colour scale vmax at q={args.colour_quantile} is {vmax:,.6f}")
    print(f"Zoom {zoom}: render mode: {render_mode}")
    print(
        f"Zoom {zoom}: whole-tile workers={max(1, int(getattr(args, 'tile_render_workers', 8)))}, "
        f"queue={max(1, int(getattr(args, 'tile_render_queue', 32)))}"
    )
    print(
        f"Zoom {zoom}: render target bands={max(1, int(getattr(args, 'render_tile_band_rows', 16)))} rows, "
        f"cache partitions={max(1, int(getattr(args, 'render_cache_partition_band_rows', 64)))} rows, "
        f"DuckDB cache/feed threads={max(1, int(getattr(args, 'render_cache_duckdb_threads', 4)))}"
    )

    stream_mode = str(getattr(args, "render_stream_mode", "auto")).lower()
    if stream_mode not in {"auto", "global_sort", "banded"}:
        raise ValueError("render_stream_mode must be auto, global_sort, or banded")
    use_banded = stream_mode == "banded" or (
        stream_mode == "auto"
        and (
            zoom >= int(getattr(args, "render_banded_min_zoom", 10))
            or int(pixel_count) >= int(
                getattr(args, "render_banded_min_pixels", 100_000_000)
            )
        )
    )

    counters: defaultdict[str, float] = defaultdict(float)
    total_tiles = 0
    new_tiles = 0
    rows_streamed = 0
    png_bytes = 0
    cache_path: Path | None = None

    context = (
        profiler.operation(
            "render_zoom_stream",
            zoom=zoom,
            blur_radius=radius,
            render_mode=render_mode,
            stream_mode="banded" if use_banded else "global_sort",
            tile_render_workers=max(1, int(getattr(args, "tile_render_workers", 8))),
        )
        if profiler is not None
        else null_profile_context()
    )
    render_duckdb_threads = (
        max(1, int(getattr(args, "render_cache_duckdb_threads", 4)))
        if use_banded
        else None
    )
    thread_context = (
        temporary_duckdb_threads(con, render_duckdb_threads)
        if use_banded
        else nullcontext()
    )
    with context as metrics, thread_context:
        if not use_banded:
            clear_incomplete_mbtiles_zoom(zoom, mbtiles_conn)
            sql = tile_expanded_pixel_sql(table_name, zoom, radius)
            if radius > 0:
                try:
                    con.execute("EXPLAIN " + sql).fetchall()
                except Exception as exc:
                    print(
                        "Warning: optimized edge-only expansion is unavailable "
                        f"({exc}); using the compatibility query."
                    )
                    sql = legacy_tile_expanded_pixel_sql(table_name, zoom, radius)
            tile_groups = iter_ordered_tile_groups(
                con=con,
                sql=sql,
                rows_per_batch=int(getattr(args, "render_row_batch_size", 500_000)),
                ntiles=ntiles,
                counters=counters,
                progress=None,
            )
            progress = tqdm(desc=f"render z{zoom}", unit="tiles", dynamic_ncols=True)
            try:
                wrote, byte_count, row_count, submitted = _render_tile_group_stream(
                    tile_groups,
                    zoom=zoom,
                    radius=radius,
                    render_mode=render_mode,
                    kernel=kernel,
                    vmax=vmax,
                    args=args,
                    mbtiles_conn=mbtiles_conn,
                    counters=counters,
                    progress=progress,
                )
            finally:
                progress.close()
            total_tiles = new_tiles = wrote
            png_bytes = byte_count
            rows_streamed = row_count
            counters["tiles_submitted"] += submitted
        else:
            cache = build_or_reuse_render_cache(
                con=con,
                table_name=table_name,
                zoom=zoom,
                pixel_count=int(pixel_count),
                args=args,
                work_dir=work_dir,
                analytical_hash=analytical_hash,
                profiler=profiler,
            )
            cache_path = cache.root
            cache_partition_rows = cache.band_tile_rows
            target_band_rows = max(
                1, int(getattr(args, "render_tile_band_rows", 16))
            )
            target_min = max(0, cache.min_base_tile_y - (1 if radius > 0 else 0))
            target_max = min(ntiles - 1, cache.max_base_tile_y + (1 if radius > 0 else 0))
            band_ids = list(
                range(
                    target_min // target_band_rows,
                    target_max // target_band_rows + 1,
                )
            )
            execution_hash = render_band_execution_hash(
                render_hash=render_hash,
                zoom=zoom,
                radius=radius,
                target_band_rows=target_band_rows,
            )
            marker_root = render_band_marker_root(state_dir, zoom)
            marker_root.mkdir(parents=True, exist_ok=True)

            valid_markers: dict[int, dict[str, Any]] = {}
            for band_id in band_ids:
                marker = read_valid_completion_marker(
                    render_band_marker_path(state_dir, zoom, band_id),
                    execution_hash,
                    marker_type="render_band_complete",
                )
                if marker is not None:
                    valid_markers[band_id] = marker

            if valid_markers:
                # Validate all saved bands against the committed archive in one scan.
                # This avoids trusting a marker whose rows were later truncated while
                # still preserving every other valid band after a partial failure.
                actual_band_counts = {
                    int(row[0]): int(row[1])
                    for row in mbtiles_conn.execute(
                        f"""
                        SELECT
                            CAST(({ntiles - 1} - tile_row) / {target_band_rows} AS INTEGER)
                                AS render_band,
                            COUNT(*)
                        FROM tiles
                        WHERE zoom_level = ?
                        GROUP BY 1
                        """,
                        [zoom],
                    ).fetchall()
                }
                invalid_bands = [
                    band_id
                    for band_id, marker in valid_markers.items()
                    if actual_band_counts.get(band_id, 0)
                    != int(marker.get("tiles_written", 0))
                ]
                for band_id in invalid_bands:
                    remove_path_quietly(
                        render_band_marker_path(state_dir, zoom, band_id)
                    )
                    valid_markers.pop(band_id, None)
                if invalid_bands:
                    print(
                        f"Zoom {zoom}: invalidated {len(invalid_bands):,} render-band "
                        "checkpoints whose committed MBTiles rows no longer match."
                    )

            if not valid_markers:
                # No trustworthy band checkpoint exists. Discard any untracked rows
                # from an old/failed renderer and begin this zoom cleanly.
                clear_incomplete_mbtiles_zoom(zoom, mbtiles_conn)
                if marker_root.exists():
                    shutil.rmtree(marker_root, ignore_errors=True)
                marker_root.mkdir(parents=True, exist_ok=True)
            else:
                print(
                    f"Zoom {zoom}: resuming banded MBTiles render with "
                    f"{len(valid_markers):,}/{len(band_ids):,} destination bands complete."
                )

            progress = tqdm(
                total=len(band_ids),
                initial=len(valid_markers),
                desc=f"render bands z{zoom}",
                unit="bands",
                dynamic_ncols=True,
            )
            use_conditional_unnest = True
            conditional_checked = False
            try:
                for band_id in band_ids:
                    marker = valid_markers.get(band_id)
                    if marker is not None:
                        total_tiles += int(marker.get("tiles_written", 0))
                        continue

                    target_start = max(target_min, band_id * target_band_rows)
                    target_end = min(
                        target_max, (band_id + 1) * target_band_rows - 1
                    )
                    _delete_mbtiles_xyz_y_band(
                        mbtiles_conn,
                        zoom,
                        target_start,
                        target_end,
                        ntiles,
                    )
                    source_min = max(0, target_start - (1 if radius > 0 else 0))
                    source_max = min(ntiles - 1, target_end + (1 if radius > 0 else 0))
                    source_files = render_cache_band_files(
                        cache.root,
                        range(
                            source_min // cache_partition_rows,
                            source_max // cache_partition_rows + 1,
                        ),
                    )
                    if not source_files:
                        wrote = byte_count = row_count = submitted = 0
                    else:
                        sql = banded_tile_expanded_pixel_sql(
                            source_files=source_files,
                            zoom=zoom,
                            radius=radius,
                            target_tile_y_min=target_start,
                            target_tile_y_max=target_end,
                            use_conditional_unnest=use_conditional_unnest,
                        )
                        if radius > 0 and not conditional_checked:
                            try:
                                con.execute("EXPLAIN " + sql).fetchall()
                            except Exception as exc:
                                print(
                                    "Warning: conditional edge-only band expansion "
                                    f"is unavailable ({exc}); using bounded compatibility SQL."
                                )
                                use_conditional_unnest = False
                                sql = banded_tile_expanded_pixel_sql(
                                    source_files=source_files,
                                    zoom=zoom,
                                    radius=radius,
                                    target_tile_y_min=target_start,
                                    target_tile_y_max=target_end,
                                    use_conditional_unnest=False,
                                )
                            conditional_checked = True
                        tile_groups = iter_ordered_tile_groups(
                            con=con,
                            sql=sql,
                            rows_per_batch=int(
                                getattr(args, "render_row_batch_size", 500_000)
                            ),
                            ntiles=ntiles,
                            counters=counters,
                            progress=None,
                        )
                        wrote, byte_count, row_count, submitted = _render_tile_group_stream(
                            tile_groups,
                            zoom=zoom,
                            radius=radius,
                            render_mode=render_mode,
                            kernel=kernel,
                            vmax=vmax,
                            args=args,
                            mbtiles_conn=mbtiles_conn,
                            counters=counters,
                            progress=None,
                        )
                    write_completion_marker(
                        render_band_marker_path(state_dir, zoom, band_id),
                        marker_type="render_band_complete",
                        config_hash=execution_hash,
                        zoom=zoom,
                        band_id=int(band_id),
                        target_tile_y_min=int(target_start),
                        target_tile_y_max=int(target_end),
                        target_band_rows=int(target_band_rows),
                        cache_partition_band_rows=int(cache_partition_rows),
                        tiles_written=int(wrote),
                        render_rows=int(row_count),
                        png_bytes=int(byte_count),
                    )
                    total_tiles += int(wrote)
                    new_tiles += int(wrote)
                    rows_streamed += int(row_count)
                    png_bytes += int(byte_count)
                    counters["tiles_submitted"] += int(submitted)
                    counters["render_bands_completed_this_execution"] += 1
                    progress.update(1)
            finally:
                progress.close()

            actual_tiles = int(
                mbtiles_conn.execute(
                    "SELECT COUNT(*) FROM tiles WHERE zoom_level = ?", [zoom]
                ).fetchone()[0]
            )
            if actual_tiles != total_tiles:
                raise RuntimeError(
                    f"Zoom {zoom}: band markers total {total_tiles:,} tiles but "
                    f"MBTiles contains {actual_tiles:,}. Remove the render-band state "
                    "for this zoom and resume."
                )

        metrics.update(
            {
                "items": int(rows_streamed),
                "item_unit": "render_rows_this_execution",
                "stream_mode": "banded" if use_banded else "global_sort",
                "tiles_total": int(total_tiles),
                "tiles_written_this_execution": int(new_tiles),
                "bytes": int(png_bytes),
                "phase_seconds": dict(counters),
            }
        )

    print(
        f"Zoom {zoom}: canonical MBTiles now contains {total_tiles:,} tiles; "
        f"this execution rendered {new_tiles:,} tiles "
        f"({format_bytes(png_bytes)} new PNG data)."
    )
    print("Render phase breakdown (worker timings are cumulative sums):")
    for name, seconds in sorted(counters.items(), key=lambda item: item[1], reverse=True):
        if name.endswith("_seconds") or name.endswith("_seconds_sum"):
            print(f"  {name}: {format_duration(seconds)}")
    return RenderZoomResult(
        total_tiles=int(total_tiles),
        tiles_written_this_execution=int(new_tiles),
        rows_streamed_this_execution=int(rows_streamed),
        png_bytes_this_execution=int(png_bytes),
        stream_mode="banded" if use_banded else "global_sort",
    )


def _write_xyz_file(path: Path, data: bytes) -> int:
    path.write_bytes(data)
    return len(data)


def extract_xyz_from_mbtiles(
    mbtiles_conn: sqlite3.Connection,
    mbtiles_path: Path,
    xyz_root: Path,
    zoom: int,
    expected_tiles: int,
    args: argparse.Namespace,
    state_dir: Path,
    render_hash: str | StageFingerprint | Iterable[str],
    profiler: StageProfiler | None = None,
) -> XYZExportResult:
    """Derive XYZ from encoded MBTiles PNGs, checkpointing by tile column."""
    zoom = int(zoom)
    expected_tiles = int(expected_tiles)
    ntiles = 1 << zoom
    source_count = int(
        mbtiles_conn.execute(
            "SELECT COUNT(*) FROM tiles WHERE zoom_level = ?", [zoom]
        ).fetchone()[0]
    )
    if source_count != expected_tiles:
        raise RuntimeError(
            f"Zoom {zoom}: MBTiles contains {source_count:,} tiles but the render "
            f"marker expects {expected_tiles:,}."
        )

    zoom_dir = xyz_root / str(zoom)
    progress_path = xyz_progress_path(state_dir, zoom)
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    render_hash_value = primary_hash(render_hash)
    column_rows = [
        (int(row[0]), int(row[1]))
        for row in mbtiles_conn.execute(
            """
            SELECT tile_column, COUNT(*)
            FROM tiles
            WHERE zoom_level = ?
            GROUP BY tile_column
            ORDER BY tile_column
            """,
            [zoom],
        ).fetchall()
    ]
    column_counts = dict(column_rows)
    completed_columns: set[int] = set()
    valid_progress = False
    if progress_path.exists():
        try:
            saved = read_json(progress_path)
            valid_progress = (
                int(saved.get("zoom", -1)) == zoom
                and str(saved.get("render_hash", "")) == render_hash_value
                and int(saved.get("layout_version", -1)) == XYZ_EXPORT_LAYOUT_VERSION
                and int(saved.get("source_tiles", -1)) == source_count
            )
            if valid_progress:
                completed_columns = {
                    int(value)
                    for value in saved.get("completed_columns", [])
                    if int(value) in column_counts
                    and (zoom_dir / str(int(value))).is_dir()
                }
        except Exception:
            valid_progress = False

    if not valid_progress:
        if zoom_dir.exists():
            shutil.rmtree(zoom_dir, ignore_errors=True)
        remove_path_quietly(progress_path)
        completed_columns = set()
    zoom_dir.mkdir(parents=True, exist_ok=True)

    workers = max(1, int(getattr(args, "xyz_extract_workers", 8)))
    queue_limit = max(
        workers,
        int(getattr(args, "xyz_extract_queue", max(16, workers * 8))),
    )
    checkpoint_every = 8
    tiles_reused = sum(column_counts[x] for x in completed_columns)
    tiles_new = 0
    bytes_new = 0
    completed_since_checkpoint = 0

    def save_progress(final: bool = False) -> None:
        atomic_write_json(
            progress_path,
            {
                "zoom": zoom,
                "render_hash": render_hash_value,
                "layout_version": XYZ_EXPORT_LAYOUT_VERSION,
                "source_mbtiles": str(mbtiles_path),
                "source_tiles": source_count,
                "completed_columns": sorted(completed_columns),
                "completed_column_count": len(completed_columns),
                "total_column_count": len(column_rows),
                "tiles_completed": sum(column_counts[x] for x in completed_columns),
                "status": "complete" if final else "running",
                "updated_at": utc_now_iso(),
            },
        )

    print(
        f"Zoom {zoom}: deriving XYZ from MBTiles with {workers} file writers; "
        f"{len(completed_columns):,}/{len(column_rows):,} tile columns complete."
    )
    progress = tqdm(
        total=source_count,
        initial=tiles_reused,
        desc=f"XYZ from MBTiles z{zoom}",
        unit="tiles",
        unit_scale=True,
        dynamic_ncols=True,
    )
    context = (
        profiler.operation(
            "extract_xyz_from_mbtiles",
            zoom=zoom,
            source_tiles=source_count,
            writer_workers=workers,
        )
        if profiler is not None
        else null_profile_context()
    )
    with context as metrics:
        executor = ThreadPoolExecutor(
            max_workers=workers,
            thread_name_prefix=f"xyz-write-z{zoom}",
        )
        try:
            for tile_x, expected_column_count in column_rows:
                if tile_x in completed_columns:
                    continue
                tile_dir = zoom_dir / str(tile_x)
                if tile_dir.exists():
                    shutil.rmtree(tile_dir, ignore_errors=True)
                tile_dir.mkdir(parents=True, exist_ok=True)

                pending: set[Any] = set()
                column_written = 0
                cursor = mbtiles_conn.execute(
                    """
                    SELECT tile_row, tile_data
                    FROM tiles
                    WHERE zoom_level = ? AND tile_column = ?
                    ORDER BY tile_row
                    """,
                    [zoom, tile_x],
                )
                while True:
                    rows = cursor.fetchmany(256)
                    if not rows:
                        break
                    for tms_y, tile_data in rows:
                        xyz_y = ntiles - 1 - int(tms_y)
                        future = executor.submit(
                            _write_xyz_file,
                            tile_dir / f"{xyz_y}.png",
                            bytes(tile_data),
                        )
                        pending.add(future)
                        if len(pending) >= queue_limit:
                            done, _ = wait(pending, return_when=FIRST_COMPLETED)
                            for finished in done:
                                pending.remove(finished)
                                bytes_new += int(finished.result())
                                tiles_new += 1
                                column_written += 1
                                progress.update(1)
                while pending:
                    done, _ = wait(pending, return_when=FIRST_COMPLETED)
                    for finished in done:
                        pending.remove(finished)
                        bytes_new += int(finished.result())
                        tiles_new += 1
                        column_written += 1
                        progress.update(1)

                if column_written != expected_column_count:
                    raise RuntimeError(
                        f"Zoom {zoom}, tile column {tile_x}: wrote {column_written:,} "
                        f"files, expected {expected_column_count:,}."
                    )
                completed_columns.add(tile_x)
                completed_since_checkpoint += 1
                if completed_since_checkpoint >= checkpoint_every:
                    save_progress(final=False)
                    completed_since_checkpoint = 0
        finally:
            executor.shutdown(wait=True, cancel_futures=False)
            progress.close()

        total_written = tiles_reused + tiles_new
        if total_written != source_count:
            raise RuntimeError(
                f"Zoom {zoom}: XYZ extraction accounted for {total_written:,} tiles, "
                f"expected {source_count:,}."
            )
        save_progress(final=True)
        atomic_write_json(
            zoom_dir / "_XYZ_COMPLETE.json",
            {
                "zoom": zoom,
                "tiles_written": source_count,
                "layout_version": XYZ_EXPORT_LAYOUT_VERSION,
                "render_hash": render_hash_value,
                "source_mbtiles": str(mbtiles_path),
                "completed_at": utc_now_iso(),
            },
        )
        metrics.update(
            {
                "items": int(source_count),
                "item_unit": "tiles",
                "bytes": int(bytes_new),
                "tiles_reused": int(tiles_reused),
                "tiles_written_this_execution": int(tiles_new),
                "tile_columns": len(column_rows),
            }
        )
    return XYZExportResult(
        total_tiles=source_count,
        tiles_written_this_execution=tiles_new,
        bytes_written_this_execution=bytes_new,
    )


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create resumable AIS track-density raster XYZ/MBTiles heatmap tiles."
    )

    parser.add_argument(
        "--ship-id-shards",
        type=int,
        default=64,
        help="Number of SHIP_ID hash shards used when building track segments.",
    )
    parser.add_argument(
        "--duckdb-memory-limit",
        default=None,
        help='Optional DuckDB memory limit, e.g. "16GB".',
    )
    parser.add_argument(
        "--duckdb-temp-dir",
        default=None,
        help=(
            "Base folder for run-specific DuckDB spill roots. Each named run and "
            "each execution receives its own subdirectory."
        ),
    )
    parser.add_argument(
        "--duckdb-preserve-insertion-order",
        action="store_true",
        default=False,
        help="Keep DuckDB insertion-order preservation enabled.",
    )

    parser.add_argument(
        "--ais-folder",
        default=r"C:\Users\Craig Pearce\Desktop\ais_2025",
        help="Folder containing AIS GeoParquet/Parquet files.",
    )
    parser.add_argument(
        "--vessel-csv",
        default=r"C:\Users\Craig Pearce\Desktop\Data_sets\MT\MT_vessel_data.csv",
        help="CSV containing vessel metadata and COMFLEET_GROUPEDTYPE.",
    )
    parser.add_argument(
        "--output-dir",
        default=r"C:\Users\Craig Pearce\Desktop\ais_heatmap_tiles",
        help="Output folder.",
    )
    parser.add_argument(
        "--output-name",
        default=None,
        help="Stable base name for the output. Strongly recommended for resumption.",
    )

    parser.add_argument(
        "--dataset-label",
        default=None,
        help=(
            "Dataset label used only for an automatically generated output name, "
            "for example ais_2023. Defaults to the AIS folder name."
        ),
    )

    parser.add_argument("--ship-id-column", default="SHIP_ID")
    parser.add_argument("--lat-column", default="LAT")
    parser.add_argument("--lon-column", default="LON")
    parser.add_argument("--speed-column", default="SPEED")
    parser.add_argument("--timestamp-column", default="TIMESTAMP")
    parser.add_argument("--vessel-ship-id-column", default="SHIP_ID")
    parser.add_argument("--vessel-group-column", default="COMFLEET_GROUPEDTYPE")

    parser.add_argument(
        "--groups",
        nargs="+",
        default=["ALL"],
        help='Use ALL, or one or more COMFLEET_GROUPEDTYPE values.',
    )
    parser.add_argument("--speed-scale", type=float, default=10.0)
    parser.add_argument("--min-speed-knots", type=float, default=1.0)
    parser.add_argument(
        "--require-both-endpoint-speeds",
        action="store_true",
        default=True,
    )
    parser.add_argument(
        "--allow-one-endpoint-speed",
        dest="require_both_endpoint_speeds",
        action="store_false",
    )
    parser.add_argument(
        "--metric",
        choices=["track_km", "vessel_hours"],
        default="track_km",
    )
    parser.add_argument("--min-zoom", type=int, default=0)
    parser.add_argument("--max-zoom", type=int, default=13)
    parser.add_argument("--max-gap-minutes", type=int, default=60)
    parser.add_argument("--max-implied-speed-knots", type=float, default=80.0)
    parser.add_argument("--sample-step-px", type=float, default=2.0)
    parser.add_argument("--max-segment-samples", type=int, default=4096)
    parser.add_argument(
        "--derive-lower-zooms-from-max",
        dest="derive_lower_zooms_from_max",
        action="store_true",
        help=(
            "Rasterise only max_zoom from vessel segments, then build a descending "
            "analytical pyramid by summing adjacent 2 x 2 pixel blocks. This is "
            "faster and is the default."
        ),
    )
    parser.add_argument(
        "--rasterise-each-zoom-from-segments",
        dest="derive_lower_zooms_from_max",
        action="store_false",
        help="Legacy slower mode: rasterise vessel segments separately for every zoom.",
    )
    parser.set_defaults(derive_lower_zooms_from_max=True)
    parser.add_argument(
        "--render-row-batch-size",
        type=int,
        default=500_000,
        help="Rows per DuckDB Arrow batch for streaming render/GeoTIFF writes.",
    )
    parser.add_argument(
        "--profile-stages",
        dest="profile_stages",
        action="store_true",
        help="Write stage timing/profiling JSON to the run-state folder.",
    )
    parser.add_argument(
        "--no-profile-stages",
        dest="profile_stages",
        action="store_false",
        help="Disable stage timing/profiling JSON output.",
    )
    parser.set_defaults(profile_stages=True)
    parser.add_argument(
        "--profile-resource-sample-seconds", type=float, default=15.0
    )
    parser.add_argument(
        "--profile-console-heartbeat-seconds", type=float, default=300.0
    )
    parser.add_argument(
        "--prevent-windows-sleep",
        dest="prevent_windows_sleep",
        action="store_true",
        help="Keep the Windows system awake while the pipeline is running.",
    )
    parser.add_argument(
        "--allow-windows-sleep",
        dest="prevent_windows_sleep",
        action="store_false",
        help="Do not request Windows system-sleep prevention.",
    )
    parser.set_defaults(prevent_windows_sleep=True)

    parser.add_argument("--pixel-flush-threshold", type=int, default=5_000_000)
    parser.add_argument("--segment-batch-size", type=int, default=250_000)
    parser.add_argument("--use-numba-rasterizer", dest="use_numba_rasterizer", action="store_true")
    parser.add_argument("--no-numba-rasterizer", dest="use_numba_rasterizer", action="store_false")
    parser.set_defaults(use_numba_rasterizer=True)
    parser.add_argument("--numba-threads", type=int, default=0)
    parser.add_argument("--arrow-batch-readahead", type=int, default=16)
    parser.add_argument("--arrow-fragment-readahead", type=int, default=8)
    parser.add_argument("--pixel-part-parquet-compression", default="zstd")
    parser.add_argument("--pixel-part-parquet-compression-level", type=int, default=1)
    parser.add_argument("--duckdb-parquet-compression-level", type=int, default=1)
    parser.add_argument("--segment-shard-workers", type=int, default=1)
    parser.add_argument("--segment-worker-duckdb-threads", type=int, default=None)
    parser.add_argument(
        "--segment-checkpoint-schema",
        choices=["compact", "diagnostic"],
        default="compact",
        help=(
            "Compact stores only the five rasterisation columns; diagnostic "
            "retains timestamps/speeds for investigations."
        ),
    )
    parser.add_argument("--blur-radius", type=int, default=3)
    parser.add_argument("--colour-quantile", type=float, default=0.995)
    parser.add_argument("--alpha-min", type=int, default=25)
    parser.add_argument("--alpha-max", type=int, default=230)

    parser.add_argument("--use-vectorized-tile-blur", dest="use_vectorized_tile_blur", action="store_true")
    parser.add_argument("--legacy-splat-blur", dest="use_vectorized_tile_blur", action="store_false")
    parser.set_defaults(use_vectorized_tile_blur=True)
    parser.add_argument("--png-compress-level", type=int, default=1)
    parser.add_argument("--png-optimize", action="store_true", default=False)
    parser.add_argument(
        "--tile-render-workers", "--tile-encode-workers",
        dest="tile_render_workers", type=int, default=8,
        help=(
            "Whole-tile worker threads. Each worker performs pixel placement, "
            "optional blur, colourisation and PNG encoding. The old "
            "--tile-encode-workers name remains an alias."
        ),
    )
    parser.add_argument(
        "--tile-render-queue", "--tile-encode-queue",
        dest="tile_render_queue", type=int, default=32,
        help="Maximum number of whole-tile jobs in the bounded worker queue.",
    )
    parser.add_argument(
        "--render-stream-mode",
        choices=["auto", "global_sort", "banded"],
        default="auto",
        help=(
            "auto uses memory-bounded tile-row bands for high zooms or large pixel "
            "tables; global_sort retains the legacy whole-table ORDER BY; banded "
            "always uses the new compact render spool."
        ),
    )
    parser.add_argument("--render-banded-min-zoom", type=int, default=10)
    parser.add_argument("--render-banded-min-pixels", type=int, default=100_000_000)
    parser.add_argument(
        "--render-tile-band-rows",
        type=int,
        default=16,
        help="Destination tile rows per render/checkpoint band.",
    )
    parser.add_argument(
        "--render-cache-partition-band-rows",
        type=int,
        default=64,
        help=(
            "Tile rows per physical Parquet cache partition. The corrected z11 "
            "benchmark selected 64 while retaining 16-row render bands."
        ),
    )
    parser.add_argument(
        "--render-cache-duckdb-threads",
        type=int,
        default=4,
        help="DuckDB threads used while building and feeding the banded render cache.",
    )
    parser.add_argument("--render-tile-parts-compression", default="zstd")
    parser.add_argument("--render-tile-parts-compression-level", type=int, default=1)
    parser.add_argument(
        "--keep-render-tile-parts-after-mbtiles",
        dest="delete_render_tile_parts_after_mbtiles",
        action="store_false",
    )
    parser.set_defaults(delete_render_tile_parts_after_mbtiles=True)
    parser.add_argument("--xyz-extract-workers", type=int, default=8)
    parser.add_argument("--xyz-extract-queue", type=int, default=64)
    parser.add_argument("--mbtiles-insert-batch-size", type=int, default=1000)
    parser.add_argument("--mbtiles-cache-mb", type=int, default=256)

    parser.add_argument("--write-value-pixels", action="store_true", default=False)
    parser.add_argument("--write-value-tiffs", action="store_true", default=False)
    parser.add_argument(
        "--write-value-composite-geotiff",
        action="store_true",
        default=False,
    )
    parser.add_argument("--value-output-zooms", nargs="*", type=int, default=None)
    parser.add_argument("--value-tiff-tile-limit", type=int, default=None)

    parser.add_argument(
        "--value-pixels-schema",
        choices=["compact", "enriched"],
        default="compact",
        help="Compact reuses gx/gy/value checkpoint; enriched adds convenience coordinates.",
    )
    parser.add_argument("--geotiff-compression", default="zstd")
    parser.add_argument("--geotiff-compression-level", type=int, default=1)
    parser.add_argument(
        "--geotiff-predictor",
        default=None,
        help="GTiff predictor (1/2/3), or none/off/0 to omit it. Default: none.",
    )
    parser.add_argument("--geotiff-num-threads", default="1")
    parser.add_argument("--geotiff-block-size", type=int, default=512)
    parser.add_argument("--geotiff-write-tiles-per-chunk", type=int, default=64)

    parser.add_argument("--no-xyz", dest="write_xyz", action="store_false")
    parser.add_argument("--no-mbtiles", dest="write_mbtiles", action="store_false")
    parser.set_defaults(write_xyz=True, write_mbtiles=True)
    parser.add_argument("--threads", type=int, default=max(1, os.cpu_count() or 1))

    # Existing-output behaviour.
    parser.add_argument(
        "--resume-existing",
        dest="resume_existing",
        action="store_true",
        help="Resume an existing named output (default).",
    )
    parser.add_argument(
        "--start-again",
        dest="resume_existing",
        action="store_false",
        help="Delete this named output and rebuild it from the beginning.",
    )
    parser.set_defaults(resume_existing=True)
    parser.add_argument(
        "--legacy-first-incomplete-zoom",
        type=int,
        default=None,
        help=(
            "One-time adoption setting for output created by the older script "
            "without run_state.json."
        ),
    )
    parser.add_argument(
        "--allow-resume-setting-mismatch",
        action="store_true",
        default=False,
        help="Allow changed output-defining settings on resume (not recommended).",
    )

    parser.add_argument(
        "--checkpoint-aggregated-pixels",
        dest="checkpoint_aggregated_pixels",
        action="store_true",
    )
    parser.add_argument(
        "--no-aggregated-pixel-checkpoints",
        dest="checkpoint_aggregated_pixels",
        action="store_false",
    )
    parser.set_defaults(checkpoint_aggregated_pixels=True)

    parser.add_argument(
        "--keep-work-on-failure",
        dest="keep_work_on_failure",
        action="store_true",
    )
    parser.add_argument(
        "--discard-work-on-failure",
        dest="keep_work_on_failure",
        action="store_false",
    )
    parser.set_defaults(keep_work_on_failure=True)
    parser.add_argument(
        "--keep-work-after-success",
        action="store_true",
        default=False,
    )
    # Backward-compatible alias for the old debugging option.
    parser.add_argument(
        "--keep-work",
        dest="keep_work_after_success",
        action="store_true",
        help=argparse.SUPPRESS,
    )

    parser.add_argument(
        "--keep-duckdb-temp-on-failure",
        dest="keep_duckdb_temp_on_failure",
        action="store_true",
    )
    parser.add_argument(
        "--discard-duckdb-temp-on-failure",
        dest="keep_duckdb_temp_on_failure",
        action="store_false",
    )
    parser.set_defaults(keep_duckdb_temp_on_failure=True)
    parser.add_argument(
        "--keep-duckdb-temp-after-success",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--no-progress-json",
        dest="write_progress_json",
        action="store_false",
        help="Do not write current_stage_progress.json during long stages.",
    )
    parser.set_defaults(write_progress_json=True)
    parser.add_argument(
        "--progress-update-seconds",
        type=float,
        default=15.0,
        help="Minimum seconds between progress JSON / ETA metadata updates.",
    )

    parser.add_argument(
        "--force-remove-output-lock",
        action="store_true",
        default=False,
    )

    return parser.parse_args(args=argv)

def run_pipeline(args: argparse.Namespace) -> None:
    """Execute one configured density-map run."""
    if args.min_zoom < 0 or args.max_zoom < args.min_zoom:
        raise ValueError("Invalid zoom range.")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    groups_without_all = [g for g in args.groups if g.upper() != "ALL"]
    if groups_without_all:
        group_slug = "_".join(safe_slug(g) for g in groups_without_all)
    else:
        group_slug = "all_vessels"

    output_name = resolve_output_name(args, output_dir, group_slug)
    paths = build_pipeline_paths(args, output_dir, output_name)
    work_dir = paths.work_dir
    state_dir = paths.state_dir
    xyz_named_path = paths.xyz_named_path
    xyz_root = paths.xyz_root
    mbtiles_path = paths.mbtiles_path
    tile_archive_path = paths.tile_archive_path
    lock = OutputRunLock(
        paths.lock_path,
        force_remove=bool(getattr(args, "force_remove_output_lock", False)),
    )

    manager: RunStateManager | None = None
    render_con: duckdb.DuckDBPyConnection | None = None
    mbtiles_conn: sqlite3.Connection | None = None
    duckdb_run_root: Path | None = None
    run_succeeded = False
    profiler: StageProfiler | None = None

    lock.acquire()
    try:
        configs = build_pipeline_configs(args)
        existing_output = has_any_named_output(
            work_dir=work_dir,
            state_dir=state_dir,
            xyz_root=xyz_named_path,
            mbtiles_path=mbtiles_path,
            output_dir=output_dir,
            output_name=output_name,
        )

        old_duckdb_run_root: Path | None = None
        if (state_dir / "run_state.json").exists():
            try:
                old_state = read_json(state_dir / "run_state.json")
                saved_root = old_state.get("duckdb", {}).get("run_root")
                if saved_root:
                    old_duckdb_run_root = Path(saved_root)
            except Exception:
                pass

        if not args.resume_existing:
            if existing_output:
                print(f"Start-again mode: deleting existing named output {output_name!r}.")
            for path in named_output_paths(
                work_dir,
                state_dir,
                xyz_named_path,
                mbtiles_path,
                output_dir,
                output_name,
            ):
                remove_path_quietly(path)
            if old_duckdb_run_root is not None:
                remove_path_quietly(old_duckdb_run_root)
            manager = create_new_run_state(state_dir, output_name, configs)
        elif (state_dir / "run_state.json").exists():
            manager = load_run_state(
                state_dir=state_dir,
                current_configs=configs,
                allow_setting_mismatch=bool(
                    getattr(args, "allow_resume_setting_mismatch", False)
                ),
            )
            print(f"Loaded resumable state for named output: {output_name}")
        elif existing_output:
            legacy_zoom = getattr(args, "legacy_first_incomplete_zoom", None)
            if legacy_zoom is None:
                raise RuntimeError(
                    "Existing output files were found, but there is no run_state.json. "
                    "For a one-time migration from the old script, set "
                    "legacy_first_incomplete_zoom to the first zoom that must be "
                    "rebuilt. Otherwise use start-again mode."
                )
            manager = create_new_run_state(state_dir, output_name, configs)
            manager.ensure_zoom_entries(args.min_zoom, args.max_zoom)
            adopt_legacy_completed_zooms(
                manager=manager,
                args=args,
                first_incomplete_zoom=int(legacy_zoom),
                xyz_root=xyz_root,
                mbtiles_path=tile_archive_path,
                output_dir=output_dir,
                output_name=output_name,
            )
        else:
            manager = create_new_run_state(state_dir, output_name, configs)

        manager.ensure_zoom_entries(args.min_zoom, args.max_zoom)
        work_dir.mkdir(parents=True, exist_ok=True)
        duckdb_run_root, execution_root = prepare_duckdb_execution_root(
            args=args,
            manager=manager,
            work_dir=work_dir,
            output_name=output_name,
        )

        print("Running with settings:")
        print(f"Output name: {output_name}")
        print(f"Path layout version: {PATH_LAYOUT_VERSION}")
        print(f"Segment fingerprint: {manager.segment_fingerprint.primary[:12]}")
        print(f"Analytical fingerprint: {manager.analytical_fingerprint.primary[:12]}")
        print(f"Render fingerprint: {manager.render_fingerprint.primary[:12]}")
        print(f"Export fingerprint: {manager.export_fingerprint.primary[:12]}")
        print(f"Work directory: {work_dir}")
        print(f"State directory: {state_dir}")
        print(f"Canonical tile archive: {tile_archive_path}")
        print(f"AIS folder: {args.ais_folder}")
        print(f"Vessel CSV: {args.vessel_csv}")
        print(f"Output dir: {args.output_dir}")
        print(f"Groups: {args.groups}")
        print(f"Metric: {args.metric}")
        print(
            f"Speed threshold: > {args.min_speed_knots} kt, "
            f"using SPEED / {args.speed_scale}"
        )
        print(f"Zooms: {args.min_zoom} to {args.max_zoom}")
        print(f"Resume existing: {args.resume_existing}")
        print(f"Run state: {manager.manifest_path}")
        print(f"Persistent DuckDB run root: {duckdb_run_root}")
        print(f"This execution's DuckDB temp root: {execution_root}")
        print(
            "Retain work after failure: "
            f"{getattr(args, 'keep_work_on_failure', True)}"
        )
        print(
            "Descending analytical pyramid enabled: "
            f"{getattr(args, 'derive_lower_zooms_from_max', True)}"
        )

        profiler = StageProfiler(
            state_dir / "stage_profile.json",
            enabled=bool(getattr(args, "profile_stages", True)),
            resource_sample_seconds=float(
                getattr(args, "profile_resource_sample_seconds", 15.0)
            ),
            console_heartbeat_seconds=float(
                getattr(args, "profile_console_heartbeat_seconds", 300.0)
            ),
        )
        args._profiler = profiler
        manager.manifest["profile_path"] = str(profiler.path)
        manager.manifest["performance_events_path"] = str(profiler.events_path)
        manager.manifest["performance_csv_path"] = str(profiler.csv_path)
        manager.manifest["performance_resources_path"] = str(
            profiler.resources_path
        )
        manager.save()
        print(f"Performance implementation: {PERFORMANCE_IMPLEMENTATION_VERSION}")
        print(f"Numba rasteriser available/enabled: {njit is not None}/{getattr(args, 'use_numba_rasterizer', True)}")
        print(f"SciPy vector blur available/enabled: {scipy_ndimage_convolve is not None}/{getattr(args, 'use_vectorized_tile_blur', True)}")
        print(f"psutil resource monitoring available: {psutil is not None}")
        print(f"Value-pixel schema: {getattr(args, 'value_pixels_schema', 'compact')}")
        missing_accelerators: list[str] = []
        if getattr(args, "use_numba_rasterizer", True) and njit is None:
            missing_accelerators.append("numba")
        if (
            int(args.blur_radius) > 0
            and getattr(args, "use_vectorized_tile_blur", True)
            and scipy_ndimage_convolve is None
        ):
            missing_accelerators.append("scipy")
        if getattr(args, "profile_stages", True) and psutil is None:
            missing_accelerators.append("psutil")
        if missing_accelerators:
            print(
                "Performance warning: install these optional packages in the "
                "Spyder environment to enable all optimized paths: "
                + ", ".join(missing_accelerators)
            )
        profiler.record_metric(
            "run_arguments",
            arguments={
                key: value
                for key, value in vars(args).items()
                if key != "_profiler"
            },
        )
        for storage_label, storage_path in (
            ("output_dir", output_dir),
            ("duckdb_execution_root", execution_root),
        ):
            try:
                usage = shutil.disk_usage(storage_path)
                profiler.record_metric(
                    "storage_capacity_at_start",
                    storage_label=storage_label,
                    path=str(storage_path),
                    total_bytes=int(usage.total),
                    used_bytes=int(usage.used),
                    free_bytes=int(usage.free),
                )
            except Exception:
                pass

        min_zoom = int(args.min_zoom)
        max_zoom = int(args.max_zoom)
        derive_from_max = bool(
            getattr(args, "derive_lower_zooms_from_max", True)
        )

        incomplete_zooms = [
            zoom
            for zoom in range(min_zoom, max_zoom + 1)
            if not manager.is_zoom_complete(
                zoom=zoom,
                args=args,
                xyz_root=xyz_root,
                mbtiles_path=tile_archive_path,
                output_dir=output_dir,
                output_name=output_name,
            )
        ]
        manager.save()
        if not incomplete_zooms:
            total_tiles = total_completed_tiles(
                manager, args.min_zoom, args.max_zoom
            )
            manager.mark_run_complete(total_tiles)
            run_succeeded = True
            print("")
            print("All requested zooms are already complete; nothing to resume.")
            print(f"Tiles recorded in completion markers: {total_tiles:,}")
            return

        print(
            "Incomplete zooms: "
            + ", ".join(str(zoom) for zoom in incomplete_zooms)
        )
        if derive_from_max:
            print(
                "Analytical pyramid processing order: "
                + " -> ".join(
                    str(zoom)
                    for zoom in range(max_zoom, min(incomplete_zooms) - 1, -1)
                )
            )
        else:
            print(f"First incomplete zoom: {min(incomplete_zooms)}")

        if visual_outputs_requested(args):
            mbtiles_conn = open_or_init_mbtiles(
                path=tile_archive_path,
                name=output_name,
                min_zoom=args.min_zoom,
                max_zoom=args.max_zoom,
                resume_existing=bool(args.resume_existing),
                cache_mb=int(getattr(args, "mbtiles_cache_mb", 256)),
            )

        tiles_written_this_execution = 0
        incomplete_zoom_set = set(incomplete_zooms)

        # A completed MBTiles archive is sufficient to derive or resume XYZ.
        # Handle zooms whose analytical export and canonical render are already
        # complete before opening DuckDB or rebuilding any analytical pyramid.
        if bool(getattr(args, "write_xyz", False)) and mbtiles_conn is not None:
            xyz_only_completed: list[int] = []
            for zoom in sorted(incomplete_zoom_set, reverse=True):
                render_done = manager.is_render_complete(
                    zoom,
                    args,
                    xyz_root,
                    tile_archive_path,
                    mbtiles_conn=mbtiles_conn,
                )
                export_done = manager.is_export_complete(
                    zoom, args, output_dir, output_name
                )
                xyz_done = manager.is_xyz_complete(zoom, args, xyz_root)
                if not (render_done and export_done and not xyz_done):
                    continue

                render_marker = read_valid_completion_marker(
                    manager.render_marker_path(zoom),
                    manager.render_fingerprint,
                    marker_type="render_complete",
                )
                if render_marker is None:
                    continue
                expected_tiles = int(render_marker.get("tiles_written", 0))
                manager.mark_zoom_stage(
                    zoom,
                    "extracting_xyz_from_completed_mbtiles",
                    tiles_expected=expected_tiles,
                )
                with profiler.stage(
                    "extract_xyz_from_completed_mbtiles",
                    zoom=zoom,
                    tiles_expected=expected_tiles,
                ):
                    xyz_result = extract_xyz_from_mbtiles(
                        mbtiles_conn=mbtiles_conn,
                        mbtiles_path=tile_archive_path,
                        xyz_root=xyz_root,
                        zoom=zoom,
                        expected_tiles=expected_tiles,
                        args=args,
                        state_dir=state_dir,
                        render_hash=manager.render_fingerprint,
                        profiler=profiler,
                    )
                manager.mark_xyz_complete(
                    zoom=zoom,
                    tiles_written=int(xyz_result.total_tiles),
                    tiles_written_this_execution=int(
                        xyz_result.tiles_written_this_execution
                    ),
                    derived_without_analytical_reload=True,
                )
                manager.mark_zoom_complete(
                    zoom=zoom,
                    tiles_written=expected_tiles,
                    derived_xyz_from_completed_mbtiles=True,
                )
                xyz_only_completed.append(zoom)

            for zoom in xyz_only_completed:
                incomplete_zoom_set.discard(zoom)

        if not incomplete_zoom_set:
            safe_close_sqlite(mbtiles_conn, context="XYZ-only completion close")
            mbtiles_conn = None
            total_tiles = total_completed_tiles(
                manager, args.min_zoom, args.max_zoom
            )
            manager.mark_run_complete(total_tiles)
            run_succeeded = True
            print("")
            print("All remaining work was completed directly from MBTiles.")
            print(f"Tiles recorded across completed zooms: {total_tiles:,}")
            if args.write_mbtiles:
                print(f"MBTiles: {mbtiles_path}")
            if args.write_xyz:
                print(f"XYZ folder: {xyz_root}")
            print(f"Run state: {manager.manifest_path}")
            return

        incomplete_zooms = sorted(incomplete_zoom_set)

        # The render database is scratch storage. Give every execution its
        # own database file as well as its own spill directory, so a stale WAL
        # or file handle from a failed execution cannot affect the resumed run.
        render_db_path = execution_root / "render.duckdb"
        render_con = connect_duckdb(
            database=render_db_path,
            args=args,
            work_dir=work_dir,
            temp_subdir="render",
        )

        segment_path: Path | None = None
        max_zoom_table_name: str | None = None
        max_zoom_pixel_count: int | None = None

        def ensure_segments() -> Path:
            nonlocal segment_path
            if segment_path is not None:
                return segment_path
            manager.manifest.setdefault("stages", {})["segments"] = {
                "status": "building_or_resuming",
                "updated_at": utc_now_iso(),
            }
            manager.save()
            with profiler.stage("build_or_resume_track_segments"):
                segment_path = build_segments_parquet(
                    args=args,
                    work_dir=work_dir,
                    config_hash=manager.segment_fingerprint,
                )
            manager.manifest.setdefault("stages", {})["segments"] = {
                "status": "complete",
                "path": str(segment_path),
                "updated_at": utc_now_iso(),
            }
            manager.save()
            return segment_path

        def try_recover_zoom_table(
            zoom: int,
            *,
            record_zoom_stage: bool,
        ) -> tuple[str, int, str] | None:
            """Load a trustworthy analytical checkpoint for one zoom, if present."""
            zoom = int(zoom)
            if record_zoom_stage:
                manager.mark_zoom_stage(zoom, "recovering_checkpoint")

            with profiler.stage("recover_zoom_checkpoint", zoom=zoom):
                loaded = try_load_existing_aggregated_pixels(
                    con=render_con,
                    zoom=zoom,
                    args=args,
                    work_dir=work_dir,
                    output_dir=output_dir,
                    output_name=output_name,
                    config_hash=manager.analytical_fingerprint,
                )

            if loaded is not None and record_zoom_stage:
                table_name, pixel_count, checkpoint_source = loaded
                manager.mark_zoom_stage(
                    zoom,
                    "pixel_aggregation_complete",
                    pixel_count=int(pixel_count),
                    checkpoint_source=checkpoint_source,
                )
            return loaded

        def ensure_max_zoom_pixel_table(
            *,
            record_zoom_stage: bool,
        ) -> tuple[str | None, int]:
            """Load or build the maximum-zoom analytical pixel table once."""
            nonlocal max_zoom_table_name, max_zoom_pixel_count
            if (
                max_zoom_table_name is not None
                and max_zoom_pixel_count is not None
            ):
                return max_zoom_table_name, max_zoom_pixel_count

            loaded = try_recover_zoom_table(
                max_zoom,
                record_zoom_stage=record_zoom_stage,
            )
            if loaded is not None:
                (
                    max_zoom_table_name,
                    max_zoom_pixel_count,
                    _checkpoint_source,
                ) = loaded
                return max_zoom_table_name, int(max_zoom_pixel_count)

            max_segments = ensure_segments()
            stage_dir = work_dir / f"pixel_parts_z{max_zoom}"
            if record_zoom_stage:
                manager.mark_zoom_stage(
                    max_zoom,
                    "building_max_zoom_pixel_parts",
                )
            with profiler.stage("build_max_zoom_pixel_parts", zoom=max_zoom):
                has_parts = build_pixel_parts_for_zoom(
                    segment_path=max_segments,
                    zoom=max_zoom,
                    stage_dir=stage_dir,
                    args=args,
                    config_hash=manager.analytical_fingerprint,
                    segment_config_hash=manager.segment_fingerprint,
                    progress_path=state_dir / "current_stage_progress.json",
                )

            if not has_parts:
                print(
                    f"Zoom {max_zoom}: no pixel parts; "
                    "all lower pyramid levels will be empty."
                )
                max_zoom_table_name = None
                max_zoom_pixel_count = 0
                return None, 0

            if record_zoom_stage:
                manager.mark_zoom_stage(
                    max_zoom,
                    "aggregating_max_zoom_pixels",
                )
            with profiler.stage("aggregate_max_zoom_pixels", zoom=max_zoom):
                max_zoom_table_name, max_zoom_pixel_count = aggregate_pixel_parts(
                    con=render_con,
                    stage_dir=stage_dir,
                    zoom=max_zoom,
                    profiler=profiler,
                )
            if getattr(args, "checkpoint_aggregated_pixels", True):
                with profiler.stage(
                    "checkpoint_max_zoom_pixels",
                    zoom=max_zoom,
                ):
                    write_aggregated_pixel_checkpoint(
                        con=render_con,
                        table_name=max_zoom_table_name,
                        zoom=max_zoom,
                        work_dir=work_dir,
                        config_hash=manager.analytical_fingerprint,
                        pixel_count=int(max_zoom_pixel_count),
                        args=args,
                        profiler=profiler,
                    )
            if record_zoom_stage:
                manager.mark_zoom_stage(
                    max_zoom,
                    "pixel_aggregation_complete",
                    pixel_count=int(max_zoom_pixel_count),
                    checkpoint_source="new_max_zoom_aggregation",
                )
            return max_zoom_table_name, int(max_zoom_pixel_count)

        def recover_or_derive_adjacent_zoom(
            *,
            zoom: int,
            source_table: str | None,
            source_zoom: int | None,
            source_count: int,
            record_zoom_stage: bool,
        ) -> tuple[str | None, int, str]:
            """Recover a zoom checkpoint or derive it from the adjacent level."""
            zoom = int(zoom)
            loaded = try_recover_zoom_table(
                zoom,
                record_zoom_stage=record_zoom_stage,
            )
            if loaded is not None:
                table_name, pixel_count, checkpoint_source = loaded
                return table_name, int(pixel_count), checkpoint_source

            if source_table is None or int(source_count) <= 0:
                print(
                    f"Zoom {zoom}: adjacent source level is empty; "
                    "recording an empty analytical level."
                )
                return None, 0, "empty_adjacent_source"

            expected_source_zoom = zoom + 1
            if source_zoom != expected_source_zoom:
                raise RuntimeError(
                    "Descending analytical pyramid lost its adjacent source: "
                    f"target zoom {zoom} expected source zoom "
                    f"{expected_source_zoom}, received {source_zoom}."
                )

            if record_zoom_stage:
                manager.mark_zoom_stage(
                    zoom,
                    "deriving_pixels_from_adjacent_zoom",
                    source_zoom=int(source_zoom),
                )
            else:
                print(
                    f"Zoom {zoom}: visual output is complete but no reusable "
                    f"analytical checkpoint was found; rebuilding it from zoom "
                    f"{source_zoom} for the remaining lower levels."
                )

            with profiler.stage(
                "derive_zoom_pixels_from_adjacent",
                source_zoom=int(source_zoom),
                target_zoom=zoom,
            ):
                table_name, pixel_count = derive_pixel_table_from_higher_zoom(
                    con=render_con,
                    source_table=source_table,
                    source_zoom=int(source_zoom),
                    target_zoom=zoom,
                    profiler=profiler,
                )

            if getattr(args, "checkpoint_aggregated_pixels", True):
                with profiler.stage(
                    "checkpoint_derived_zoom_pixels",
                    zoom=zoom,
                    source_zoom=int(source_zoom),
                ):
                    write_aggregated_pixel_checkpoint(
                        con=render_con,
                        table_name=table_name,
                        zoom=zoom,
                        work_dir=work_dir,
                        config_hash=manager.analytical_fingerprint,
                        pixel_count=int(pixel_count),
                        args=args,
                        profiler=profiler,
                    )

            checkpoint_source = f"derived_from_z{int(source_zoom)}"
            if record_zoom_stage:
                manager.mark_zoom_stage(
                    zoom,
                    "pixel_aggregation_complete",
                    pixel_count=int(pixel_count),
                    checkpoint_source=checkpoint_source,
                    source_zoom=int(source_zoom),
                )
            return table_name, int(pixel_count), checkpoint_source

        def write_and_render_zoom(
            *,
            zoom: int,
            table_name: str | None,
            pixel_count: int,
        ) -> None:
            """Run incomplete analytical, canonical MBTiles and derived XYZ stages."""
            nonlocal tiles_written_this_execution
            zoom = int(zoom)
            pixel_count = int(pixel_count)

            export_done = manager.is_export_complete(
                zoom, args, output_dir, output_name
            )
            render_done = manager.is_render_complete(
                zoom,
                args,
                xyz_root,
                tile_archive_path,
                mbtiles_conn=mbtiles_conn,
            )
            xyz_done = manager.is_xyz_complete(zoom, args, xyz_root)

            if table_name is None or pixel_count <= 0:
                if not export_done:
                    manager.mark_export_complete(
                        zoom,
                        pixel_count=0,
                        empty_analytical_level=True,
                    )
                if not render_done:
                    clear_incomplete_mbtiles_zoom(zoom, mbtiles_conn)
                    manager.mark_render_complete(
                        zoom,
                        tiles_written=0,
                        pixel_count=0,
                        empty_analytical_level=True,
                    )
                if bool(getattr(args, "write_xyz", False)) and not xyz_done:
                    clear_incomplete_xyz_output(zoom, xyz_root, state_dir=state_dir)
                    manager.mark_xyz_complete(
                        zoom,
                        tiles_written=0,
                        pixel_count=0,
                        empty_analytical_level=True,
                    )
                manager.mark_zoom_complete(
                    zoom=zoom,
                    tiles_written=0,
                    pixel_count=0,
                )
                return

            if not export_done:
                manager.mark_zoom_stage(
                    zoom,
                    "writing_analytical_outputs",
                    pixel_count=pixel_count,
                )
                with profiler.stage(
                    "write_analytical_outputs",
                    zoom=zoom,
                    pixel_count=pixel_count,
                ):
                    export_value_outputs(
                        con=render_con,
                        table_name=table_name,
                        zoom=zoom,
                        args=args,
                        output_dir=output_dir,
                        output_name=output_name,
                        pixel_count=pixel_count,
                        work_dir=work_dir,
                    )
                manager.mark_export_complete(zoom, pixel_count=pixel_count)
            else:
                print(f"Zoom {zoom}: analytical export marker is complete; skipping export.")

            zoom_tiles = 0
            if visual_outputs_requested(args) and not render_done:
                # New PNG bytes invalidate any previously derived XYZ tree.
                manager.xyz_marker_path(zoom).unlink(missing_ok=True)
                clear_incomplete_xyz_output(zoom, xyz_root, state_dir=state_dir)
                manager.mark_zoom_stage(
                    zoom,
                    "rendering_mbtiles",
                    pixel_count=pixel_count,
                )
                with profiler.stage(
                    "render_visual_tiles_to_mbtiles",
                    zoom=zoom,
                    pixel_count=pixel_count,
                ):
                    render_result = render_zoom(
                        con=render_con,
                        table_name=table_name,
                        zoom=zoom,
                        args=args,
                        mbtiles_conn=mbtiles_conn,
                        pixel_count=pixel_count,
                        work_dir=work_dir,
                        state_dir=state_dir,
                        analytical_hash=manager.analytical_fingerprint,
                        render_hash=manager.render_fingerprint,
                    )
                zoom_tiles = int(render_result.total_tiles)
                tiles_written_this_execution += int(
                    render_result.tiles_written_this_execution
                )
                manager.mark_render_complete(
                    zoom=zoom,
                    tiles_written=zoom_tiles,
                    pixel_count=pixel_count,
                    stream_mode=render_result.stream_mode,
                )
                render_done = True
                xyz_done = False
                if bool(getattr(args, "delete_render_tile_parts_after_mbtiles", True)):
                    for cache_dir in work_dir.glob(f"render_source_z{zoom}_b*"):
                        remove_path_quietly(cache_dir)
            elif visual_outputs_requested(args):
                marker = read_valid_completion_marker(
                    manager.render_marker_path(zoom),
                    manager.render_fingerprint,
                    marker_type="render_complete",
                )
                zoom_tiles = int(marker.get("tiles_written", 0)) if marker else 0
                print(
                    f"Zoom {zoom}: canonical MBTiles render marker is complete; "
                    "skipping rendering."
                )

            if bool(getattr(args, "write_xyz", False)) and not xyz_done:
                if mbtiles_conn is None or xyz_root is None:
                    raise RuntimeError(
                        "XYZ extraction requires the canonical MBTiles archive and an XYZ root."
                    )
                manager.mark_zoom_stage(
                    zoom,
                    "extracting_xyz_from_mbtiles",
                    pixel_count=pixel_count,
                    tiles_expected=zoom_tiles,
                )
                with profiler.stage(
                    "extract_xyz_from_mbtiles",
                    zoom=zoom,
                    tiles_expected=zoom_tiles,
                ):
                    xyz_result = extract_xyz_from_mbtiles(
                        mbtiles_conn=mbtiles_conn,
                        mbtiles_path=tile_archive_path,
                        xyz_root=xyz_root,
                        zoom=zoom,
                        expected_tiles=zoom_tiles,
                        args=args,
                        state_dir=state_dir,
                        render_hash=manager.render_fingerprint,
                        profiler=profiler,
                    )
                manager.mark_xyz_complete(
                    zoom=zoom,
                    tiles_written=int(xyz_result.total_tiles),
                    tiles_written_this_execution=int(
                        xyz_result.tiles_written_this_execution
                    ),
                )
                xyz_done = True
            elif bool(getattr(args, "write_xyz", False)):
                print(f"Zoom {zoom}: XYZ extraction marker is complete; skipping XYZ.")

            manager.mark_zoom_complete(
                zoom=zoom,
                tiles_written=int(zoom_tiles),
                pixel_count=pixel_count,
            )


        if derive_from_max:
            highest_incomplete = max(incomplete_zoom_set)
            lowest_incomplete = min(incomplete_zoom_set)

            source_table_name: str | None = None
            source_zoom: int | None = None
            source_pixel_count = 0

            # Resume from the closest trustworthy analytical level whenever
            # possible. On a fresh run this naturally falls back to max_zoom.
            if highest_incomplete < max_zoom:
                loaded = try_recover_zoom_table(
                    highest_incomplete,
                    record_zoom_stage=True,
                )
                if loaded is not None:
                    (
                        source_table_name,
                        source_pixel_count,
                        checkpoint_source,
                    ) = loaded
                    source_zoom = highest_incomplete
                    print(
                        f"Resuming pyramid at incomplete zoom {source_zoom} "
                        f"from {checkpoint_source}."
                    )

                if source_table_name is None:
                    for candidate_zoom in range(
                        highest_incomplete + 1,
                        max_zoom,
                    ):
                        candidate = try_recover_zoom_table(
                            candidate_zoom,
                            record_zoom_stage=False,
                        )
                        if candidate is None:
                            continue
                        (
                            source_table_name,
                            source_pixel_count,
                            checkpoint_source,
                        ) = candidate
                        source_zoom = candidate_zoom
                        print(
                            f"Resuming descending pyramid from completed zoom "
                            f"{source_zoom} checkpoint ({checkpoint_source})."
                        )
                        break

            if source_table_name is None:
                source_table_name, source_pixel_count = (
                    ensure_max_zoom_pixel_table(
                        record_zoom_stage=max_zoom in incomplete_zoom_set,
                    )
                )
                source_zoom = max_zoom

            if source_zoom is None:
                raise RuntimeError(
                    "Could not establish a source zoom for the descending pyramid."
                )

            if source_zoom in incomplete_zoom_set:
                write_and_render_zoom(
                    zoom=source_zoom,
                    table_name=source_table_name,
                    pixel_count=source_pixel_count,
                )
                incomplete_zoom_set.discard(source_zoom)
            else:
                print(
                    f"Zoom {source_zoom}: visual completion marker found; "
                    "using its analytical level only as the pyramid source."
                )

            for zoom in range(source_zoom - 1, lowest_incomplete - 1, -1):
                zoom_needs_outputs = zoom in incomplete_zoom_set
                next_table_name, next_pixel_count, _checkpoint_source = (
                    recover_or_derive_adjacent_zoom(
                        zoom=zoom,
                        source_table=source_table_name,
                        source_zoom=source_zoom,
                        source_count=source_pixel_count,
                        record_zoom_stage=zoom_needs_outputs,
                    )
                )

                if zoom_needs_outputs:
                    write_and_render_zoom(
                        zoom=zoom,
                        table_name=next_table_name,
                        pixel_count=next_pixel_count,
                    )
                    incomplete_zoom_set.discard(zoom)
                else:
                    print(
                        f"Zoom {zoom}: visual completion marker found; "
                        "using its analytical level only for the next reduction."
                    )

                if (
                    source_table_name is not None
                    and source_table_name != next_table_name
                ):
                    render_con.execute(
                        f"DROP TABLE IF EXISTS {source_table_name}"
                    )
                    if source_table_name == max_zoom_table_name:
                        max_zoom_table_name = None

                source_table_name = next_table_name
                source_pixel_count = int(next_pixel_count)
                source_zoom = zoom

            if source_table_name is not None:
                render_con.execute(
                    f"DROP TABLE IF EXISTS {source_table_name}"
                )
                if source_table_name == max_zoom_table_name:
                    max_zoom_table_name = None
        else:
            # Legacy mode: each incomplete zoom is rasterised independently from
            # the vessel segments. Keep the original ascending processing order.
            for zoom in range(min(incomplete_zooms), max_zoom + 1):
                if zoom not in incomplete_zoom_set:
                    print(f"Zoom {zoom}: completion marker found; skipping.")
                    continue

                loaded = try_recover_zoom_table(
                    zoom,
                    record_zoom_stage=True,
                )
                if loaded is not None:
                    table_name, pixel_count, _checkpoint_source = loaded
                else:
                    segments = ensure_segments()
                    stage_dir = work_dir / f"pixel_parts_z{zoom}"
                    manager.mark_zoom_stage(zoom, "building_pixel_parts")
                    with profiler.stage("build_pixel_parts", zoom=int(zoom)):
                        has_parts = build_pixel_parts_for_zoom(
                            segment_path=segments,
                            zoom=zoom,
                            stage_dir=stage_dir,
                            args=args,
                            config_hash=manager.analytical_fingerprint,
                            segment_config_hash=manager.segment_fingerprint,
                            progress_path=(
                                state_dir / "current_stage_progress.json"
                            ),
                        )

                    if not has_parts:
                        print(
                            f"Zoom {zoom}: no pixel parts; "
                            "recording an empty zoom."
                        )
                        write_and_render_zoom(
                            zoom=zoom,
                            table_name=None,
                            pixel_count=0,
                        )
                        incomplete_zoom_set.discard(zoom)
                        continue

                    manager.mark_zoom_stage(zoom, "aggregating_pixels")
                    with profiler.stage("aggregate_pixels", zoom=int(zoom)):
                        table_name, pixel_count = aggregate_pixel_parts(
                            con=render_con,
                            stage_dir=stage_dir,
                            zoom=zoom,
                            profiler=profiler,
                        )
                    if getattr(args, "checkpoint_aggregated_pixels", True):
                        with profiler.stage(
                            "checkpoint_aggregated_pixels",
                            zoom=int(zoom),
                        ):
                            write_aggregated_pixel_checkpoint(
                                con=render_con,
                                table_name=table_name,
                                zoom=zoom,
                                work_dir=work_dir,
                                config_hash=manager.analytical_fingerprint,
                                pixel_count=int(pixel_count),
                                args=args,
                                profiler=profiler,
                            )
                    manager.mark_zoom_stage(
                        zoom,
                        "pixel_aggregation_complete",
                        pixel_count=int(pixel_count),
                        checkpoint_source="new_aggregation",
                    )

                write_and_render_zoom(
                    zoom=zoom,
                    table_name=table_name,
                    pixel_count=int(pixel_count),
                )
                incomplete_zoom_set.discard(zoom)
                render_con.execute(f"DROP TABLE IF EXISTS {table_name}")

        if max_zoom_table_name is not None:
            render_con.execute(
                f"DROP TABLE IF EXISTS {max_zoom_table_name}"
            )

        # Close output databases before declaring 100% completion. A DuckDB
        # temp-file deletion error on close is reported as a warning because all
        # zoom outputs and their transaction commits have already completed.
        safe_close_duckdb(render_con, context="final render database close")
        render_con = None
        safe_close_sqlite(mbtiles_conn, context="final MBTiles close")
        mbtiles_conn = None

        total_tiles = total_completed_tiles(manager, args.min_zoom, args.max_zoom)
        if manager.first_incomplete_zoom(
            args.min_zoom,
            args.max_zoom,
            args,
            xyz_root,
            tile_archive_path,
            output_dir,
            output_name,
        ) is not None:
            raise RuntimeError(
                "Processing ended but one or more requested zooms lack a valid "
                "completion marker. Intermediate work has been retained."
            )

        manager.mark_run_complete(total_tiles)
        run_succeeded = True

        print("")
        print("Done.")
        print(f"Tiles written this execution: {tiles_written_this_execution:,}")
        print(f"Tiles recorded across completed zooms: {total_tiles:,}")
        if args.write_mbtiles:
            print(f"MBTiles: {mbtiles_path}")
        elif visual_outputs_requested(args):
            print(f"Internal visual tile archive: {tile_archive_path}")
        if args.write_xyz:
            print(f"XYZ folder: {xyz_root}")
        print(f"Run state: {manager.manifest_path}")
        if getattr(args, "profile_stages", True):
            print(f"Stage profile: {state_dir / 'stage_profile.json'}")

    except BaseException as exc:
        if manager is not None:
            try:
                manager.record_error(exc)
            except Exception as state_exc:
                print(f"Warning: could not record failure in run state: {state_exc}")
        raise

    finally:
        safe_close_duckdb(render_con, context="error/final cleanup")
        safe_close_sqlite(mbtiles_conn, context="error/final cleanup")

        if manager is not None:
            if run_succeeded:
                if not getattr(args, "keep_work_after_success", False):
                    remove_path_quietly(work_dir)
                if (
                    duckdb_run_root is not None
                    and not getattr(args, "keep_duckdb_temp_after_success", False)
                ):
                    remove_path_quietly(duckdb_run_root)
            else:
                if not getattr(args, "keep_work_on_failure", True):
                    remove_path_quietly(work_dir)
                if (
                    duckdb_run_root is not None
                    and not getattr(args, "keep_duckdb_temp_on_failure", True)
                ):
                    remove_path_quietly(duckdb_run_root)
                elif duckdb_run_root is not None:
                    print(f"Incomplete run: retained work directory: {work_dir}")
                    print(
                        "Incomplete run: retained DuckDB run root: "
                        f"{duckdb_run_root}"
                    )

        if profiler is not None:
            try:
                profiler.close()
                profiler.print_summary()
            except Exception as profile_exc:
                print(f"Warning: could not finalise performance logs: {profile_exc}")
        lock.release()


@contextmanager
def windows_sleep_prevention(enabled: bool = True) -> Iterator[bool]:
    """Keep Windows awake for long runs while still allowing the display to sleep."""
    if not enabled or os.name != "nt":
        yield False
        return
    es_continuous = 0x80000000
    es_system_required = 0x00000001
    try:
        result = ctypes.windll.kernel32.SetThreadExecutionState(
            es_continuous | es_system_required
        )
    except Exception as exc:
        print(f"Warning: could not request Windows sleep prevention: {exc}")
        yield False
        return
    if not result:
        print("Warning: Windows rejected the sleep-prevention request.")
        yield False
        return
    try:
        yield True
    finally:
        try:
            ctypes.windll.kernel32.SetThreadExecutionState(es_continuous)
        except Exception as exc:
            print(f"Warning: could not restore Windows execution state: {exc}")

def main() -> None:
    # Keep Spyder-specific settings separate from pipeline execution.
    RUN_FROM_SPYDER = True
    args = get_args_for_spyder() if RUN_FROM_SPYDER else parse_args()
    with windows_sleep_prevention(
        bool(getattr(args, "prevent_windows_sleep", True))
    ) as sleep_prevention_active:
        print(
            "Windows sleep prevention active: "
            f"{sleep_prevention_active}"
        )
        run_pipeline(args)


if __name__ == "__main__":
    main()
