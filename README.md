# AIS Raster-Density Heatmap

This repository creates AIS density layers as visual XYZ tiles and MBTiles, with optional analytical sparse Parquet and Float32 GeoTIFF outputs.

All run settings are stored in the visible `config.env` file. The same configuration is used whether the script is run from Spyder or a terminal.

## Repository files

```text
build_ais_heatmap.py
config.env
requirements.txt
.gitignore
README.md
```

`config.env` is intentionally committed to the repository so colleagues can see and edit it after cloning or downloading the project. It contains processing settings and local file paths, but no database credentials.

## Supported metrics

Three metrics are supported through `METRIC`.

### Track kilometres

```dotenv
METRIC=track_km
```

The script orders observations by vessel and timestamp, constructs valid consecutive track segments, estimates each segment's length, and distributes its kilometres along the Web Mercator pixels crossed by the segment.

This is generally the best choice for showing route or shipping-lane density.

### Vessel hours

```dotenv
METRIC=vessel_hours
```

The script constructs the same valid segments but distributes the elapsed time between their endpoints, expressed in vessel-hours, along the crossed pixels.

This is useful for showing time spent moving through an area.

### AIS point count

```dotenv
METRIC=point_count
```

Each qualifying AIS source row contributes exactly one count to the Web Mercator pixel containing that observation. The script does **not** interpolate a line to the next position and does not use the timestamp-gap or implied-speed segment checks.

This is a basic observation-density measure. It is affected by reporting frequency, duplicate records, receiver coverage, and data sampling. It should not be interpreted as distance travelled, time spent, or unique vessel count.

## Inclusive speed threshold

The speed test is configured with:

```dotenv
SPEED_SCALE=10.0
MIN_SPEED_KNOTS=1.0
```

The test is inclusive:

```text
SPEED / SPEED_SCALE >= MIN_SPEED_KNOTS
```

With the defaults, a raw `SPEED` value of `10` is included because it represents exactly `1.0` knot. Values below `10` are excluded, which removes stopped and very slow observations from the density layer.

For `point_count`, every observation is tested independently.

For `track_km` and `vessel_hours`, this setting controls how the two endpoints of a segment are tested:

```dotenv
REQUIRE_BOTH_ENDPOINT_SPEEDS=true
```

- `true`: both endpoints must be at or above the threshold.
- `false`: the average speed of the two endpoints must be at or above the threshold.

`REQUIRE_BOTH_ENDPOINT_SPEEDS` is ignored for `point_count`.

## Input requirements

### AIS Parquet files

`AIS_FOLDER` must contain Parquet or GeoParquet files. Its subdirectories are searched recursively.

The default AIS columns are:

```text
SHIP_ID
LAT
LON
SPEED
TIMESTAMP
```

The names can be changed in `config.env`:

```dotenv
SHIP_ID_COLUMN=SHIP_ID
LAT_COLUMN=LAT
LON_COLUMN=LON
SPEED_COLUMN=SPEED
TIMESTAMP_COLUMN=TIMESTAMP
```

`TIMESTAMP_COLUMN` is required by `track_km` and `vessel_hours`. It is not used by `point_count`.

### Vessel metadata

`VESSEL_CSV` must point to a CSV containing at least:

```text
SHIP_ID
COMFLEET_GROUPEDTYPE
```

The column names can be changed with:

```dotenv
VESSEL_SHIP_ID_COLUMN=SHIP_ID
VESSEL_GROUP_COLUMN=COMFLEET_GROUPEDTYPE
```

## Installation

Python 3.10 or newer is recommended.

From PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

`rasterio` is required when either of these settings is enabled:

```dotenv
WRITE_VALUE_TIFFS=true
WRITE_VALUE_COMPOSITE_GEOTIFF=true
```

If Rasterio is difficult to install using `pip`, a Conda environment can be used:

```powershell
conda install -c conda-forge rasterio
```

## Configure the run

Open `config.env` and update at least these paths:

```dotenv
AIS_FOLDER='C:/path/to/ais_parquet_files'
VESSEL_CSV='C:/path/to/vessel_metadata.csv'
OUTPUT_DIR='C:/path/to/output_folder'
DUCKDB_TEMP_DIR='C:/path/to/fast_temporary_storage'
```

Forward slashes are recommended for Windows paths. Relative paths are supported and are resolved from the directory containing `config.env`.

### Example customer point-count run

```dotenv
GROUPS=ALL
METRIC=point_count
SPEED_SCALE=10.0
MIN_SPEED_KNOTS=1.0
MIN_ZOOM=0
MAX_ZOOM=11
```

The default visual output applies a small heatmap blur:

```dotenv
BLUR_RADIUS=2
```

Set the following when the visual PNG tiles should display only the exact occupied pixels without spreading values to neighbouring pixels:

```dotenv
BLUR_RADIUS=0
```

The analytical Parquet and GeoTIFF values are always written **before** visual blur and colour rendering. For `point_count`, those analytical values are the direct observation counts.

## Vessel groups

Use every vessel represented by the AIS input:

```dotenv
GROUPS=ALL
```

Use one vessel group:

```dotenv
GROUPS='DRY BULK'
```

Use multiple vessel groups separated by commas:

```dotenv
GROUPS='DRY BULK,TANKERS,PASSENGER SHIPS'
```

Do not combine `ALL` with specific groups.

## Track-only quality controls

These settings apply only to `track_km` and `vessel_hours`:

```dotenv
MAX_GAP_MINUTES=60
MAX_IMPLIED_SPEED_KNOTS=80.0
SAMPLE_STEP_PX=3.0
MAX_SEGMENT_SAMPLES=4096
```

They are ignored for `point_count`, because no consecutive-position segments are constructed.

## Output types

Visual outputs:

```dotenv
WRITE_XYZ=true
WRITE_MBTILES=true
```

Analytical outputs containing values before colour rendering:

```dotenv
WRITE_VALUE_PIXELS=true
WRITE_VALUE_TIFFS=false
WRITE_VALUE_COMPOSITE_GEOTIFF=true
```

The analytical outputs mean:

| Metric | Analytical pixel value |
|---|---|
| `track_km` | Estimated track kilometres assigned to the pixel |
| `vessel_hours` | Estimated vessel-hours assigned to the pixel |
| `point_count` | Number of qualifying AIS source observations in the pixel |

The composite GeoTIFF is generally the easiest analytical output to open in QGIS. Separate value TIFF tiles may create a very large number of files.

## Analytical output zooms

`VALUE_OUTPUT_ZOOMS` controls which zoom levels receive analytical Parquet or GeoTIFF outputs.

Maximum zoom only:

```dotenv
VALUE_OUTPUT_ZOOMS=max
```

Every processed zoom:

```dotenv
VALUE_OUTPUT_ZOOMS=all
```

Selected zooms:

```dotenv
VALUE_OUTPUT_ZOOMS=9,10,11
```

The selected zooms must fall between `MIN_ZOOM` and `MAX_ZOOM`.

## Running the script

From PowerShell:

```powershell
python build_ais_heatmap.py
```

From Spyder:

1. Open `build_ais_heatmap.py`.
2. Ensure Spyder uses the Python environment where `requirements.txt` was installed.
3. Edit `config.env`, not the Python source.
4. Run the script normally.

The script automatically loads `config.env` from the same directory as the Python file.

## Resource settings

```dotenv
THREADS=4
DUCKDB_MEMORY_LIMIT=32GB
DUCKDB_TEMP_DIR='C:/ais_duckdb_temp'
SHIP_ID_SHARDS=8
PIXEL_FLUSH_THRESHOLD=750000
SEGMENT_BATCH_SIZE=100000
```

`DUCKDB_TEMP_DIR` should preferably be on a fast local SSD with substantial free space. Large AIS datasets can create sizeable temporary spill files.

`SHIP_ID_SHARDS` is used for the segment-building workflow and for partitioning staged `point_count` observations. Despite its historical name, `SEGMENT_BATCH_SIZE` is also used as the PyArrow batch size when rasterising points.

Lowering `THREADS`, `PIXEL_FLUSH_THRESHOLD`, or `SEGMENT_BATCH_SIZE` can reduce memory pressure at the cost of additional processing overhead and temporary files.

## Output naming

Leave this setting as `None` to generate a name from the vessel groups, metric, inclusive speed threshold, and zoom range:

```dotenv
OUTPUT_NAME=None
```

Generated names use `ge` to mean “greater than or equal to”, for example:

```text
ais_2025_dry_bulk_point_count_ge_1_0kt_z0_11
```

Set an explicit name to control all output prefixes:

```dotenv
OUTPUT_NAME=ais_2025_customer_point_count
```

`OUTPUT_NAME` must be a filename-style name, not a path.

## Configuration validation

The script validates the configuration before processing, including:

- the selected metric;
- required paths and source files;
- zoom ranges;
- positive batch and sharding values;
- speed scale and threshold;
- alpha and colour-quantile limits;
- group selection;
- analytical zoom selection;
- Rasterio availability when GeoTIFF output is enabled;
- whether at least one output type is enabled.

Numbers may contain underscores for readability:

```dotenv
PIXEL_FLUSH_THRESHOLD=750_000
```

Booleans should normally use:

```dotenv
true
false
```

Optional values can use:

```dotenv
None
```

## Temporary data

The script creates a work directory inside `OUTPUT_DIR`. By default it is removed after a successful or failed run:

```dotenv
KEEP_WORK=false
```

# Density Map Configuration Guide

- Input and output paths

AIS_FOLDER='C:/Users/Craig Pearce/Desktop/Data_sets/ais_files/ais_2025' — Choose the folder containing the downloaded raw AIS Parquet files.

VESSEL_CSV='C:/Users/Craig Pearce/Desktop/Data_sets/MT/MT_vessel_data.csv' — Choose the vessel metadata CSV used to select vessels by grouped vessel type.

OUTPUT_DIR='C:/Users/Craig Pearce/Desktop/ais_heatmap_tiles' — Choose the directory in which all heatmap, analytical, metadata, and temporary outputs will be created.

OUTPUT_NAME=None — Enter a custom output name or use None to generate one automatically from the selected groups, metric, speed threshold, and zoom range.

- AIS input column names

SHIP_ID_COLUMN=SHIP_ID — Specify the AIS input column containing the vessel identifier.

LAT_COLUMN=LAT — Specify the AIS input column containing latitude.

LON_COLUMN=LON — Specify the AIS input column containing longitude.

SPEED_COLUMN=SPEED — Specify the AIS input column containing reported vessel speed.

TIMESTAMP_COLUMN=TIMESTAMP — Specify the AIS input column containing the observation timestamp used to order positions and construct tracks.

- Vessel metadata column names

VESSEL_SHIP_ID_COLUMN=SHIP_ID — Specify the vessel metadata column containing the vessel identifier used to join with the AIS data.

VESSEL_GROUP_COLUMN=COMFLEET_GROUPEDTYPE — Specify the vessel metadata column containing the grouped vessel classification used for filtering.

- Vessel selection

GROUPS='DRY BULK' — Choose one or more vessel groups to include, separated by commas, or use ALL to include every vessel in the metadata file.

- Density metric

METRIC=track_km — Choose track_km, vessel_hours, or point_count to determine what each density-map pixel represents.

SPEED_SCALE=10.0 — Specify the factor by which the raw speed value is divided to convert it into knots.

MIN_SPEED_KNOTS=1.0 — Set the inclusive minimum speed in knots, so observations or track endpoints at this speed or above are included.

REQUIRE_BOTH_ENDPOINT_SPEEDS=true — For track metrics, set this to true to require both segment endpoints to meet the speed threshold, or false to test their average speed instead.

- Zoom range

MIN_ZOOM=0 — Set the lowest Web Mercator zoom level to generate.

MAX_ZOOM=11 — Set the highest and most detailed Web Mercator zoom level to generate.

- Track quality controls

MAX_GAP_MINUTES=60 — Set the maximum permitted time gap between consecutive AIS positions before the connection between them is rejected.

MAX_IMPLIED_SPEED_KNOTS=80.0 — Set the maximum realistic calculated speed allowed between consecutive AIS positions before the segment is excluded.

These two controls apply only to track_km and vessel_hours, because point_count processes each AIS observation independently.

- DuckDB processing resources

THREADS=4 — Set the maximum number of CPU threads DuckDB may use during processing.

DUCKDB_MEMORY_LIMIT=32GB — Set the maximum amount of memory DuckDB may use before spilling temporary data to disk.

DUCKDB_TEMP_DIR='C:/ais_duckdb_temp' — Choose the preferably fast local directory where DuckDB can store temporary spill files.

DUCKDB_PRESERVE_INSERTION_ORDER=false — Set this to false to allow DuckDB to optimise processing without preserving the original row insertion order.

SHIP_ID_SHARDS=8 — Divide vessels or point-count records into this number of processing partitions to reduce the size of each working dataset.

- Track rasterisation

SAMPLE_STEP_PX=3.0 — For track metrics, set the approximate distance in pixels between sampled points along each vessel segment.

MAX_SEGMENT_SAMPLES=4096 — Set the maximum number of sample points allowed for any individual vessel segment.

These two settings are ignored for point_count, because no lines are drawn between observations.

- Sparse-pixel processing

PIXEL_FLUSH_THRESHOLD=750000 — Set the approximate number of accumulated pixel values held in memory before they are written to temporary storage.

SEGMENT_BATCH_SIZE=100000 — Set the number of track segments or point observations processed in each rasterisation batch.

- Visual heatmap rendering

BLUR_RADIUS=2 — Set the radius of the blur applied to the rendered visual heatmap tiles.

COLOUR_QUANTILE=0.995 — Set the upper value quantile used to scale heatmap colours while reducing the influence of extreme outliers.

ALPHA_MIN=25 — Set the minimum opacity applied to visible non-zero heatmap pixels.

ALPHA_MAX=230 — Set the maximum opacity applied to the highest rendered heatmap values.

These visual settings affect the appearance of PNG tiles but do not change the underlying analytical values.

- Visual output options

WRITE_XYZ=true — Set this to true to write PNG map tiles in standard zoom, x, and y folder structures.

WRITE_MBTILES=true — Set this to true to package the rendered PNG tiles into a single MBTiles database.

Analytical output options

WRITE_VALUE_PIXELS=true — Set this to true to write sparse Parquet files containing the underlying numeric value of each non-zero pixel.

WRITE_VALUE_TIFFS=false — Set this to true to write separate Float32 GeoTIFF files for individual analytical map tiles.

WRITE_VALUE_COMPOSITE_GEOTIFF=true — Set this to true to combine analytical pixel values into a larger Float32 GeoTIFF for each selected zoom.

- Analytical output zooms

VALUE_OUTPUT_ZOOMS=max — Choose which zoom levels receive analytical outputs by using max, all, a comma-separated list such as 9,10,11, or None for the maximum zoom only.

VALUE_TIFF_TILE_LIMIT=None — Set an optional maximum number of analytical TIFF tiles for testing, or use None to write every qualifying tile.

- Temporary work files

KEEP_WORK=false — Set this to true to retain intermediate DuckDB and Parquet files for debugging, or false to remove them after a successful run.

Set `KEEP_WORK=true` only when intermediate DuckDB and Parquet files are needed for debugging. These files can be large.


# Code Summary

The new AIS density-map script converts raw AIS vessel-position data into reusable analytical and visual density-map products. It reads the source Parquet files with DuckDB, optionally filters them to selected vessel groups using vessel metadata, applies configurable speed and track-quality rules, and stages the retained AIS points into SHIP_ID hash shards so that later track construction can be processed in manageable units. 

The script supports two track-based measures of activity: track_km, which accumulates vessel distance through map pixels, and vessel_hours, which accumulates vessel movement time. Consecutive AIS observations are ordered by vessel and timestamp, converted into movement segments, and filtered for invalid timing, excessive gaps, unrealistic implied speeds and the configured minimum-speed threshold before their values are passed to the rasterisation stage.

The accepted track segments are converted into Web Mercator pixel coordinates and rasterised at the maximum requested zoom using a vectorised Numba implementation where available. Pixel contributions are accumulated sparsely in NumPy, compacted into unique pixels and written to compressed Parquet parts, after which lower analytical zooms can be derived by successively aggregating 2 × 2 pixel blocks rather than rerasterising all of the original vessel segments.

The analytical pixel values can then be rendered into visual heatmap tiles using configurable blur, logarithmic scaling, colour and transparency. The current architecture uses memory-bounded high-zoom rendering, optional vectorised SciPy blur, parallel tile workers and a canonical MBTiles archive, from which standard XYZ PNG files can be generated independently.

The script can also preserve the underlying numeric density values as sparse Parquet data or Float32 GeoTIFFs for analytical use rather than visualisation alone. It is designed as a resumable production pipeline: analytical, rendering and export stages have separate configuration fingerprints and completion checkpoints, allowing completed work to be reused after interruption or when only later output stages need to be regenerated.

Overall, the script provides a configurable and performance-optimised way to transform large AIS datasets into reusable density products, with DuckDB, Parquet, NumPy, Numba, staged checkpoints, bounded rendering and detailed performance monitoring used to manage processing time, memory and recovery. The current implementation is centred on track_km and vessel_hours and separates the analytical calculation from visual rendering and final export.


# AIS Density Map Script: High-Level Code Blocks

- Script overview, imports and mapping constants — Lines 1–123: Describes the resumable density-map architecture, loads DuckDB, NumPy, PyArrow, Pillow, tqdm and optional Numba, SciPy, psutil and Rasterio accelerators, and defines the Web Mercator, tile-size and colour-mapping constants used throughout the pipeline.    
- Spyder settings and performance configuration — Lines 125–340: Defines the default AIS/vessel/output paths, vessel groups, density metric, speed rules, zoom range, DuckDB resources, sharding, rasterisation, rendering, GeoTIFF and output settings, together with batching, caching, threading and memory-control parameters.
- Path, naming and configuration helper functions — Lines 342–755: Provides safe SQL/string handling, compact output-path generation, dataset-name inference, analytical/render/export configuration builders and stable configuration fingerprints used to identify compatible resumable outputs.
- Run-state, checkpoint and completion management — Lines 766–1437: Implements stage-specific configuration hashes, completion markers and resumable state for segment, analytical, render and export stages, allowing the script to distinguish valid existing outputs from incomplete or incompatible runs.
- Performance profiling and resource monitoring — Lines 1439–1921: Records stage timings, CPU and memory use, process-tree resources, disk I/O, throughput and performance events, while also producing JSON, CSV and system-information diagnostics and long-run console heartbeats.
- General Parquet, coordinate and DuckDB utility functions — Lines 1925–3400: Provides file-size and Parquet metadata helpers, Web Mercator coordinate conversion, compact pixel storage, compression handling, temporary-directory management and consistent DuckDB thread, memory and spill configuration.
- Vessel metadata selection — Lines 3402–3451: Reads the vessel metadata CSV, filters to the requested COMFLEET_GROUPEDTYPE vessel groups and creates a temporary DuckDB lookup table of selected SHIP_ID values before AIS processing begins.
- AIS point staging and SHIP_ID sharding — Lines 3454–3543: Reads the source AIS Parquet files once, applies the vessel filter, selects only the columns required for density processing, converts speed into knots, validates timestamps and coordinates, and writes the retained points into Parquet datasets partitioned by SHIP_ID hash shard.
- Track-segment construction and quality filtering — Lines 3546–3728: Processes each SHIP_ID shard independently, orders positions by vessel and timestamp, creates consecutive-point segments with LAG, calculates elapsed time and great-circle distance, rejects excessive gaps and unrealistic implied speeds, applies the speed threshold and writes compact or diagnostic segment checkpoints.
- Segment checkpoint orchestration and optional parallelism — Lines 3731–3965: Reuses completed staged and segment checkpoints where possible, identifies only incomplete shards, and optionally processes pending shards in parallel worker processes with controlled DuckDB thread allocation.
- Vectorised segment rasterisation and sparse pixel-part generation — Lines 3968–4348: Streams segment checkpoints through Arrow batches, converts endpoints to Web Mercator pixels, samples each segment at the configured pixel spacing, uses Numba for the optimised rasterisation path where available, compacts duplicate pixel keys with NumPy and periodically writes sparse pixel parts to compressed Parquet.
- Pixel aggregation and analytical checkpoints — Lines 4352–4682: Combines duplicate pixel contributions with DuckDB, creates one sparse analytical pixel table per zoom, writes resumable aggregated-pixel Parquet checkpoints and can recover a trustworthy analytical layer from either internal checkpoints or existing analytical value-pixel exports.
- Analytical value outputs and GeoTIFF generation — Lines 4684–5400: Writes analytical metadata and optional compact/enriched pixel Parquet outputs, creates individual Float32 GeoTIFF tiles or composite GeoTIFFs, and uses benchmark-selected ZSTD compression, tiled storage and bounded batch/strip writing for large analytical rasters
- Density heatmap rendering and colourisation — Lines 5406–5479: Builds the Gaussian blur kernel, places analytical values into tile arrays, applies either vectorised SciPy convolution or the legacy splat method, performs logarithmic colour scaling and transparency mapping and converts the resulting RGBA arrays to PNG.
- MBTiles creation and tile-rendering infrastructure — Lines 5493–6600: Creates and validates the canonical MBTiles archive, manages SQLite performance settings, determines per-zoom colour-scale limits, expands only edge pixels required by the blur radius and prepares bounded whole-tile rendering workers
- Memory-bounded high-zoom rendering — Lines 5964–6600: For large pixel tables or high zooms, builds a compact render-source cache partitioned by tile-row bands and renders only the active band plus its blur halo, avoiding the memory cost of a global high-zoom sort.
- Zoom rendering and MBTiles persistence — Lines 6617–6985: Determines the rendering mode and stream strategy, renders tiles with bounded worker queues, colourises and PNG-encodes them, converts XYZ rows to TMS order and commits encoded PNGs to the canonical MBTiles archive in batches.
- XYZ export from MBTiles — Lines 6988–7216: Derives the file-based XYZ tile hierarchy directly from the already-encoded MBTiles PNGs, using parallel file writers and tile-column checkpoints so XYZ generation can resume independently without rerendering the density data.
- Command-line parsing and runtime options — Lines 7219–7599: Defines the PowerShell interface for AIS paths, vessel metadata, metric, vessel groups, speed and track-quality rules, zooms, DuckDB resources, rasterisation, rendering, GeoTIFF outputs, checkpoints, resumption and diagnostic behaviour.
- Main workflow, recovery and cleanup — Lines 7601–8695: Creates or resumes the named run, validates existing outputs, establishes isolated DuckDB scratch storage, profiles the run, builds/reuses segments, creates the maximum analytical zoom and derives lower zooms from it where enabled, writes analytical outputs, renders MBTiles, derives XYZ, validates completion and cleans up temporary work while preserving failure diagnostics.
- Script entry point — Lines 8697–8712: Separates Spyder-specific execution settings from the general pipeline, optionally prevents Windows system sleep during long runs, and starts run_pipeline() when the script is executed directly.

