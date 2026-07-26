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

Set `KEEP_WORK=true` only when intermediate DuckDB and Parquet files are needed for debugging. These files can be large.
