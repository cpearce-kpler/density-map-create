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

The AIS density-map script converts downloaded vessel-position data into visual and analytical maps of maritime activity. It reads the raw AIS Parquet files directly, filters the records to selected vessel groups using the vessel metadata file, and applies a configurable minimum-speed threshold so stopped or near-stopped vessels can be excluded.

The script can calculate three different measures of activity. track_km estimates the distance travelled through each map pixel, vessel_hours estimates the amount of time all of the vessels under analysis spent within a given pixel, and point_count counts the number of qualifying AIS observations within each pixel. For the track-based metrics, consecutive observations are ordered by vessel and time, joined into movement segments, and checked for excessive time gaps or unrealistic implied speeds before being included.

The accepted observations or track segments are converted into Web Mercator pixel coordinates at each selected zoom level. The script then accumulates the chosen metric into a sparse pixel dataset, which allows large areas to be processed without storing every empty map pixel. Work is divided into batches and vessel-based shards, while DuckDB and temporary Parquet files are used to control memory use and support large datasets.

After the analytical pixel values have been calculated, the script can create visual heatmap tiles by applying blur, logarithmic scaling, colour and transparency. These tiles can be written as standard XYZ PNG folders or packaged into a single MBTiles file for use in QGIS, web maps or other compatible applications.

The script can also preserve the underlying numeric results as sparse Parquet files or Float32 GeoTIFFs. These analytical outputs allow users to inspect, compare and perform further calculations on the actual activity values rather than relying only on the rendered heatmap colours.

Overall, the script provides a configurable way to transform raw AIS observations into reusable density products that support route analysis, vessel-activity assessment, customer visualisations and bespoke spatial requests. It operates independently of the aggregate spatial-index workflow and requires only the downloaded AIS data, vessel metadata and the settings defined in config.env.


# AIS Density Map Script: High-Level Code Blocks

- Script overview, imports and mapping constants — Lines 1–61: Describes the three supported density metrics, loads the data-processing, database, raster and image libraries, and defines the Web Mercator and Earth-measurement constants used throughout the script.

- Metric descriptions and configuration parsing helpers — Lines 64–237: Defines the meaning of each metric and provides reusable functions for reading text, Boolean, integer, decimal, list, path and analytical-zoom values from config.env.

- Configuration validation and loading — Lines 239–410: Reads all runtime settings from config.env, converts them into an argument object, and validates the selected metric, zoom range, speed threshold, processing resources and requested output formats.

- General SQL, filename and input-discovery helpers — Lines 413–434: Provides safe SQL quoting, output-name cleaning and recursive discovery of AIS Parquet and GeoParquet source files.

- Web Mercator coordinate and sparse-pixel helpers — Lines 437–527: Converts longitude and latitude into global Web Mercator pixels, samples vessel segments across those pixels, accumulates metric values and periodically writes sparse pixel parts to Parquet.

- DuckDB resource configuration — Lines 529–572: Applies the configured thread count, memory limit, temporary storage location and insertion-order settings to each DuckDB processing connection.

- Vessel metadata selection — Lines 574–639: Reads the vessel metadata CSV, standardises vessel IDs and grouped vessel types, filters to the requested vessel groups and creates a temporary lookup table for use during AIS processing.

- AIS point staging and vessel-based sharding — Lines 642–735: Reads the downloaded AIS Parquet files, joins them to the selected vessel list, converts speed into knots, validates coordinates and timestamps, and partitions the retained observations by vessel-ID hash for manageable processing.

- Track-segment construction and quality filtering — Lines 738–987: For track_km and vessel_hours, orders observations by vessel and time, connects consecutive positions, rejects long gaps or unrealistic movements, applies the inclusive speed threshold and calculates the distance or elapsed-time value assigned to each valid segment.

- Direct point-count preparation — Lines 989–1116: For point_count, reads qualifying AIS observations directly, applies the vessel-group and minimum-speed filters, assigns each observation a value of one and writes a compact staged dataset without constructing tracks.

- Metric rasterisation into pixel parts — Lines 1118–1296: Processes either valid track segments or individual AIS points in batches, converts them into pixels at each requested zoom and writes temporary sparse Parquet parts when the in-memory accumulator reaches its configured limit.

- Pixel aggregation and analytical zoom selection — Lines 1298–1351: Combines all temporary pixel parts for a zoom into one summed pixel table and determines whether analytical value outputs should be created for that zoom.

- Analytical metadata and sparse value output — Lines 1353–1462: Calculates Web Mercator resolution, writes JSON metadata describing the selected metric and filters, and exports the underlying non-zero pixel values as analytical Parquet files.

- Analytical GeoTIFF creation — Lines 1464–1833: Creates projection information and writes either individual Float32 GeoTIFF tiles or larger composite GeoTIFFs containing the underlying numeric density values.

- Analytical output coordination — Lines 1835–1879: Controls which Parquet and GeoTIFF analytical products are written for each zoom according to the configured output settings.

- Visual heatmap processing — Lines 1881–1956: Builds the blur kernel, distributes each pixel value into the surrounding image area, applies logarithmic colour scaling and transparency, and converts the rendered result into PNG data.

- MBTiles setup and visual scaling helpers — Lines 1958–2077: Creates the MBTiles database structure and metadata, calculates the colour-scale maximum for each zoom, and retrieves the source pixels required to render each map tile.

- XYZ and MBTiles tile rendering — Lines 2079–2173: Renders each populated tile, applies blur and colourisation, writes standard XYZ PNG files, inserts tiles into MBTiles using the required TMS row order and reports the total tiles created.

- Main workflow setup — Lines 2176–2245: Loads the configuration, validates input paths, reports the selected metric and speed rules, generates output names and prepares the working, XYZ and MBTiles locations.

- Metric-specific preparation and zoom processing — Lines 2246–2324: Chooses either the point-count or track-segment workflow, creates the MBTiles file when requested, processes every zoom, aggregates pixel values, writes analytical products and renders visual tiles.

- Completion reporting and cleanup — Lines 2326–2347: Closes databases, reports the generated outputs and removes temporary working files unless KEEP_WORK is enabled.

- Script entry point — Lines 2349–2350: Starts the complete density-map workflow when the Python file is executed directly.
