CHANGELOG
=========

v2.1.0
------

* Add new coanacatl driver; like the existing MVT driver, but using coanacatl instead of mapbox_vector_tile. [PR #350](https://github.com/tilezen/tilequeue/pull/350).
* Switch `whosonfirst.mapzen.com` to `whosonfirst.org`. [PR #333](https://github.com/tilezen/tilequeue/pull/333).
* Add new source for `tilezen.org`. [PR #330](https://github.com/tilezen/tilequeue/pull/330).
* Add admin areas. [PR #338](https://github.com/tilezen/tilequeue/pull/338).
* Update S3 paths [Issue #344](https://github.com/tilezen/tilequeue/issue/344).
* Add metadata to RAWR tile S3 PUTs. [Issue #345](https://github.com/tilezen/tilequeue/issue/345).
* Add optional metadata to S3 puts [PR #343](https://github.com/tilezen/tilequeue/pull/343).
* Rework so that multiple different nominal zooms can be processed at once.
* Add pyramid option to enqueue all low zoom tiles
* Add meta-tile-low-zoom command
* Support multiple RAWR tile coordinates to render.
* Add a command to render a single RAWR tile.
* Allow batch enqueue to read coords from file
* Don't start tile key name with a slash. 
* Log bucket and path on S3 errors.
* Pass through configurable object ACL.
* Add `run_id` to batch tile runs
* Allow values from the environment to override the config file, including region.
* Allow DB hosts to be a list for PostgreSQL connections. Chooses a random host for simple load balance. [PR #335](https://github.com/tilezen/tilequeue/pull/335).
* Rewrite store to use boto3 rather than boto2. This seems to solve some issues around accessing S3 buckets in different regions. [PR #334](https://github.com/tilezen/tilequeue/pull/334).
* Fix crash on using the single queue type. [PR #327](https://github.com/tilezen/tilequeue/pull/327).
* Add missing 'continue' statement after logging exception. [PR #349](https://github.com/tilezen/tilequeue/pull/349).
* Fix incorrect method call on StringIO in store. [PR #337](https://github.com/tilezen/tilequeue/pull/337).
* Pass `check-metatile-exists` value on batch enqueue. [Issue #346](https://github.com/tilezen/tilequeue/issue/346).

v2.0.7
------

* Add guard against intersection errors.

v2.0.6
------

* Add command to enqueue coords from stdin.

v2.0.5
------

* Add option to check for metatile before processing
* Add optional batch memory override

v2.0.4
------

* Add logging to batch process and skip individual failures. See [#322](https://github.com/tilezen/tilequeue/pull/322).

v2.0.3
------

* Add backoff and retry to s3 PUTs. See [#321](https://github.com/tilezen/tilequeue/pull/321).

v2.0.0
-------------

* **Release date**: 2017-12-06.
* **Requirements**: `raw_tiles v0.1`, `mapbox-vector-tile v1.2.0`.

### Summary

* New tilequeue commands have been added to help deal with common issues:
* * `status` returns information about whether the tile is present in storage, the tiles-of-interest (TOI) list and the in-flight list. This can help diagnosing issues with bad or stale tiles being returned.
* * `stuck-tiles` lists tiles which are present in storage but not in the TOI. These might be causing problems, since they will not be updated. `delete-stuck-tiles` takes the list output by `stuck-tiles` and deletes them.
* Database queries are now per-table rather than per-layer. This can help efficiency slightly, as the database may be able to optimise the number of blocks read from disk into a single pass. However, the main reason was to prepare for RAWR tiles, which are [collected into per-table files](https://github.com/tilezen/raw_tiles).
* Added support for RAWR tiles. These serialise the data for several tables into one static file, which can then be used to render tiles without needing further access to the database, which should allow it to scale more easily.
* Added support for 4x4 metatiles, with optional "1024px" size tile.
* Tilequeue process now logs in JSON format. This allows the use of some more advanced query features of the AWS CloudWatch system.
* Added support for building a Docker image of tilequeue.
* There are alternative, configurable implementations of the TOI, including all and none. The multiple queue job dispatcher can be configured to route based on TOI membership. These changes together mean it's possible to configure the cluster to run in "global build" mode, where all tiles are rendered, but the TOI decides priority, and in the normal mode, where only tiles in the TOI are rendered.

### Details

* Use pypi for mapbox-vector and edtf packages. See [#219](https://github.com/tilezen/tilequeue/pull/219).
* Update output props processing for normalized SQL. See [#221](https://github.com/tilezen/tilequeue/pull/221).
* Create source metadata based on query results. See [#222](https://github.com/tilezen/tilequeue/pull/222).
* Move kind calculation to Python. See [#223](https://github.com/tilezen/tilequeue/pull/223).
* Enable Shapely speedups when they're available. See [#224](https://github.com/tilezen/tilequeue/pull/224).
* Retry deleting tiles on S3. See [#225](https://github.com/tilezen/tilequeue/pull/225).
* Makes queries per-table. See [#227](https://github.com/tilezen/tilequeue/pull/227).
* Tidy things up for a data fetcher interface. See [#228](https://github.com/tilezen/tilequeue/pull/228).
* Fixture-based data source. See [#229](https://github.com/tilezen/tilequeue/pull/229).
* Remove hard coded list of layers in conversion. See [#231](https://github.com/tilezen/tilequeue/pull/231).
* Add a tile status command. See [#232](https://github.com/tilezen/tilequeue/pull/232).
* Move min zoom check after yaml calculation. See [#233](https://github.com/tilezen/tilequeue/pull/233).
* Support lists of templates per source. See [#234](https://github.com/tilezen/tilequeue/pull/234).
* Move `tilequeue.postgresql` -> `tilequeue.query.pool`. See [#236](https://github.com/tilezen/tilequeue/pull/236).
* Correct function used for parser. See [#237](https://github.com/tilezen/tilequeue/pull/237).
* Rename start/stop to be consistent with rest of system. See [#238](https://github.com/tilezen/tilequeue/pull/238).
* Add tilequeue command to list stuck tiles. See [#239](https://github.com/tilezen/tilequeue/pull/239).
* Add a command to process a single tile. See [#240](https://github.com/tilezen/tilequeue/pull/240).
* Add support for local fixtures. See [#241](https://github.com/tilezen/tilequeue/pull/241).
* Add option to pass all relations to fixture data fetcher. See [#242](https://github.com/tilezen/tilequeue/pull/242).
* Modify Unit of work on queue. See [#243](https://github.com/tilezen/tilequeue/pull/243).
* Add RAWR data fetcher. See [#244](https://github.com/tilezen/tilequeue/pull/244).
* Add dependency on `enum34` package to provide enumerations. See [#247](https://github.com/tilezen/tilequeue/pull/247).
* Add RAWR enqueue and process commands. See [#248](https://github.com/tilezen/tilequeue/pull/248).
* Add logging to RAWR commands. See [#249](https://github.com/tilezen/tilequeue/pull/249).
* Update sample config with RAWR options. See [#250](https://github.com/tilezen/tilequeue/pull/250).
* Update WOF process to enqueue to RAWR queue. See [#251](https://github.com/tilezen/tilequeue/pull/251).
* Remove legacy tilequeue `intersect` command. See [#252](https://github.com/tilezen/tilequeue/pull/252).
* Update name used for logging RAWR expiry and job enqueueing activity. See [#253](https://github.com/tilezen/tilequeue/pull/253).
* Remove unused commands. See [#254](https://github.com/tilezen/tilequeue/pull/254).
* Hook up RAWR data fetcher. See [#255](https://github.com/tilezen/tilequeue/pull/255).
* Add JSON logging to tilequeue processing. See [#256](https://github.com/tilezen/tilequeue/pull/256).
* Fix fixture source property. See [#257](https://github.com/tilezen/tilequeue/pull/257).
* Add stats handling. See [#258](https://github.com/tilezen/tilequeue/pull/258).
* Add `rawr-seed-toi` command. See [#259](https://github.com/tilezen/tilequeue/pull/259).
* Set default loglevel for loggers to INFO. See [#260](https://github.com/tilezen/tilequeue/pull/260).
* Emit JSON from RAWR processing logging. See [#261](https://github.com/tilezen/tilequeue/pull/261).
* Include VERSION file in package. See [#265](https://github.com/tilezen/tilequeue/pull/265).
* Add region to boto3 SQS/S3 clients. See [#266](https://github.com/tilezen/tilequeue/pull/266).
* Resolve new flake8 errors. See [#267](https://github.com/tilezen/tilequeue/pull/267).
* Log coordinates as integers. See [#268](https://github.com/tilezen/tilequeue/pull/268).
* Fix some RAWR-related TODOs. See [#269](https://github.com/tilezen/tilequeue/pull/269).
* Dockerize Tilequeue. See [#270](https://github.com/tilezen/tilequeue/pull/270).
* Use underscore as the separator instead of dash. See [#271](https://github.com/tilezen/tilequeue/pull/271).
* Handle the case where the queue returns no msgs. See [#272](https://github.com/tilezen/tilequeue/pull/272).
* Add support for local storage of RAWR tiles. See [#273](https://github.com/tilezen/tilequeue/pull/273).
* Emit more detailed timing and stats. See [#274](https://github.com/tilezen/tilequeue/pull/274).
* Log when RAWR processing starts/stops. See [#275](https://github.com/tilezen/tilequeue/pull/275).
* Correct all calls to create RAWR enqueuer's. See [#276](https://github.com/tilezen/tilequeue/pull/276).
* Add configuration comment about metatile size 2 (i.e: 512px tile support). See [#278](https://github.com/tilezen/tilequeue/pull/278).
* Updates for tilequeue processing on dev. See [#279](https://github.com/tilezen/tilequeue/pull/279).
* Remove gzip RAWR formatter. See [#280](https://github.com/tilezen/tilequeue/pull/280).
* Various RAWR tile fixes. See [#281](https://github.com/tilezen/tilequeue/pull/281).
* Add a running section to README. See [#282](https://github.com/tilezen/tilequeue/pull/282).
* Update all packages to the latest versions. See [#288](https://github.com/tilezen/tilequeue/pull/288).
* Simplify mapping from coordinate to queue message handle. See [#290](https://github.com/tilezen/tilequeue/pull/290).
* Updates for SQS message visibility handling. See [#293](https://github.com/tilezen/tilequeue/pull/293).
* Add support for configured nominal zoom to single tile processing. See [#294](https://github.com/tilezen/tilequeue/pull/294).
* Add command to enqueue randomly sampled pyramids of tiles to the render queue. See [#295](https://github.com/tilezen/tilequeue/pull/295).
* Log additional details on msg ack errors. See [#296](https://github.com/tilezen/tilequeue/pull/296).
* Simplify `_fetch_and_output` function. See [#298](https://github.com/tilezen/tilequeue/pull/298).
* Update tilequeue proc stats. See [#299](https://github.com/tilezen/tilequeue/pull/299).
* Add support to enqueue random samples to RAWR queue. See [#300](https://github.com/tilezen/tilequeue/pull/300).
* Add extra data tables for [Who's On First](https://whosonfirst.mapzen.com) neighbourhood data, [openstreetmapdata.com](https://www.openstreetmapdata.com) land and water polygons and [Natural Earth](https://naturalearthdata.com) urban area polygons. See [#301](https://github.com/tilezen/tilequeue/pull/301).
* Add additional RAWR intersector implementations. See [#302](https://github.com/tilezen/tilequeue/pull/302).
* Update RAWR intersect config parsing. See [#303](https://github.com/tilezen/tilequeue/pull/303).
* Use consistent precision for different sizes of MVT tile. Previously, all had been 4096 coordinates per tile (a.k.a. "extent"), now the 256px are 4096, 512px are 8192, and so on. See [#304](https://github.com/tilezen/tilequeue/pull/304).
* Fix some errors in workers. See [#305](https://github.com/tilezen/tilequeue/pull/305).
* Add config for urban areas data source from [Natural Earth](https://naturalearthdata.com). See [#306](https://github.com/tilezen/tilequeue/pull/306).
* Log parent if available on fetch errors. See [#308](https://github.com/tilezen/tilequeue/pull/308).
* Optionally disable 1024px tile. See [#309](https://github.com/tilezen/tilequeue/pull/309).
* Miscellaneous RAWR tile fixes. See [#310](https://github.com/tilezen/tilequeue/pull/310).
* Configurable up-zooming of tiles from Redshift. See [#311](https://github.com/tilezen/tilequeue/pull/311).
* Refactor store creation in command.py. See [#312](https://github.com/tilezen/tilequeue/pull/312).
* Filter to different queue based on TOI membership. See [#313](https://github.com/tilezen/tilequeue/pull/313).
* Move intersection to RAWR enqueue step. See [#314](https://github.com/tilezen/tilequeue/pull/314).
* Add `batch-enqueue` command to start AWS Batch jobs to render tiles, and `batch-process` command which is run in a single instance of an AWS Batch job. See [#315](https://github.com/tilezen/tilequeue/pull/315).

v1.9.1
------
* Backport fix for including VERSION file in package. See [#265](https://github.com/tilezen/tilequeue/pull/265).

v1.9.0
------
* Add additional postgres support to toi (tiles-of-interest) gardening. See [#204](https://github.com/tilezen/tilequeue/pull/204).
* Add default toi-prune cfg to fix test failures. See [#215](https://github.com/tilezen/tilequeue/pull/215).
* Reduce default queue buffer size. See [#214](https://github.com/tilezen/tilequeue/pull/214).
* Remove temporary Redis TOI dump command. See [#196](https://github.com/tilezen/tilequeue/pull/196).

v1.8.1
------
* Backport fix for including VERSION file in package. See [#265](https://github.com/tilezen/tilequeue/pull/265).

v1.8.0
------
* Move TOI from redis to s3.
* Support reading immortal tiles from s3.
* Split up process_coord into process and format.
* Create a TOI set on seed if file toi-set doesn’t exist.
* Add fix when metatiles are disabled.
* Support single file for intersect command.
* Add only valid zoom levels to TOI.

v1.7.0
------
* **New features:**
    * Add new command that 'gardens' the tiles of interest set to add and remove tiles based on various rules. See [#176](https://github.com/tilezen/tilequeue/pull/176), [#178](https://github.com/tilezen/tilequeue/pull/178), [#179](https://github.com/tilezen/tilequeue/pull/179), [#180](https://github.com/tilezen/tilequeue/pull/180), [#182](https://github.com/tilezen/tilequeue/pull/182), [#183](https://github.com/tilezen/tilequeue/pull/183), [#184](https://github.com/tilezen/tilequeue/pull/184), and [#189](https://github.com/tilezen/tilequeue/pull/189).
* **Enhancements:**
    * When enqueueing tiles of interest for seed rendering, enqueue at zoom level 15 instead of 16. See [#181](https://github.com/tilezen/tilequeue/pull/181).
    * Add optional support for `statsd` in command.py. See [#185](https://github.com/tilezen/tilequeue/pull/185).

v1.6.0
------
* **New features:**
    * Add support for 2x2 metatiles (and 512px tiles). See [#163](https://github.com/tilezen/tilequeue/pull/163), [#166](https://github.com/tilezen/tilequeue/pull/166), and [#169](https://github.com/tilezen/tilequeue/pull/169).
    * Cut child 256px tiles from 512px parent in the 2x2 metatile, rather than re-requesting that 256px bbox from database. See [#158](https://github.com/tilezen/tilequeue/pull/158).
    * Pass nominal zoom instead of coordinates. See [#161](https://github.com/tilezen/tilequeue/pull/161).
* **Enhancements:**
    * Drop parts of MultiPolygons which lie outside the clip boundary of the tile (primarily affects buildings and water layers). See [#171](https://github.com/tilezen/tilequeue/pull/171).
    * Make queue sizes configurable, and default to smaller queue size to accomodate larger 2x2 metatiles. See [#172](https://github.com/tilezen/tilequeue/pull/172).
    * Move existing tiles of interest (TOI) instead of copying it to avoid AWS Redis failover. See [#122](https://github.com/tilezen/tilequeue/pull/122).
    * Load new TOI from file 'toi.txt', just as the TOI dump process saves to 'toi.txt'. See [#122](https://github.com/tilezen/tilequeue/pull/122).
* **Bug fixes:**
    * Delete rejected jobs from SQS queue. See [#173](https://github.com/tilezen/tilequeue/pull/173).
    * Trap MemoryError and let ops recover process. See [#174](https://github.com/tilezen/tilequeue/pull/174).
    * Fix LinearRing error. See [#175](https://github.com/tilezen/tilequeue/pull/175).

v1.5.0
------
* Emit additional metrics during intersection

v1.4.0
------
* When checking to see if a tile has changed, compare ZIP file contents only. (See [#152](https://github.com/tilezen/tilequeue/issues/152))
* On WOF neighbourhood update, return a better error message for invalid dates. (See [#154](https://github.com/tilezen/tilequeue/pull/154))
* Remove "layers to format" functionality. (See [#155](https://github.com/tilezen/tilequeue/pull/155))

v1.3.0
------
* Roll back the use of psycopg2 connection pools. (See [#149](https://github.com/tilezen/tilequeue/pull/149))

v1.2.1
------
* Fix connection leak: Track connections before trying to use them. (See [#150](https://github.com/tilezen/tilequeue/pull/150))
* Fix issue processing WOF data with z coordinates: Ignore extra coordinates, e.g: z, in reprojection. (See [#148](https://github.com/tilezen/tilequeue/pull/148))

v1.2.0
------
* Improve performance of GeoJSON and TopoJSON format generation by using ujson (See [#139](https://github.com/tilezen/tilequeue/issues/139))
* Improve performance of MVT format generation by using builtin round function (See [#144](https://github.com/tilezen/tilequeue/pull/144))
* Reduce database pressure by use psycopg2 connection pools (See [#141](https://github.com/tilezen/tilequeue/issues/141))
* Reduce database requests by registering hstore/json (See [#142](https://github.com/tilezen/tilequeue/issues/142))
* Reduce memory usage during tile seeding (See [#126](https://github.com/tilezen/tilequeue/issues/126))
* Expose unique option for seeding tile generator (See [#127](https://github.com/tilezen/tilequeue/issues/127))
* Support long zooms (See [#130](https://github.com/tilezen/tilequeue/pull/130))

v1.1.1
------
* Use queue name from message to acknowledge message. See [#134](https://github.com/tilezen/tilequeue/issues/134).

v1.1.0
------
* Add command to dump the tiles of interest list to a text file
* Add support for generating metatiles (see also: tapalcatl)
* Add priority queues implementation
* Increase topojson scale from 1024 -> 4096

v1.0.1
------
* Add bbox filter to test for partial overlapping, rather than intersecting, the bbox.

v1.0.0
------
* Update wof projection to 3857.
* Update srid throughout from 900913 -> 3857.
* Add basic size logging for the objects returned from queries.
* Support multiple geometries in queries.
* Add filter to intersect with padded bounds.
* Conditionally store tile data. Print total storage counts for each tile.
* Correct the buffered mvt format definition.
* Convert wof min/max zooms to floats.
* Update sample cfg to reflect latest choices.
* Add pyclipper dependency to requirements.

v0.10.0
-------
* Improve json encoding
* Add boroughs to wof processing
* Add support for configurable area thresholds
* Add l10n names to wof processing
* Port used TileStache fork code to eliminate dependency
* Add configurable bounds buffer support
* Remove simplification, now a vector-datasource transform step
* Ensure json properties are utf-8 encoded
* Add support to generate s3 urls with no path
* Handle empty strings when edtf parsing wof dates

v0.9.0
------
* Delegate quantization to mapbox-vector-tile. See [#82](https://github.com/mapzen/tilequeue/issues/82).
* Expand mz_properties in features. See [#81](https://github.com/mapzen/tilequeue/pull/81).
* Exclude null values from yaml output. See [#84](https://github.com/mapzen/tilequeue/pull/84).
* Remove outdated tests exercising a transform that is no longer used. See [6de8f00](https://github.com/mapzen/tilequeue/commit/6de8f00579840794bdb7febd4e113a5cd976421a).

v0.8.0
------
* Use an empty list if the 'transforms' parameter is missing, rather than raise KeyError.
* Metatile at z16
* Support storing individual formatted layers
* Pass context object to post-process functions.
* Add resource abstraction to manage transforms with io requirements
* Don't filter out small features at z16, which may be needed for subsequent zooms

v0.7.1
------
* Eliminate extra slash in S3 filename. [Issue](https://github.com/mapzen/tilequeue/pull/65).
* Update `make_queue` signature to support sending items to the queue for rendering when a request for a tile which hasn't been rendered before is received. [Issue](https://github.com/mapzen/tilequeue/pull/66) as part of [larger issue](https://github.com/mapzen/tile-tasks/issues/39).

v0.7.0
------
* WOF neighbourhoods with inception and cessation dates are now respected, with those features being hidden from tiles. [Issue](https://github.com/mapzen/tilequeue/issues/59).
* The WOF update process is now robust to intermittent HTTP errors. This could help if you've been experiencing occasional WOF update failures. [Issue](https://github.com/mapzen/vector-datasource/tilequeue/60).

v0.6.1
------
* Made the WOF processor robust to missing `lbl:longitude` and `lbl:latitude` properties by falling back to the geometry's position. This isn't as good as the label position, but better than erroring.

v0.6.0
------
* Added a date prefix to be used to distinguish between different versions of the tiles in the store. This means it's not necessary to create a new bucket for each release.
* Added a hash prefix for files stored in an S3 bucket. This is recommended practice for distributing load for a bucket across an S3 cluster.

v0.5.1
------
* Move reproject_lnglat_to_mercator function for outside usage

v0.5.0
------
* Configure better defaults in sample config
* Add WOF neighbourhood processing
  - add command to load initial neighbourhoods to database
  - add command to track updates
* Create abstraction to factor out threaded enqueueing

v0.4.1
------
* Add bbox_intersection filter for Jinja, which allows clipping to the query bounding box.

v0.4.0
------
* Convert post process config into a list, to support generating dynamic `label_placement`s for more than one input layer.

v0.3.0
------
* Add read_tile to store interface

0.2.0
-----
* Stable
