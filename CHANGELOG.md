CHANGELOG
=========

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
