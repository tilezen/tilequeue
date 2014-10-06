# Tilequeue

Queue operations to manage the processes surrounding tile rendering.

## Operations

There are several operations:

* Writing to the queue
* Reading from the queue and processing tiles
* Seeding the queue
* Generate a single tile

### Writing to the queue

This operation takes in a file, each line representing a tile, and
writes these to a queue. The format of each line should be:

    <zoom>/<column>/<row>

This is compatible with the format of the expired tilelist file that
osm2pgsql will generate. The idea is to take this file, and be able to
directly use it to populate a queue.

The format of the tiles on the queue match the format of the expired
tilelist.

### Processing the queue

This operation reads from the queue, renders the tile into several
output formats, and stores these formats on S3.

### Seeding the queue

This operation generates the initial list of jobs that need to be
performed. The strategy here is to pre-seed all tiles up to a
pre-defined zoom level, eg 10, and then seed higher zoom levels from
metro extract areas.

## Installation

    pip install Shapely protobuf
    pip install --allow-external PIL --allow-unverified PIL git+https://github.com/mapzen/TileStache@integration-1
    python setup.py develop

## Running tests

    python setup.py test

## Execution

All 3 operations support reading the aws key and secret either from
environment variables, or from command line arguments.

#### AWS Auth Environment variables

These match the python boto convention:

* AWS_ACCESS_KEY_ID
* AWS_SECRET_ACCESS_KEY

And the command line options are the environment variables in lower
case:

* aws_access_key_id
* aws_secret_access_key

#### Typical command execution

##### Write expired tiles to queue after osm diff is applied

    tilequeue write \
        --queue-name <name-of-aws-queue> \
        --expired-tiles-file <path/to/list/of/expired/tiles>

##### Read tasks from queue, and save generated tiles to S3

    tilequeue process \
        --daemon
        --queue-name <name-of-aws-queue> \
        --s3-bucket <name-of-s3-bucket> \
        --s3-path osm \
        --s3-reduced-redundancy \
        --tilestache-config <path/to/tilestache/config>

The `--daemon` flag will cause the command to loop on polling the
queue. Otherwise, the command will exit once the queue becomes empty.

By default, the output formats that will be generated are geojson and
opensciencemap. The formats can instead be specified explicitly:

    --output-formats json vtm mapbox

Multiple `tilequeue process` tasks can be run in parallel. The SQS queue
handles synchronization between the workers. All IO is blocking, and
will consist of reading tasks from SQS, querying the database, and
writing the results to S3. It's anticipated that the database would be
the bottleneck, and in that case as many workers as the database can
support should be run.

##### Seeding the queue with initial tasks

    tilequeue seed \
        --queue-name <name-of-aws-queue> \
        --zoom-until 14 \
        --filter-metro-zoom 11 \
        --unique-tiles \
        --metro-extract-url https://raw.githubusercontent.com/mapzen/metroextractor-cities/master/cities.json

This will:

* default to starting with zoom level 0 (can be modified with
  `--zoom-start <start-zoom>`)
* for zoom levels 0 -> 10, all tiles will be seeded
* for zoom levels 11 -> 14, only tiles within the metro extract bounds
  will be seeded

Note:

* Zoom levels specified for start, filtering, and end are inclusive.
* This only populates the queue with tasks; it does not perform the
  work. Use `tilequeue process` to process the tasks on the queue
  subsequently.

##### Generate a single tile

Given a coordinate on the command line, generate a tile. This can be useful to test that the processing part of the
pipeline is working correctly.

     tilequeue generate-tile \
           --tile <zoom>/<column>/<row> \
           --tilestache-config <path/to/tilestache/config> \
           --output-formats topojson \

And if want it uploaded to s3 simply add extra options

     tilequeue generate-tile \
           --tile <zoom>/<column>/<row> \
           --tilestache-config <path/to/tilestache/config> \
           --output-formats topojson \
           --s3-bucket <name-of-s3-bucket> \
           --s3-path osm \
           --s3-reduced-redundancy
