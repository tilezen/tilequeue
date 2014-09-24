# Tilequeue

Queue operations to manage the processes surrounding offline tile
generation.

## Operations

There are 3 operations:

* Writing to the queue
* Reading from the queue
* Seeding the queue

### Writing to the queue

This operation takes in a file, each line representing a tile, and
writes these to a queue. The format of each line should be:

    <zoom>/<column>/<row>

This is compatible with the format of the expired tilelist file that
osm2pgsql will generate. The idea is to take this file, and be able to
directly use it to populate a queue.

The format of the tiles on the queue match the format of the expired
tilelist.

### Reading from the queue

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

    queue-write \
        --queue-name <name-of-aws-queue> \
        --expired-tiles-file <path/to/list/of/expired/tiles>

    queue-read \
        --queue-name <name-of-aws-queue> \
        --s3-bucket <name-of-s3-bucket> \
        --tilestache-config <path/to/tilestache/config> \
        --s3-reduced-redundancy

    queue-seed \
        --queue-name <name-of-aws-queue> \
        --zoom-until 14 \
        --filter-metro-zoom 11 \
        --unique-tiles \
        --metro-extract-url https://raw.githubusercontent.com/mapzen/metroextractor-cities/master/cities.json
