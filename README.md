# tilequeue

A tile generator, used by itself for asyncronous tile generation or with [tileserver](https://github.com/tilezen/tileserver/) for serving tiles.

## Installation

We recommend following the vector-datasource [installation instructions](https://github.com/tilezen/vector-datasource/wiki/Mapzen-Vector-Tile-Service). 

_Note: Installation has been tested using Python 2.7 and results with other versions may vary._

In addition to the dependencies in [requirements.txt](requirements.txt), tileserver requires

* PostgreSQL client-side development library and headers (for psycopg)
* GEOS library

These can be installed on Debian-based systems with
```
sudo apt-get install libpq-dev libgeos-c1v5
```

Then install the python requirements with

    pip install -Ur requirements.txt

Then:

    python setup.py develop

## Configuration

See [`config.yaml.sample`](https://github.com/tilezen/tilequeue/blob/master/config.yaml.sample)

## Layer definitions

To understand the language tilequeue layer definitions, it's best to look at the [Tilezen vector-datasource](https://github.com/tilezen/vector-datasource)

## License

Tilequeue is available under [the MIT license](https://github.com/tilezen/tilequeue/blob/master/LICENSE.txt).
