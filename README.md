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

### Coanacatl

Note that if you want to configure the `coanacatl` format, you will need to install the [coanacatl](https://github.com/tilezen/coanacatl) library. This is totally optional and tilequeue will work fine with the regular `mvt` format, but can provide some robustness and speed improvements.

## Configuration

See [`config.yaml.sample`](https://github.com/tilezen/tilequeue/blob/master/config.yaml.sample)

## Layer definitions

To understand the language tilequeue layer definitions, it's best to look at the [Tilezen vector-datasource](https://github.com/tilezen/vector-datasource)

## Running

A list of commands is available by running `tilequeue --help`. Each command also supports usage information by running `tilequeue <CMD> --help`. All commands require a configuration file to be passed through the `--config` argument. A brief summary of commands:

* `process`: Start the tilequeue worker process, reading jobs from the queue and processing them.
* `seed`: Enqueue the tiles defined in the `tiles/seed` section of the config, and (if configured) add them to the TOI.
* `dump-tiles-of-interest`: Write out the TOI to `toi.txt`.
* `load-tiles-of-interest`: Replace the TOI with the contents of `toi.txt`.
* `enqueue-tiles-of-interest`: Enqueue the TOI as a set of jobs.
* `prune-tiles-of-interest`: Prune the TOI according to the rules in the `toi-prune` section of the config.
* `wof-process-neighbourhoods`: Fetch the latest WOF neighbourhood data and update the database, enqueueing jobs for any changes.
* `wof-load-initial-neighbourhoods`: Load WOF neighbourhood data into the database.
* `consume-tile-traffic`: Read tile access log files and insert corresponding records into a PostgreSQL compatible database (we use AWS Redshift).
* `stuck-tiles`: Find tiles which exist in the store, but are not in the TOI. These won't be updated when the data changes, so should be deleted. Tiles can become stuck due to race conditions between various components, e.g: a tile being dropped from the TOI while its job is still in the queue. Outputs a list of tiles to `stdout`.
* `delete-stuck-tiles`: Read a list of tiles from `stdin` and delete them. Designed to be used in conjunction with `stuck-tiles`.
* `rawr-process`: Read from RAWR tile queue and generate RAWR tiles.
* `rawr-seed-toi`: Read the TOI and enqueue the corresponding RAWR tile jobs.
* `tile-status`: Report the status of the given tiles in the store, queue and TOI.
* `tile`: Render a single tile.
* `rawr-enqueue`: Enqueue RAWR tiles corresponding to expired tiles.

### Testing

You can run the tests with the command `python setup.py test` in the top level source directory.

### Code style

We use `flake8` to check our source code is PEP8 compatible. You can run this using the command:

```
find . -not -path '*/.eggs/*' -not -path '*OSciMap4*' -name '*.py' | xargs flake8
```

You might find it useful to add that as a git pre-commit hook, or to run a PEP8 checker in your editor.

### Profiling

A great way to get a high level view of the time consumed by the code is to run it via [`python-flamegraph`](https://github.com/evanhempel/python-flamegraph), which produces a profile suitable for processing into an SVG using another tool called [FlameGraph](http://www.brendangregg.com/flamegraphs.html). For example, to run a graph for a single tile:

```
python -m flamegraph -o perf.log `which tilequeue` tile --config config.yaml 10/163/395
flamegraph.pl --title "Tilequeue 10/163/395" perf.log > perf.svg
```

Note that you may need to add the path to `flamegraph.pl` from Brendan Gregg's repository if you haven't installed it in your `$PATH`.

## License

Tilequeue is available under [the MIT license](https://github.com/tilezen/tilequeue/blob/master/LICENSE.txt).
