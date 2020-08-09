
# Qserv KPM test harness

This package contains Python code and configuration examples for KPM testing
of QServ cluster. It is used in implementation of command line application
`qserv-kraken` (lives in `admin/bin/`) which is the main tool for KPM testing.
Folder `config` in this package contains an example of the configuration file
that can be used for running `qserv-kraken`.

`qserv-kraken` runs multiple "clients", each client generating queries of
specific query class and executing them sequentially. Total number of clients
is determined by configuration (sum of all `concurrentQueries` values), and it
is typically around 100. Every client in this implementation runs in a
separate sub-process, all those sub-processes are started and managed by
`qserv-kraken`. Depending on hardware it may be better to run the whole
shebang on several hosts, `qserv-kraken` provides simple way to do that.


## Configuration

The queries that need to be executed, number of clients and few other
parameters are specified in the YAML configuration file, here is the
description of its contents.

At the top level config file has two keys:

- `queryClasses` -- defines execution policies for different classes of
  queries,
- `queries` -- defines queries to be executed.

### `queryClasses` tree

The next level in `queryClasses` defines a bunch of keys corresponding to
query classes ("LV", "NearN", etc.)

For each query class next level of the tree defines parameters for query
execution with these keys:

- `concurrentQueries` -- (`int`) number of concurrent queries to run, which is
  also a number of sub-processes that will be executing them;
- `targetTime` -- target time for query execution, this is not used currently;
- `maxRate` -- (`float`, optional) maximum query submission rate in Hz for
  queries of this class for single client, if not specified or `null` then
  rate is not limited;
- `arraysize` -- (`int`, optional) number of result rows to fetch at once with
  `fetchmany()` DBAPI call, if not specified or `null` then driver default is
  used.

### `queries` tree

The next level in `queries` tree specifies keys for query classes. These are
the same classes as defined in `queryClasses` tree, but it is not required to
be a full set of classes. A subset of the class names can be used in `queries`
tree and only these classes will be selected for execution.

Next level in `queries` tree contains query definitions. Each query defined
there has a unique key which can be an arbitrary name (e.g. "q1", "q42"). This
key is used to identify queries in the log and in monitoring output. The value
for the query definition is either a fixed query string (e.g. "SELECT * FROM
Object") or a definition of a template used to generate queries. Template
definition has these keys:

- `template` -- (`str`) text of a query which include template variables
  enclosed into curly braces ("SELECT ... WHERE ra > {ra_min}");
- `variables` -- definition of one or more template variables.

In the definition of template variables the name of a variable is a key, and
the value is a configuration of how to generate values for this variable.
Currently there are two supported specifications:

- Random choice of integer numbers that are read from a file, this is going to
  be used for random objectIds. This specification has configuration with two
  keys:
  - `path` -- (`str`) the name of a file to read numbers from, this is a text
    files with a bunch of numbers separated by spaces and/or newlines.
  - `mode` -- can be "random" or "sequential", default is "random"; with
    random mode numbers are drawn from the pool randomly, with sequential mode
    they are drawn sequentially with rollover.
- Randomly generated numbers from a specified distribution. Configuration for
  this has the key `distribution` which specifies the type of distribution,
  currently supported are `uniform` and `uniform_int`. For both of these the
  keys `min` and `max` specify parameters of the uniform distribution.

File `config/common.yaml` contains an example all possible cases.

`qserv-kraken` accepts multiple configuration files, if there is more than one
file is provided their contents is merged and parameters from later files
override parameters from earlier files. This is a convenient way to specify
overrides without updating common shared configuration.


## How to run tests

In addition to configuration file `qserv-kraken` accepts a bunch of command
line parameters:

- database connection options (host, port, user name, etc.);
- execution options for time limit and splitting workload into multiple jobs;
- options for saving monitoring information.

Database connection options are standard for mysql type of database connection.


### Execution options

If no special options are provided then `qserv-kraken` will run as many
sub-processes as specified by configuration file. Depending on configuration
this could overload typical small scale system so it may be necessary to split
whole workload into smaller units, each executed on separate machine.
`qserv-kraken` can deterministically spit configuration into a fixed number of
slots and run queries from a single slot only. Option `--num-slots` defines
how many slots the workload needs to be divided into, and option `--slot`
specifies slot number (in range 0 to `num_slots - 1`) to use for execution.

Simple example, suppose that `config.yaml` defines full configuration with 100
parallel queries and you want to run it on 3 different hosts. This is how it is
going to look like:

- `ssh host1 "qserv-kraken --num-slots=3 --slot=0 config.yaml"`
- `ssh host2 "qserv-kraken --num-slots=3 --slot=1 config.yaml"`
- `ssh host3 "qserv-kraken --num-slots=3 --slot=2 config.yaml"`

(first one of the tree will run 34 parallel queries and remaining two will run
33 parallel queries each).

By default `qserv-kraken` runs forever or until it is killed. To limit for how
long it should run use `-t` or `--time-limit` option which accepts max. number
of seconds to run for.


### Monitoring options

`qserv-kraken` produces monitoring information for executing queries, by
default this information is not published anywhere. If `--monitor=log` option
is given then all monitoring information is dumped to Python logger with the
name `metrics`.

To save monitoring metrics in the format that can be ingested into InfluxDB
use option `--monitor=influxdb-file`. Default template for file name for
saving that output is `qserv-kraken-mon-%S-%T.dat` where `%T` is replaced with
a timestamp when the file is open and `%S` is replaced with slot number.
`qserv-kraken` re-opens new output file every hour, this could be changed with
option `--monitor-rollover`. InfluxDB files contain the name of the database
to ingest the data into, default name for that is `qserv_kraken` and it can be
changed with `--influxdb-db` option.


## Running on verification cluster

Natural way to run this at NCSA is probably at verification cluster with slurm
batch system. There are possible different approaches to running multiple jobs
at the same time:

- submit individual single-task jobs separately providing different `--slot`
  parameters to each job,
- submit one job consisting of multiple tasks and figuring out slot number
  from slurm environment.

Latter option is supported by `qserv-kraken` with a special command line
option `--slurm`. When this option is passed it uses slurm environment
variables `SLURM_NTASKS` and `SLURM_NODEID` (see
https://slurm.schedmd.com/srun.html). For this to work correctly one has to
make sure that slurm allocates whole node per task, this can be done in
different ways, one of them is to pass `--ntasks` and `--nodes` options with
the same values to sbatch/srun.

Here is an example of a script for sbatch command:

    #!/bin/bash -l

    #SBATCH -p normal
    #SBATCH -J kraken
    #SBATCH --ntasks=4
    #SBATCH --nodes=4
    #SBATCH -t 2:00:00

    srun --ntasks=4 --nodes=4 qserv-kraken --slurm -t 3600 -v config.yaml
