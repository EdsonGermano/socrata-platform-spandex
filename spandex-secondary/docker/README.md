# Secondary Watcher docker support

The files in this directory allow you to build a docker image for the Spandex secondary watcher.  
The spandex secondary assembly must be copied to `spandex-secondary-assembly.jar` in this directory before building.

## Required Runtime Variables

All variables required by [the data-coordinator secondary watcher base image](https://github.com/socrata/data-coordinator/tree/master/coordinator/docker-secondary-watcher#required-runtime-variables)
are required.  

In addition, the following are required:

* `SPANDEX_SECONDARY_ES_HOST` - The hostname of the Elasticsearch server

## Optional Runtime Variables

All optional variables supported by [the data-coordinator secondary watcher base image](https://github.com/socrata/data-coordinator/tree/master/coordinator/docker-secondary-watcher#optional-runtime-variables)
are supported.  

In addition, the following optional variables are supported.  For defaults, see the [Dockerfile](Dockerfile).

* `SPANDEX_SECONDARY_ES_CLUSTER_NAME` - Name of the cluster to connect to.
* `SPANDEX_SECONDARY_ES_PORT` - The port number of the Elasticsearch server.
* `SPANDEX_SECONDARY_ES_INDEX_ALIAS` - The index/alias in Elasticsearch in which to maintain documents.
* `SPANDEX_SECONDARY_NUM_WORKERS` - Number of workers to run per secondary instance.
* `SPANDEX_SECONDARY_TYPE` - The secondary type to use, controls what `store_id` we do work for.
* `LOG_LEVEL` - The log4j log level to emit messages from all classes in `com.socrata.spandex` package, only applicable when using docker command run-with-log-level.
