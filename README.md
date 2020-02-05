# observations-manager

Saves and serves sensor observations.

Each observation is assigned to *timeseries*. A timeseries is series of observations, all with the same properties, e.g. madeBySensor, observedProperty, etc.

Using this timeseries approach in combination with a TimescaleDB [hypertable](https://docs.timescale.com/latest/using-timescaledb/hypertables) in the hope of significant performance benefits.


## Environment variables

For the timescaledb environment settings it's primarily as simple as just pasting over the settings from the [portal](https://portal.timescale.cloud/). 

Note how I have a `TIMESCALE_DEFAULT_DB_NAME` variable. This is the name of the database created by default. For example the timescaledb docker image creates a database called _postgres_ by default, whereas Timescale's cloud service creates one called _defaultdb_. Even if you use a different database name to these defaults it's still worth listing the default name so that knex can try connecting to the default and then once connected it will create the new database with the name you want.
