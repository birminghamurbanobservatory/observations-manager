# observations-manager

Saves and serves sensor observations.

Each observation is assigned to *timeseries*. A timeseries is series of observations, all with the same properties, e.g. madeBySensor, observedProperty, etc.

Using this timeseries approach in combination with a TimescaleDB [hypertable](https://docs.timescale.com/latest/using-timescaledb/hypertables) in the hope of significant performance benefits.