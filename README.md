# observations-manager

Saves and serves sensor observations.

Each observation is assigned to *timeseries*. A timeseries is series of observations, all with the same properties, e.g. madeBySensor, observedProperty, etc.

Using this timeseries approach in combination with [bucketing](https://www.mongodb.com/blog/post/time-series-data-and-mongodb-part-2-schema-design-best-practices) has significant performance benefits.