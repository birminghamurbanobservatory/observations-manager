import {config} from '../config';

export const knex = require('knex')({
  client: 'pg',
  connection: {
    host: config.timescale.host,
    user: config.timescale.user,
    port: config.timescale.port,
    password: config.timescale.password,
    database: config.timescale.name,
    ssl: config.timescale.ssl
  },
  pool: {
    min: 2,
    max: 15 // default is 10. N.B. TimescaleDB cloud limit is 100, so you might cross it with multiple replica instances.
  }
});