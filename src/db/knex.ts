import {config} from '../config';

export const knex = require('knex')({
  client: 'pg',
  connection: {
    host: config.timescale.host,
    user: config.timescale.user,
    password: config.timescale.password,
    database: config.timescale.name
  }
});