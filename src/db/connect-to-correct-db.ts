import {config} from '../config';
import * as logger from 'node-logger';


// Guide: https://stackoverflow.com/questions/27154167/create-drop-database-task-for-gulp-knex

export async function connectToCorrectDb(): Promise<void> {

  logger.debug(`About to try connecting to a database called '${config.timescale.name}'.`);

  // Using the timescaledb credentials from config lets begin by trying to connect to the database we want to.
  const knex1 = require('knex')({
    client: 'pg',
    connection: {
      host: config.timescale.host,
      user: config.timescale.user,
      password: config.timescale.password,
      database: config.timescale.name
    }
  });


  let needToCreateCorrectDb;
  try {

    const result = await knex1.raw('SELECT current_database()');
    const returnedDbName = extractDbNameFromRawResult(result);
    if (returnedDbName === config.timescale.name) {
      logger.debug(`Successfully able to connect to the correct timescale database (${config.timescale.name}).`);
      return;
    } else {
      // It would be unusual to reach this
      throw new Error(`The result of the 'SELECT current_database()' query was ${returnedDbName}, but we were expecting ${config.timescale.name}`);
    }  

  } catch (err) {

    if (err.code === '3D000') {
      // If we get here then chances are the timescaledb instance has only just been created, it only has the default database named 'postgres'. Thus we'll want to create our own.
      needToCreateCorrectDb = true;
    } else {
      throw err;
    }

  }


  if (needToCreateCorrectDb) {
    
    logger.warn(`A database called ${config.timescale.name} does not exist yet so lets try creating one. But first we must connect to the default 'postgres' database.`);

    await knex1.destroy();
    const knex2 = require('knex')({
      client: 'pg',
      connection: {
        host: config.timescale.host,
        user: config.timescale.user,
        password: config.timescale.password,
        database: 'postgres' // i.e. the default
      }
    });

    // Check we can connect to the postgres database
    try {
      await knex2.raw('SELECT current_database()');
    } catch (err) {
      throw new Error(`Failed to even connect to the 'postgres' database. Reason: ${err.message}.`);
    }

    // Create the new database
    try {
      await knex2.raw(`CREATE DATABASE ${config.timescale.name}`);
      logger.debug(`Database called ${config.timescale.name} has been created.`);
    } catch (err) {
      throw new Error(`Failed to create a database called ${config.timescale.name}. Reason: ${err.message}`);
    }

    // Now we can destroy the postgres database connection
    await knex2.destroy();

    // Now the credentials in our config should work
    const knex3 = require('knex')({
      client: 'pg',
      connection: {
        host: config.timescale.host,
        user: config.timescale.user,
        password: config.timescale.password,
        database: config.timescale.name
      }
    });

    const result3 = await knex3.raw('SELECT current_database()');
    logger.debug(`The currently selected database is now: '${extractDbNameFromRawResult(result3)}'.`);

  }

  return;

}


function extractDbNameFromRawResult(result: any): string {
  try {
    return result.rows[0].current_database;  
  } catch (err) {
    throw new Error(`Could not extract database name from raw result. Reason: ${err.message}.`);
  }
}