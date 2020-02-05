import {knex} from './knex';
import {Promise} from 'bluebird';
import * as logger from 'node-logger';
import {createObservationsTable} from '../components/observation/observation.service';
import {createTimeseriesTable} from '../components/timeseries/timeseries.service';
import {connectToCorrectDb} from './connect-to-correct-db';
import {createLocationsTable} from '../components/location/location.service';


export async function initialiseDb(): Promise<void> {

  // If the database (e.g. called observations manager) doesn't already exist then you'll want to create it. This is easier said than done as you'll need to reintialise knex with the database as the default 'postgres', then do a knex.raw CREATE DATABASE, then change back to using this new database name.
  try {
    await connectToCorrectDb();
  } catch (err) {
    throw new Error(`Failed to connect to correct database: ${err.message}.`);
  }

  // Check we can actually reach the database, if not log the error.
  try {
    await getTables();
    logger.debug('Successfully reached database during initialisation');
  } catch (err) {
    throw new Error(`Failed reach database whilst initialising it. Reason: ${err.message}`);
  }
  
  // Check that the extensions we need are installed
  const timescaledbExtensionInstalled = await checkForExtension('timescaledb');
  if (!timescaledbExtensionInstalled) {
    await knex.raw('CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;');
    logger.info('Timescaledb extension installed, because it was not yet installed.');
  }

  const postgisExtensionInstalled = await checkForExtension('postgis');
  if (!postgisExtensionInstalled) {
    await knex.raw('CREATE EXTENSION postgis');
    logger.info('postgis extension installed, because it was not yet installed.');
  }
  
  const ltreeExtensionInstalled = await checkForExtension('ltree');
  if (!ltreeExtensionInstalled) {
    await knex.raw('CREATE EXTENSION ltree');
    logger.info('ltree entension installed, because it was not yet installed.');
  }  
  
  // The order here is important, as some tables depend on others.
  const tables = [
    {
      name: 'timeseries',
      itsCreateFunction: createTimeseriesTable
    },
    {
      name: 'locations',
      itsCreateFunction: createLocationsTable
    },
    {
      name: 'observations',
      itsCreateFunction: createObservationsTable
    }
  ];

  // It's important that this is mapSeries and not map, in case some tables must be created before others.
  await Promise.mapSeries(tables, async (table): Promise<void> => {
    await createTableIfMissing(table.name, table.itsCreateFunction);
  });


  return;

}


async function createTableIfMissing(tableName: string, createTableFunction: () => void): Promise<void> {
  
  const tableExists = await knex.schema.hasTable(tableName);

  if (tableExists) {
    return;
  } else {
    logger.debug(`Creating ${tableName} table, because it does not exist yet.`);
    await createTableFunction();
    return;
  }

}


async function checkForExtension(extName): Promise<boolean> {

  const extension = await knex.select('extname')
  .from('pg_extension')
  .where('extname', extName)
  .first();

  return Boolean(extension);

}


async function getTables(): Promise<any[]> {
  const tables = await knex.select('table_name')
  .from('information_schema.tables')
  .where('table_schema', 'public')
  .andWhere('table_type', 'BASE TABLE');  
  return tables;
}


async function isTableEmpty(tableName: string): Promise<boolean> {
  const result = await knex.raw(`SELECT COUNT(*) FROM ${tableName}`);
  return result.rows[0].count === '0';
}



