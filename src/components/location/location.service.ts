import {knex} from '../../db/knex';
import {LocationClient} from './location-client.class';
import {LocationApp} from './location-app.class';
import {cloneDeep} from 'lodash';
import {LocationNotFound} from './errors/LocationNotFound';
import {GetLocationByClientIdFail} from './errors/GetLocationByClientIdFail';
import {convertKeysToCamelCase, convertKeysToSnakeCase} from '../../utils/case-converters';
import {stripNullProperties} from '../../utils/strip-null-properties';
import {LocationDb} from './location-db.class';
import {CreateLocationFail} from './errors/CreateLocationFail';
import {v4 as uuid} from 'uuid';



export async function createLocationsTable(): Promise<void> {

  await knex.schema.createTable('locations', (table): void => {

    table.bigIncrements('id');
    table.text('client_id').unique().notNullable(); // the unique method here should create an index
    table.specificType('geo', 'GEOGRAPHY').notNullable(); // defaults to SRID=4326, and can handle locations with elevation
    table.jsonb('geojson').notNullable();
    table.timestamp('valid_at', {useTz: true}).notNullable();

  });

  await knex.raw('CREATE INDEX locations_geog_index ON locations USING GIST (geo);');

  return;
}


export async function getLocationByClientId(clientId: string): Promise<LocationApp> {

  let foundLocation;
  try {
    const result = await knex('locations')
    .select()
    .where({
      client_id: clientId
    });
    foundLocation = result[0];
  } catch (err) {
    throw new GetLocationByClientIdFail(undefined, err.message);
  }

  if (!foundLocation) {
    throw new LocationNotFound(`Failed to find a location with client id '${clientId}'`);
  }

  return locationDbToApp(foundLocation);  

}


export async function createLocation(location: LocationApp): Promise<LocationApp> {

  // If by this point the location still doesn't have a client_id, then let's assign one here.
  if (!location.clientId) {
    location.clientId = uuid();
  }

  const locationDb = locationAppToDb(location);

  let createdLocation: LocationDb;
  try {
    const result = await knex('locations')
    .insert({
      id: locationDb.id,
      client_id: locationDb.client_id,
      geojson: locationDb.geojson,
      geo: knex.raw(`ST_GeomFromGeoJSON('${JSON.stringify(locationDb.geo)}')::geography`),
      valid_at: locationDb.valid_at
    })
    .returning('*');
    createdLocation = result[0];
  } catch (err) {
    throw new CreateLocationFail(undefined, err.message);
  }  

  return locationDbToApp(createdLocation);

} 


export function locationClientToApp(locationClient: LocationClient): LocationApp {
  const locationApp: any = cloneDeep(locationClient);
  locationApp.clientId = locationClient.id;
  if (locationApp.validAt) {
    locationApp.validAt = new Date(locationApp.validAt);
  }
  delete locationApp.id;
  return locationApp;
}


export function locationAppToClient(locationApp: LocationApp): LocationClient {
  const locationClient: any = cloneDeep(locationApp);
  locationClient.id = locationClient.clientId;
  delete locationClient.clientId;
  return locationClient;
}


export function locationDbToApp(locationDb: LocationDb): LocationApp {
  const locationApp: any = convertKeysToCamelCase(stripNullProperties(locationDb));
  locationApp.geometry = locationDb.geojson;
  delete locationApp.geo;
  delete locationApp.geojson;
  return locationApp;
}


export function locationAppToDb(locationApp: LocationApp): LocationDb {
  const locationDb: any = convertKeysToSnakeCase(locationApp);
  locationDb.geo = locationDb.geometry;
  locationDb.geojson = locationDb.geometry;
  delete locationDb.geometry;
  return locationDb;
}