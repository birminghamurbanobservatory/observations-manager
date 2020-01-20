import {ObservationCore} from './observation-core.class';
import {ObservationApp} from './observation-app.class';
import {pick, cloneDeep, concat} from 'lodash';
import {TimeseriesApp} from '../timeseries/timeseries-app.class';
import {TimeseriesProps} from '../timeseries/timeseries-props.class';
import {ObservationClient} from './observation-client.class';
import {ObservationDb} from './observation-db.class';
import {GetObservationByIdFail} from './errors/GetObservationByIdFail';
import {ObservationNotFound} from './errors/ObservationNotFound';
import {ObservationAlreadyExists} from './errors/ObservationAlreadyExists';
import {knex} from '../../db/knex';
import * as check from 'check-types';
import {CreateObservationFail} from './errors/CreateObservationFail';
import {GetObservationsFail} from './errors/GetObservationsFail';


export async function createObservationsTable(): Promise<void> {

  await knex.schema.createTable('observations', (table): void => {

    table.specificType('id', 'BIGSERIAL'); // Don't set this as primary or else create_hypertable won't work.
    table.string('timeseries', 24).notNullable(); // 24 is the length of a Mongo ObjectID string
    table.timestamp('result_time', {useTz: true}).notNullable();
    table.bigInteger('location');
    table.specificType('value_number', 'numeric');
    table.boolean('value_boolean');
    table.text('value_text');
    table.jsonb('value_json');
    table.specificType('flags', 'text ARRAY'); // https://www.postgresql.org/docs/9.1/arrays.html

    table.foreign('location')
    .references('id')
    .inTable('locations')
    .withKeyName('location_id');

  });

  // Create the hypertable
  await knex.raw(`SELECT create_hypertable('observations', 'result_time');`);
  // TODO: By default this will create an index called observations_result_time_idx, should I delete this given that I'll create one below that also uses the timeseries and will be used instead anyway? 
  // N.B. if you want a custom primary key, or a unique index, then you must include the result_time.
  // docs: https://docs.timescale.com/latest/using-timescaledb/schema-management#indexing
  await knex.raw('CREATE UNIQUE INDEX timeseries_spatiotemporal_uniq_idx ON observations (timeseries, result_time, location DESC)');
  // N.B. we need the COALESCE for instances when no location is provided, i.e. the column stores a NULL value. Without it two identical locationless observations wouldn't trigger a duplicate error, because NULL counts as distinct each time.

  return;
}



export async function saveObservation(obsCore: ObservationCore, timeseriesId: number): Promise<any> {

  const observationDb = buildObservationDb(obsCore, timeseriesId);

  let createdObservation: ObservationDb;
  try {
    const result = await knex('observations')
    .insert(observationDb)
    .returning('*');
    createdObservation = result[0];
  } catch (err) {
    if (err.code === '23505') {
      throw new ObservationAlreadyExists(`An observation with a resultTime of ${obsCore.resultTime.toISOString()} and location id '${obsCore.location}' already exists for the timeseries with id: '${timeseriesId}'.`);
    } else {
      throw new CreateObservationFail(undefined, err.message);
    }
  }

  return observationDbToCore(createdObservation);

}




export async function getObservationById(id: string): Promise<ObservationCore> {

  const {timeseriesId, resultTime} = deconstructObservationId(id);

  let foundObservation;
  try {
    const result = await knex('observations')
    .select()
    .where({
      timeseries: timeseriesId,
      result_time: resultTime
    });
    foundObservation = result[0];
  } catch (err) {
    throw new GetObservationByIdFail(undefined, err.message);
  }

  if (!foundObservation) {
    throw new ObservationNotFound(`Failed to find an observation with ID '${id}'`);
  }

  return observationDbToCore(foundObservation);

}



export async function getObservations(where: {timeseriesIds?: string[]; resultTime?: any; flags?: any}, options: {limit?: number, offset?: number}): Promise<ObservationCore[]> {

  // TODO: Need to make sure there's a max limit applied on the numbers of observations than can be retrieved at once.
  // TODO: Need to actually filter by timeseriesIds.
  // TODO: It is possible no timeseriesIds have been provided, in which case just get all the most recent observations, as many as the limit allows. E.g. a request could have come from another microservice that just wants the last n observations from wherever.
  // TODO: resultTime can be an object with lt, gt, gte, lte properties.
  // TODO: need to be able to specify that flags should not exist ($exists: false), as well as being able to filter by specific flags.


  let foundObservations;
  try {
    const result = await knex('observations')
    .select()
    .where((builder) => {
      if (where.timeseriesIds) {
        builder.whereIn('timeseries', where.timeseriesIds);
      }
    })
    .limit(options.limit || 100000)
    .offset(options.offset || 0);
    foundObservations = result;
  } catch (err) {
    throw new GetObservationsFail(undefined, err.message);
  }


  return foundObservations.map(observationDbToCore);

}



export function extractCoreFromObservation(observation: ObservationApp): ObservationCore {
  const obsCore: ObservationCore = {
    value: observation.hasResult.value,
    resultTime: new Date(observation.resultTime)
  };
  if (observation.hasResult.flags) {
    obsCore.flags = observation.hasResult.flags;
  }
  return obsCore;
}


export function extractTimeseriesPropsFromObservation(observation: ObservationApp): TimeseriesProps {

  const props = pick(observation, [
    'madeBySensor',
    'inDeployments',
    'hostedByPath',
    'observedProperty',
    'hasFeatureOfInterest',
    'usedProcedures'
  ]);

  return props;
}


export function buildObservation(obsCore: ObservationCore, timeseries: TimeseriesApp): ObservationApp {

  const observation: ObservationApp = pick(timeseries, [
    'madeBySensor',
    'inDeployments',
    'hostedByPath',
    'observedProperty',
    'hasFeatureOfInterest',
    'usedProcedures'
  ]);

  observation.id = generateObservationId(timeseries.id, obsCore.resultTime);
  observation.resultTime = obsCore.resultTime;
  observation.hasResult = {
    value: obsCore.value,
  };
  if (obsCore.flags) {
    observation.hasResult.flags = obsCore.flags;
  }

  return observation;
}


// TODO: Might be worth trying to optimise this at some point
export function buildObservations(obsCores: ObservationCore[], timeseries: TimeseriesApp[]): ObservationApp[] {

  // Makes sense to loop through the obs rather than the timeseries given that it may be worth maintaining the order of the observations
  const observations = obsCores.map((obsCore) => {
    const matchingTimeseries = timeseries.find((ts) => ts.id === obsCore.timeseries);
    if (!matchingTimeseries) {
      throw new Error('No matching timeseries found to build obs from');
    }
    const observation = buildObservation(obsCore, matchingTimeseries);
    return observation;
  });

  return observations;
}


// N.B: This approach won't work if there's ever a situation when you get more that one observation from a given timeseries at the same resultTime and location. The most likely reason you'd have two observations at the same time is if you apply a procedure that manipulated the data in some way, however this would change the userProcedures array, and therefore the timeseriesId, so this particular example doesn't pose any issues to using this approach.
// N.B. The location id is included, because perhaps you have a sensor that can make simulataneous observations as several locations, if you don't incorporate the location then it will only allow you to save a single observation as they'll all have the same resultTime.
// N.B. we default to the locationId to 0 if no locationId is given, e.g. the obs didn't have a specific location.
export function generateObservationId(timeseriesId: number, resultTime: string | Date, locationId = 0): string {
  return `${timeseriesId}-${locationId}-${new Date(resultTime).toISOString()}`;
}



export function deconstructObservationId(id: string): {timeseriesId: number; resultTime: Date; locationId?: number} {
  const firstSplit = nthIndex(id, '-', 1);
  const secondSplit = nthIndex(id, '-', 2);
  const components: any = {
    timeseriesId: Number(id.slice(0, firstSplit)),
    resultTime: new Date(id.slice(secondSplit + 1, id.length))
  }; 
  const locationId = Number(id.slice(firstSplit + 1, secondSplit));
  if (locationId !== 0) {
    components.locationId = locationId;
  }
  return components;
}


function nthIndex(str: string, subString: string, nthOccurrence: number): number {
  const L = str.length;
  let i = -1;
  let n = nthOccurrence;
  while (n-- && i++ < L) {
    i = str.indexOf(subString, i);
    if (i < 0) break;
  }
  return i;
}



export function observationDbToCore(observationDb: ObservationDb): ObservationCore {

  const obsCore: ObservationCore = {
    timeseries: Number(observationDb.timeseries),
    resultTime: new Date(observationDb.result_time)
  };
  if (observationDb.flags) {
    obsCore.flags = observationDb.flags;
  }

  // Find the column that's not null
  if (observationDb.value_number !== null) {
    // For some reason the values from the value_number column are coming back as strings, so lets convert them back to a number here.
    obsCore.value = Number(observationDb.value_number);
  } else if (observationDb.value_boolean !== null) {
    obsCore.value = observationDb.value_boolean;
  } else if (observationDb.value_text !== null) {
    obsCore.value = observationDb.value_text;
  } else if (observationDb.value_json !== null) {
    obsCore.value = observationDb.value_json;
  }

  return obsCore;

}


export function buildObservationDb(obsCore: ObservationCore, timeseriesId: number): ObservationDb {
  
  const observationDb: ObservationDb = {
    result_time: new Date(obsCore.resultTime).toISOString(),
    timeseries: timeseriesId
  };
  if (obsCore.flags && obsCore.flags.length > 0) {
    observationDb.flags = obsCore.flags;
  }
  if (obsCore.location) {
    observationDb.location = obsCore.location;
  }

  if (check.number(obsCore.value)) {
    observationDb.value_number = obsCore.value;

  } else if (check.boolean(obsCore.value)) {
    observationDb.value_boolean = obsCore.value;

  } else if (check.string(obsCore.value)) {
    observationDb.value_text = obsCore.value;

  } else if (check.object(obsCore.value) || check.array(obsCore.value)) {
    observationDb.value_json = obsCore.value;

  } else {
    throw new Error(`Unexpected observation value: ${obsCore.value}`);
  }

  return observationDb;

}


export function observationClientToApp(observationClient: ObservationClient): ObservationApp {
  const observationApp = cloneDeep(observationClient);
  observationApp.resultTime = new Date(observationApp.resultTime);
  return observationApp;
}


export function observationAppToClient(observationApp: ObservationApp): ObservationClient {
  const observationClient = cloneDeep(observationApp);
  return observationClient;
}