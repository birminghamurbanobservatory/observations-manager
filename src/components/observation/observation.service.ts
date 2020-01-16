import {ObservationCore} from './observation-core.class';
import {ObservationApp} from './observation-app.class';
import {pick, cloneDeep, concat} from 'lodash';
import {TimeseriesApp} from '../timeseries/timeseries-app.class';
import {TimeseriesProps} from '../timeseries/timeseries-props.class';
import {ObservationClient} from './observation-client.class';
import {ObservationRow} from './observation-row.class';
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
    table.specificType('value_number', 'numeric');
    table.boolean('value_boolean');
    table.text('value_text');
    table.jsonb('value_json');
    table.specificType('flags', 'text ARRAY'); // https://www.postgresql.org/docs/9.1/arrays.html
    // TODO: Add a location column that points to the locations table

  });

  // Create the hypertable
  await knex.raw(`SELECT create_hypertable('observations', 'result_time');`);
  // TODO: By default this will create an index called observations_result_time_idx, should I delete this given that I'll create one below that also uses the timeseries and will be used instead anyway? 
  // N.B. if you want a custom primary key, or a unique index, then you must include the result_time.
  await knex.raw('CREATE UNIQUE INDEX timeseries_result_time_uniq_idx ON observations (timeseries, result_time DESC)');
  // docs: https://docs.timescale.com/latest/using-timescaledb/schema-management#indexing

  return;
}



export async function saveObservation(obsCore: ObservationCore, timeseriesId: string): Promise<any> {

  const observationRow = buildObservationRow(obsCore, timeseriesId);

  let createdObservation: ObservationRow;
  try {
    const result = await knex('observations')
    .insert(observationRow)
    .returning('*');
    createdObservation = result[0];
  } catch (err) {
    if (err.code === '23505') {
      throw new ObservationAlreadyExists(`An observation with a resultTime of ${obsCore.resultTime.toISOString()} already exists for the timeseries with id: '${timeseriesId}'.`);
    } else {
      throw new CreateObservationFail(undefined, err.message);
    }
  }

  return observationRowToCore(createdObservation);

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

  return observationRowToCore(foundObservation);

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


  return foundObservations.map(observationRowToCore);

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


// TODO: This approach won't work if there's ever a situation when you get more that one observation from a given timeseries at the same resultTime. The most likely reason you'd have two observations at the same time is if you apply a procedure that manipulated the data in some way, however this would change the userProcedures array, and therefore the timeseriesId, so this particular example doesn't pose any issues to using this approach.
export function generateObservationId(timeseriesId: string, resultTime: string | Date): string {
  return `${timeseriesId}-${new Date(resultTime).toISOString()}`;
}

export function deconstructObservationId(id: string): {timeseriesId: string; resultTime: Date} {
  const splitAt = id.indexOf('-');
  return {
    timeseriesId: id.slice(0, splitAt),
    resultTime: new Date(id.slice(splitAt + 1, id.length))
  }; 
}



export function observationRowToCore(observationRow: ObservationRow): ObservationCore {

  const obsCore: ObservationCore = {
    timeseries: observationRow.timeseries,
    resultTime: new Date(observationRow.result_time)
  };
  if (observationRow.flags) {
    obsCore.flags = observationRow.flags;
  }

  // Find the column that's not null
  if (observationRow.value_number !== null) {
    // For some reason the values from the value_number column are coming back as strings, so lets convert them back to a number here.
    obsCore.value = Number(observationRow.value_number);
  } else if (observationRow.value_boolean !== null) {
    obsCore.value = observationRow.value_boolean;
  } else if (observationRow.value_text !== null) {
    obsCore.value = observationRow.value_text;
  } else if (observationRow.value_json !== null) {
    obsCore.value = observationRow.value_json;
  }

  return obsCore;

}


export function buildObservationRow(obsCore: ObservationCore, timeseriesId: string): ObservationRow {
  
  const observationRow: ObservationRow = {
    result_time: new Date(obsCore.resultTime).toISOString(),
    timeseries: timeseriesId
  };
  if (obsCore.flags && obsCore.flags.length > 0) {
    observationRow.flags = obsCore.flags;
  }

  if (check.number(obsCore.value)) {
    observationRow.value_number = obsCore.value;

  } else if (check.boolean(obsCore.value)) {
    observationRow.value_boolean = obsCore.value;

  } else if (check.string(obsCore.value)) {
    observationRow.value_text = obsCore.value;

  } else if (check.object(obsCore.value) || check.array(obsCore.value)) {
    observationRow.value_json = obsCore.value;

  } else {
    throw new Error(`Unexpected observation value: ${obsCore.value}`);
  }

  return observationRow;

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