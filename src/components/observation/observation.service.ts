import ObsBucket from './obs-bucket.model';
import {ObservationApp} from './observation-app.class';
import {pick, cloneDeep, concat} from 'lodash';
import {TimeseriesApp} from '../timeseries/timeseries-app.class';
import {TimeseriesProps} from '../timeseries/timeseries-props.class';
import {BucketResult} from './bucket-result.class';
import {AddResultFail} from './errors/AddResultFail';
import {ObservationClient} from './observation-client.class';
import {GetResultByIdFail} from './errors/GetResultByIdFail';
import {ObservationNotFound} from './errors/ObservationNotFound';
import {ObservationAlreadyExists} from './errors/ObservationAlreadyExists';
import {knex} from '../../db/knex';



export async function createObservationsTable(): Promise<void> {

  await knex.schema.createTable('observations', (table): void => {

    table.specificType('id', 'BIGSERIAL'); // Don't set this as primary or else create_hypertable won't work.
    table.string('timeseries', 24).notNullable(); // 24 is the length of a Mongo ObjectID string
    table.timestamp('result_time', {useTz: true}).notNullable();
    table.specificType('value_numeric', 'numeric');
    table.boolean('value_boolean');
    table.text('value_text');
    table.jsonb('value_json');
    table.text('flags'); // use comma delimited string for the flags?

  });

  // Create the hypertable
  await knex.raw(`SELECT create_hypertable('observations', 'result_time');`);
  // N.B. if you want a custom primary key, or a unique index, then you must include the result_time.
  await knex.raw('CREATE UNIQUE INDEX ON observations (timeseries, result_time DESC)');
  // docs: https://docs.timescale.com/latest/using-timescaledb/schema-management#indexing

  return;
}




// Guide: https://www.mongodb.com/blog/post/time-series-data-and-mongodb-part-2-schema-design-best-practices
export async function addResult(result: BucketResult, timeseriesId: string): Promise<void> {

  // Because the resultTime forms part of the observation ID we generate, we need to ensure a result with this resultTime doesn't already exist for this timeseries. This also prevents us accidently saving the same observation over and over, e.g. because an ingester keep pushing duplicate observations. 
  // Adding a _id to each result and try to create a unique index would end up in a massive index. 
  // So the simple solution is just to first see if we can find a result with this resultTime.
  let resultAlreadyExists;
  try {
    await getResultById(generateObservationId(timeseriesId, result.resultTime)); 
    resultAlreadyExists = true;
  } catch (err) {
    // In this case we actually want an error to be caught, spefically a ObservationNotFound error.
    if (err.name === 'ObservationNotFound') {
      resultAlreadyExists = false;
    } else {
      throw err;
    }
  }

  if (resultAlreadyExists) {
    throw new ObservationAlreadyExists(`An observation with a resultTime of ${result.resultTime.toISOString()} already exists for this timeseries.`);
  }

  const resultTime = new Date(result.resultTime);
  const day = getDateAsDay(resultTime);
  const maxResultsPerBucket = 200;

  try {
    await ObsBucket.updateOne({
      timeseries: timeseriesId,
      day,
      nResults: {$lt: maxResultsPerBucket},
    }, 
    {
      $push: {results: result},
      $min: {startDate: resultTime},
      $max: {endDate: resultTime},
      $inc: {nResults: 1}
    },
    {
      upsert: true
    })
    .exec();
  } catch (err) {
    throw new AddResultFail(undefined, err.message); 
  }

  return;
}


export async function getResultById(id: string): Promise<BucketResult> {

  const {timeseriesId, resultTime} = deconstructObservationId(id);
  const day = getDateAsDay(resultTime);

  let buckets;
  try {
    buckets = await ObsBucket.find({
      timeseries: timeseriesId,
      day,
      startDate: {$lte: resultTime}, 
      endDate: {$gte: resultTime}
    }).exec();
  } catch (err) {
    throw new GetResultByIdFail(undefined, err.message);
  }

  if (buckets.length === 0) {
    throw new ObservationNotFound(`Failed to find an observation with ID '${id}'`);
  }

  // There might be a few buckets returned (e.g. if data originally came in out of order and the sensor has a high sample rate), and thus we need to get all the results together before finding the one we want.
  const allResults = buckets.reduce((resultsSoFar, currentBucket) => {
    return concat(resultsSoFar, currentBucket.results);
  }, []);
  const result = allResults.find((result) => result.resultTime.toISOString() === resultTime.toISOString());

  if (!result) {
    throw new ObservationNotFound(`Failed to find an observation with ID '${id}'`);
  }

  return result;

}


export function getDateAsDay(date: Date): Date {
  return new Date(date.toISOString().slice(0, 10));
}


export function extractResultFromObservation(observation: ObservationApp): BucketResult {
  const result: BucketResult = {
    value: observation.hasResult.value,
    resultTime: new Date(observation.resultTime)
  };
  if (observation.hasResult.flags) {
    result.flags = observation.hasResult.flags;
  }
  return result;
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


export function buildObservation(result: BucketResult, timeseries: TimeseriesApp): ObservationApp {

  const observation: ObservationApp = pick(timeseries, [
    'madeBySensor',
    'inDeployments',
    'hostedByPath',
    'observedProperty',
    'hasFeatureOfInterest',
    'usedProcedures'
  ]);

  observation.id = generateObservationId(timeseries.id, result.resultTime);
  observation.resultTime = result.resultTime;
  observation.hasResult = {
    value: result.value,
  };
  if (result.flags) {
    observation.hasResult.flags = result.flags;
  }

  return observation;
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

export function obsBucketDbToApp(obsBucketDb: any): any {
  const obsBucketApp = obsBucketDb.toObject();
  obsBucketApp.id = obsBucketApp._id.toString();
  delete obsBucketApp._id;
  delete obsBucketApp.__v;
  return obsBucketApp;  
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