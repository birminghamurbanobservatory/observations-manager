import ObsBucket from './obs-bucket.model';
import {ObservationApp} from './observation-app.class';
import {pick, cloneDeep} from 'lodash';
import {TimeseriesApp} from '../timeseries/timeseries-app.class';
import {TimeseriesProps} from '../timeseries/timeseries-props.class';
import {BucketResult} from './bucket-result.class';
import {AddResultFail} from './errors/AddResultFail';
import {ObservationClient} from './observation-client.class';


// Guide: https://www.mongodb.com/blog/post/time-series-data-and-mongodb-part-2-schema-design-best-practices
export async function addResult(result: BucketResult, timeseriesId: string): Promise<void> {

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