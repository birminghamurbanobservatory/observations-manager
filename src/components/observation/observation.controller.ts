import {ObservationClient} from './observation-client.class';
import {addResult, extractTimeseriesPropsFromObservation, extractResultFromObservation, buildObservation, observationClientToApp, observationAppToClient} from './observation.service';
import * as observationService from '../observation/observation.service';
import {upsertTimeseries, getTimeseries} from '../timeseries/timeseries.service';
import * as logger from 'node-logger';
import * as joi from '@hapi/joi';


export async function createObservation(observation: ObservationClient): Promise<ObservationClient> {

  logger.debug('Creating observation');
  const obs = observationClientToApp(observation);

  const props = extractTimeseriesPropsFromObservation(obs);
  const result = extractResultFromObservation(obs);

  const timeseries = await upsertTimeseries(props, result.resultTime);
  logger.debug(`Corresponding timeseries (id: ${timeseries.id}) upserted.`);

  await addResult(result, timeseries.id);
  logger.debug('Observation has been added to database');

  const createdObservation = buildObservation(result, timeseries);
  logger.debug('Complete observation', createdObservation);

  return observationAppToClient(createdObservation);

}



export async function getObservation(id: string): Promise<ObservationClient> {

  logger.debug(`Getting observation with id '${id}'`);
  const {timeseriesId} = observationService.deconstructObservationId(id);
  const result = await observationService.getResultById(id);
  const timeseries = await getTimeseries(timeseriesId);
  const observation = buildObservation(result, timeseries);
  logger.debug('Observation found', observation);
  return observationService.observationAppToClient(observation);

}




const getObservationsWhereSchema = joi.object({
  startDate: joi.string().isoDate(),
  endDate: joi.string().isoDate(),
  madeBySensor: joi.string(),
  inDeployment: joi.string(), 
  isHostedBy: joi.string(),
  observedProperty: joi.string(),
  hasFeatureOfInterest: joi.string(),
  usedProcedure: joi.string() // need to allow more than one to be specified?
})
.min(1)
.required();

// TODO: We'll probably want a user to be able limit just how many many observations they get in one go (e.g. for use with pagination), therefore we should add an options argument where this limit can be set.
export async function getObservations(where): Promise<ObservationClient[]> {

  // First we need to see if there's any matching timeseries

  // Now to get all the observations for these timeseries

  // TODO: How do we prevent a query from returning an obscene amount of results? You could run an initial query just to get the bucket IDs and nResults to check it won't be too many. Then use {$in: bucketIds} to get the full buckets. However if there is too many results, you don't just want to throw an error, you should instead, for example, return the first 1000 observations. What makes this tricky is because observations can arrive out of order and you might have buckets for 100's of different sensors, you'll still need to get most of these buckets in order to merge them together, sort them, and select the first 1000. If you also get the startDate and endDate on the first query you may be able to exclude a few buckets that you know you won't need, but you could still get a load of data that you'll just filter out.
  // You could just choose to order by timeseries instead, and thus the user could just get results from the first few buckets from a given timeseries, and would need to make further requests to get any other timeseries that may be present.
  // This is possibly a situation where TimescaleDB would work better. However, unless you store the data as JSON you'll end up with a load of data type issues again.

}