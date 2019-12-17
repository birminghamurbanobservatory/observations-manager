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
// export async function getObservations(where): Promise<ObservationClient[]> {

//   // First we need to see if there's any matching timeseries

//   // Now to get all the observations for these timeseries



// }
