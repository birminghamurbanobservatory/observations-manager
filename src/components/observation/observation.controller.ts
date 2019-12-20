import {ObservationClient} from './observation-client.class';
import {extractTimeseriesPropsFromObservation, extractCoreFromObservation, buildObservation, observationClientToApp, observationAppToClient} from './observation.service';
import * as observationService from '../observation/observation.service';
import {upsertTimeseries, getTimeseries} from '../timeseries/timeseries.service';
import * as logger from 'node-logger';
import * as joi from '@hapi/joi';
import {InvalidObservation} from './errors/InvalidObservation';



const createObservationSchema = joi.object({
  madeBySensor: joi.string().required(),
  hasResult: joi.object({
    value: joi.any().required(),
    flags: joi.array().items(joi.string())
  }).required(),
  resultTime: joi.string().isoDate().required(),
  inDeployments: joi.array().items(joi.string()),
  hostedByPath: joi.array().items(joi.string()),
  hasFeatureOfInterest: joi.string(),
  observedProperty: joi.string(),
  usedProcedures: joi.array().items(joi.string())
}).required();

export async function createObservation(observation: ObservationClient): Promise<ObservationClient> {

  const {error: validationErr} = createObservationSchema.validate(observation);
  if (validationErr) {
    throw new InvalidObservation(`Observation is invalid. Reason: ${validationErr.message}`);
  }

  logger.debug('Creating observation');
  const obs = observationClientToApp(observation);

  const props = extractTimeseriesPropsFromObservation(obs);
  const obsCore = extractCoreFromObservation(obs);

  const timeseries = await upsertTimeseries(props, obsCore.resultTime);
  logger.debug(`Corresponding timeseries (id: ${timeseries.id}) upserted.`);

  await observationService.saveObservation(obsCore, timeseries.id);
  logger.debug('Observation has been added to the database');

  const createdObservation = buildObservation(obsCore, timeseries);
  logger.debug('Complete observation', createdObservation);

  return observationAppToClient(createdObservation);

}



export async function getObservation(id: string): Promise<ObservationClient> {

  logger.debug(`Getting observation with id '${id}'`);
  const {timeseriesId} = observationService.deconstructObservationId(id);
  const obsCore = await observationService.getObservationById(id);
  const timeseries = await getTimeseries(timeseriesId);
  const observation = buildObservation(obsCore, timeseries);
  logger.debug('Observation found', observation);
  return observationService.observationAppToClient(observation);

}




const getObservationsWhereSchema = joi.object({
  startDate: joi.string().isoDate(),
  endDate: joi.string().isoDate(),
  madeBySensor: joi.string(),
  inDeployment: joi.string(), // what if the obs can be in more than one, e.g. use inDeployment: {in: []}
  isHostedBy: joi.string(),
  observedProperty: joi.string(),
  hasFeatureOfInterest: joi.string(),
  usedProcedure: joi.string() // need to allow more than one to be specified?
})
.min(1)
.required();

const getObservationsOptionsSchema = joi.object({
  limit: joi.number().integer().positive().max(1000),
  offset: joi.number().integer().positive()
})
.required();


// TODO: We'll probably want a user to be able limit just how many many observations they get in one go (e.g. for use with pagination), therefore we should add an options argument where this limit can be set.
export async function getObservations(where, options): Promise<ObservationClient[]> {

  // First we need to see if there's any matching timeseries


  // Now to get all the observations for these timeseries

  return [];

}
