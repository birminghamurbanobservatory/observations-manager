import {ObservationClient} from './observation-client.class';
import {extractTimeseriesPropsFromObservation, extractCoreFromObservation, buildObservation, observationClientToApp, observationAppToClient} from './observation.service';
import * as observationService from '../observation/observation.service';
import {getTimeseries, findTimeseries, findTimeseriesUsingIds, findSingleMatchingTimeseries, convertPropsToExactWhere, updateTimeseries, createTimeseries} from '../timeseries/timeseries.service';
import * as logger from 'node-logger';
import * as joi from '@hapi/joi';
import {InvalidObservation} from './errors/InvalidObservation';
import {BadRequest} from '../../errors/BadRequest';
import {config} from '../../config';
import {uniq, cloneDeep} from 'lodash';
import {ObservationCore} from './observation-core.class';
import {TimeseriesApp} from '../timeseries/timeseries-app.class';

const maxObsPerRequest = config.obs.maxPerRequest;


const createObservationSchema = joi.object({
  madeBySensor: joi.string().required(),
  hasResult: joi.object({
    value: joi.any().required(),
    flags: joi.array().min(1).items(joi.string())
  }).required(),
  resultTime: joi.string().isoDate().required(),
  inDeployments: joi.array().min(1).items(joi.string()),
  hostedByPath: joi.array().min(1).items(joi.string()),
  hasFeatureOfInterest: joi.string(),
  observedProperty: joi.string(),
  usedProcedures: joi.array().min(1).items(joi.string())
  // TODO: Allow a location object
}).required();

export async function createObservation(observation: ObservationClient): Promise<ObservationClient> {

  const {error: validationErr} = createObservationSchema.validate(observation);
  if (validationErr) {
    throw new InvalidObservation(`Observation is invalid. Reason: ${validationErr.message}`);
  }

  logger.debug('Creating observation');
  const obs = observationClientToApp(observation);

  const props = extractTimeseriesPropsFromObservation(obs);
  const exactWhere = convertPropsToExactWhere(props);
  const obsCore = extractCoreFromObservation(obs);

  // N.B. we get the timeseries first before either inserting or updating it, as we need to check the observation is saved propertly first (e.g. no ObservationAlreadyExists errors occur) before updating the firstObs or lastObs of the timeseries.

  // Is there a matching timeseries?
  const matchingTimeseries = await findSingleMatchingTimeseries(exactWhere);

  let createdObsCore: ObservationCore;
  let upsertedTimeseries: TimeseriesApp;

  if (matchingTimeseries) {
    logger.debug('A corresponding timeseries was found for this observation', matchingTimeseries);

    // Save the obs
    createdObsCore = await observationService.saveObservation(obsCore, matchingTimeseries.id);
    logger.debug('Observation has been added to the observations table');

    // Update the timeseries
    const updates: any = {};
    if (matchingTimeseries.firstObs > obs.resultTime) {
      updates.first_obs = obs.resultTime;
    }
    if (matchingTimeseries.lastObs < obs.resultTime) {
      updates.last_obs = obs.resultTime;
    }
    logger.debug('Updates for timeseries', updates);

    if (Object.keys(updates).length > 0) {
      logger.debug('Updating existing timeseries');
      upsertedTimeseries = await updateTimeseries(matchingTimeseries.id, updates);
    } else {
      logger.debug('Existing timeseries does not need updating');
      upsertedTimeseries = matchingTimeseries;
    }

  } else {
    logger.debug('A corresponding timeseries does not yet exist for this observation. Creating now.');

    // Need to create the timeseries first
    const timeseriesToCreate: any = cloneDeep(props);
    timeseriesToCreate.firstObs = obs.resultTime;
    timeseriesToCreate.lastObs = obs.resultTime;

    const upsertedTimeseries = await createTimeseries(timeseriesToCreate);

    // Now to create the observation
    createdObsCore = await observationService.saveObservation(obsCore, upsertedTimeseries.id);

  }

  logger.debug('Created Observation Core', createdObsCore);
  logger.debug(`Upserted timeseries.`, upsertedTimeseries);

  const createdObservation = buildObservation(createdObsCore, upsertedTimeseries);
  logger.debug('Complete saved observation', createdObservation);

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
  resultTime: joi.object({
    lt: joi.string().isoDate(),
    lte: joi.string().isoDate(),
    gt: joi.string().isoDate(),
    gte: joi.string().isoDate()
  })
  .without('lt', 'lte')
  .without('gt', 'gte'),
  madeBySensor: joi.alternatives().try(
    joi.string(),
    joi.object({
      in: joi.array().items(joi.string()).min(1).required()
    })
  ),
  inDeployment: joi.alternatives().try(
    joi.string(), // find obs that belong to this deployment (may belong to more)
    joi.object({
      in: joi.array().items(joi.string()).min(1), // obs that belong to any of these deployments
      exists: joi.boolean()
    }).min(1)
  ),
  inDeployments: joi.alternatives().try(
    joi.array().items(joi.string()).min(1),
    joi.object({
      // don't yet support 'in' here.
      exists: joi.boolean()
    }).min(1)
  ),
  // For exact matches
  hostedByPath: joi.alternatives().try(
    joi.array().items(joi.string()).min(1),
    joi.object({
      in: joi.array().min(1).items(
        joi.array().items(joi.string()).min(1)
      ),
      exists: joi.boolean()
    }).min(1)
  ),
  // For when the platformId can occur anywhere in the path
  isHostedBy: joi.alternatives().try(
    joi.string(),
    joi.object({
      in: joi.array().items(joi.string()).min(1).required()
    })
  ),
  // For lquery strings, e.g. 'building-1.room-1.*' or {in: ['building-1.*', 'building-2.*']}
  hostedByPathSpecial: joi.alternatives().try(
    joi.string(),
    joi.object({
      in: joi.array().items(joi.string()).min(1).required()
    })
  ),  
  observedProperty: joi.alternatives().try(
    joi.string(),
    joi.object({
      in: joi.array().items(joi.string()).min(1),
      exists: joi.boolean()
    }).min(1)
  ),
  hasFeatureOfInterest: joi.alternatives().try(
    joi.string(),
    joi.object({
      in: joi.array().items(joi.string()).min(1),
      exists: joi.boolean()
    }).min(1)
  ),
  usedProcedure: joi.alternatives().try(
    joi.string(),
    joi.object({
      in: joi.array().items(joi.string()).min(1)
    }).min(1)
  ),
  usedProcedures: joi.alternatives().try(
    joi.array().items(joi.string()).min(1),
    joi.object({
      // don't yet support 'in' here.
      exists: joi.boolean()
    }).min(1)
  ),
  // TODO: What about filtering by flags, or filtering out flagged observations, just watch properties like this aren't used to filter the timeseries documents, just the observation rows.
})
.required();
// Decided not to have a minimum number of keys here, i.e. so that superusers or other microservices can get observations with limitations. The limitation will come from a pagination approach, whereby only so many observations can be returned per request.

const getObservationsOptionsSchema = joi.object({
  limit: joi.number()
    .integer()
    .positive()
    .max(maxObsPerRequest)
    .default(maxObsPerRequest),
  offset: joi.number()
    .integer()
    .positive()
    .default(0)
})
.required();

export async function getObservations(where, options): Promise<ObservationClient[]> {

  const {error: whereErr, value: whereValidated} = getObservationsWhereSchema.validate(where);
  if (whereErr) throw new BadRequest(whereErr.message);
  const {error: optionsErr, value: optionsValidated} = getObservationsOptionsSchema.validate(options);
  if (whereErr) throw new BadRequest(optionsErr.message);

  // If no "where" parameters have been provided that allow us to filter down the timeseriesId we need to search by, then best to get observations first, and then get their timeseries after so we can give the observations some extra metadata.
  const whereKeys = Object.keys(whereValidated);
  const getObsBeforeTimeseries = whereKeys.length === 0 || (whereKeys.length === 1 && whereKeys[0] === 'resultTime');

  let timeseries;
  let timeseriesIds;

  if (!getObsBeforeTimeseries) {
    // Find any matching timeseries
    timeseries = await findTimeseries(whereValidated);
    timeseriesIds = timeseries.map((ts): string => ts.id);
  }

  // Now to get all the observations for these timeseries
  // TODO: do I need this function to also tell us whether if hit the maximum obs limit?
  const obsCores = await observationService.getObservations({
    timeseriesIds
    // TODO: add resultTime and flags params.
  }, {
    limit: optionsValidated.limit,
    offset: optionsValidated.offset
  });

  // If we need to get timeseries info AFTER having got the obs
  if (obsCores.length > 0 && !timeseries) {
    // Get the timeseriesIds from the obs
    const timeseriesIdsFromObs = uniq(obsCores.map((obsCore) => obsCore.timeseries));
    timeseries = await findTimeseriesUsingIds(timeseriesIdsFromObs);
  }

  const observations = observationService.buildObservations(obsCores, timeseries);

  const observationsForClient = observations.map(observationService.observationAppToClient);

  return observationsForClient;

}
