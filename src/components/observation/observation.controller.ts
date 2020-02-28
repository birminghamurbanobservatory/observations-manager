import {ObservationClient} from './observation-client.class';
import {extractTimeseriesPropsFromObservation, extractCoreFromObservation, observationClientToApp, observationAppToClient} from './observation.service';
import * as observationService from '../observation/observation.service';
import {getTimeseries, findTimeseries, findTimeseriesUsingIds, findSingleMatchingTimeseries, convertPropsToExactWhere, updateTimeseries, createTimeseries} from '../timeseries/timeseries.service';
import * as logger from 'node-logger';
import * as joi from '@hapi/joi';
import {InvalidObservation} from './errors/InvalidObservation';
import {BadRequest} from '../../errors/BadRequest';
import {config} from '../../config';
import {uniq, cloneDeep, isEqual} from 'lodash';
import {ObservationCore} from './observation-core.class';
import {TimeseriesApp} from '../timeseries/timeseries-app.class';
import {locationClientToApp, getLocationByClientId, createLocation, locationAppToClient} from '../location/location.service';
import {validateGeometry} from '../../utils/geojson-validator';
import {GeometryMismatch} from './errors/GeometryMismatch';
import * as check from 'check-types';

const maxObsPerRequest = config.obs.maxPerRequest;


//-------------------------------------------------
// Create Observation
//-------------------------------------------------
const createObservationSchema = joi.object({
  madeBySensor: joi.string().required(),
  hasResult: joi.object({
    value: joi.any().required(),
    flags: joi.array().min(1).items(joi.string())
  }).required(),
  resultTime: joi.string().isoDate().required(),
  phenomenonTime: joi.object({
    hasBeginning: joi.string().isoDate(),
    hasEnd: joi.string().isoDate()
  }),
  inDeployments: joi.array().min(1).items(joi.string()),
  hostedByPath: joi.array().min(1).items(joi.string()),
  hasFeatureOfInterest: joi.string(),
  observedProperty: joi.string(),
  discipline: joi.array().min(1).items(joi.string()),
  usedProcedure: joi.array().min(1).items(joi.string()),
  location: joi.object({
    id: joi.string().guid(), // this is the client_id, a uuid,
    validAt: joi.string().isoDate(),
    geometry: joi.object({
      type: joi.string().required(),
      coordinates: joi.array().required()
    })
    .custom((value) => {
      validateGeometry(value); // throws an error if invalid
      return value;
    })
    .required()
  })
}).required();

export async function createObservation(observation: ObservationClient): Promise<ObservationClient> {

  const {error: validationErr} = createObservationSchema.validate(observation);
  if (validationErr) {
    throw new InvalidObservation(`Observation is invalid. Reason: ${validationErr.message}`);
  }

  const obs = observationClientToApp(observation);

  // Handle the location first (if present)
  let matchingLocation;
  if (observation.location) {
    const locationFromObs = locationClientToApp(observation.location);
    // Does this location already exist in our database?
    if (locationFromObs.clientId) {
      try {
        matchingLocation = await getLocationByClientId(locationFromObs.clientId);
        logger.debug('Matching location found', matchingLocation);
      } catch (err) {
        if (err.name === 'LocationNotFound') {
          logger.debug(`A location with clientId '${locationFromObs.clientId}' does not already exist in the database.`);
        } else {
          throw err;
        }
      }
      if (matchingLocation) {
        // Although the IDs may match, we need to check that the geometry object is also the same
        if (!isEqual(matchingLocation.geometry, locationFromObs.geometry)) {
          throw new GeometryMismatch();
        }
      }
    }
    if (!matchingLocation) {
      logger.debug('Creating a new location');
      // Use the observation resultTime as the validAt time for the location if it hasn't been specifically provided.
      if (check.not.assigned(locationFromObs.validAt)) {
        locationFromObs.validAt = new Date(observation.resultTime);
      }
      matchingLocation = await createLocation(locationFromObs);
      logger.debug('New location created', matchingLocation);
    }
  }

  const props = extractTimeseriesPropsFromObservation(obs);
  const exactWhere = convertPropsToExactWhere(props);
  const obsCore = extractCoreFromObservation(obs);

  if (matchingLocation) {
    obsCore.location = matchingLocation.id;
  }

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

    upsertedTimeseries = await createTimeseries(timeseriesToCreate);

    // Now to create the observation
    createdObsCore = await observationService.saveObservation(obsCore, upsertedTimeseries.id);

  }

  logger.debug('Created Observation Core', createdObsCore);
  logger.debug(`Upserted timeseries.`, upsertedTimeseries);

  let newObservationClientId;
  if (matchingLocation) {
    newObservationClientId = observationService.generateObservationId(upsertedTimeseries.id, createdObsCore.resultTime, matchingLocation.id);
  } else {
    newObservationClientId = observationService.generateObservationId(upsertedTimeseries.id, createdObsCore.resultTime);
  }

  const createdObservation = await observationService.getObservationByClientId(newObservationClientId);
  logger.debug('Complete saved observation', createdObservation);

  const createdObservationForClient = observationAppToClient(createdObservation);
  return createdObservationForClient;

}


//-------------------------------------------------
// Get Observation
//-------------------------------------------------
export async function getObservation(id: string): Promise<ObservationClient> {

  logger.debug(`Getting observation with id '${id}'`);
  const observation = await observationService.getObservationByClientId(id);
  logger.debug('Observation found', observation);
  return observationService.observationAppToClient(observation);

}



//-------------------------------------------------
// Get Observations
//-------------------------------------------------
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
  discipline: joi.alternatives().try(
    joi.string(),
    joi.object({
      in: joi.array().items(joi.string()).min(1)
    }).min(1)
  ),
  disciplines: joi.alternatives().try( // for an exact match of the whole array
    joi.array().items(joi.string()).min(1),
    joi.object({
      exists: joi.boolean()
    }).min(1)
  ),
  usedProcedure: joi.alternatives().try(
    joi.string(),
    joi.object({
      in: joi.array().items(joi.string()).min(1)
    }).min(1)
  ),
  usedProcedures: joi.alternatives().try( // for an exact match of the whole array
    joi.array().items(joi.string()).min(1),
    joi.object({
      exists: joi.boolean()
    }).min(1)
  ),
  // Add in spatial queries
  // TODO: What about filtering by flags, or filtering out flagged observations, just watch properties like this aren't used to filter the timeseries documents, just the observation rows.
})
.required();
// Decided not to have a minimum number of keys here, i.e. so that superusers or other microservices can get observations without limitations. The limitation will come from a pagination approach, whereby only so many observations can be returned per request.

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
  // TODO: Provide the option to include/exclude the location objects.
})
.required();

export async function getObservations(where = {}, options = {}): Promise<ObservationClient[]> {

  const {error: whereErr, value: whereValidated} = getObservationsWhereSchema.validate(where);
  if (whereErr) throw new BadRequest(whereErr.message);
  const {error: optionsErr, value: optionsValidated} = getObservationsOptionsSchema.validate(options);
  if (whereErr) throw new BadRequest(optionsErr.message);

  const observations = await observationService.getObservations(whereValidated, {
    limit: optionsValidated.limit,
    offset: optionsValidated.offset
  });

  const observationsForClient = observations.map(observationService.observationAppToClient);
  logger.debug(`Got ${observationsForClient.length} observations.`, observationsForClient);
  return observationsForClient;

}
