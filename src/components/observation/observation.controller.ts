import {ObservationClient} from './observation-client.class';
import {extractTimeseriesPropsFromObservation, extractCoreFromObservation, observationClientToApp, observationAppToClient} from './observation.service';
import * as observationService from '../observation/observation.service';
import {findSingleMatchingTimeseries, convertPropsToExactWhere, updateTimeseries, createTimeseries, decodeTimeseriesId, generateHashFromTimeseriesProps, findSingleTimeseriesUsingHash} from '../timeseries/timeseries.service';
import * as logger from 'node-logger';
import * as joi from '@hapi/joi';
import {BadRequest} from '../../errors/BadRequest';
import {config} from '../../config';
import {cloneDeep, isEqual, sortBy} from 'lodash';
import {ObservationCore} from './observation-core.class';
import {TimeseriesApp} from '../timeseries/timeseries-app.class';
import {locationClientToApp, getLocationByClientId, createLocation, locationAppToClient} from '../location/location.service';
import {GeometryMismatch} from './errors/GeometryMismatch';
import * as check from 'check-types';
import {validateObservation} from './observation-validator';


const maxObsPerRequest = config.obs.maxPerRequest;


//-------------------------------------------------
// Create Observation
//-------------------------------------------------
export async function createObservation(observation: ObservationClient): Promise<ObservationClient> {

  const validObservation = validateObservation(observation);

  const obs = observationClientToApp(validObservation);

  // Handle the location first (if present)
  let matchingLocation;
  if (validObservation.location) {
    const locationFromObs = locationClientToApp(validObservation.location);
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
          logger.warn('Geometry mismatch', {matchingLocation: matchingLocation.geometry, locationFromObs: locationFromObs.geometry});
          throw new GeometryMismatch();
        }
      }
    }
    if (!matchingLocation) {
      logger.debug('Creating a new location');
      // Use the observation resultTime as the validAt time for the location if it hasn't been specifically provided.
      if (check.not.assigned(locationFromObs.validAt)) {
        locationFromObs.validAt = obs.resultTime;
      }
      // I have seen some race condition errors, e.g. when new Netatmo stations come online, because observations arrive at the same time, all with the same client id and it ends up trying to create a location just after it's already been created by the observation that arrived a split second earlier. The catch here should account for this.
      try {
        matchingLocation = await createLocation(locationFromObs);
        logger.debug('New location created', matchingLocation);
      } catch (err) {
        if (err.name === 'LocationAlreadyExists' && locationFromObs.clientId) {
          logger.debug(`A LocationAlreadyExists error was thrown whilst creating a location with client id '${locationFromObs.clientId}', probably because of a race condition. The location should therefore now be saved, so we'll try to get it instead.`);
          matchingLocation = await getLocationByClientId(locationFromObs.clientId);
          logger.debug(`Managed to get the location with client id ${matchingLocation.clientId} (following race condition catch)`);
        } else {
          throw err;
        }
      }
    }
  }

  const props = extractTimeseriesPropsFromObservation(obs);
  const hash = generateHashFromTimeseriesProps(props);
  const obsCore = extractCoreFromObservation(obs);

  if (matchingLocation) {
    obsCore.location = matchingLocation.id;
  }

  // N.B. we get the timeseries first before either inserting or updating it, as we need to check the observation is saved propertly first (e.g. no ObservationAlreadyExists errors occur) before updating the firstObs or lastObs of the timeseries.

  // Is there a matching timeseries?
  let matchingTimeseries;
  try {
    matchingTimeseries = await findSingleTimeseriesUsingHash(hash);
  } catch (err) {
    if (err.name === 'TimeseriesNotFound') {
      // Do nothing, this is to be expected sometimes.
    } else {
      throw err;
    }
  }

  let createdObsCore: ObservationCore;
  let upsertedTimeseries: TimeseriesApp;

  if (matchingTimeseries) {
    logger.debug('A corresponding timeseries was found for this observation', matchingTimeseries);

    // Save the obs
    try {
      createdObsCore = await observationService.saveObservation(obsCore, matchingTimeseries.id);
    } catch (err) {
      // Added this extra logging here so I can see which observation it was that failed to save due to a bad value
      if (err.name === 'UnexpectedObservationValue') {
        logger.error(`Failed to save observation. Reason: ${err.message}`, {observation});
      }
      throw err;
    }
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
    timeseriesToCreate.hash = hash;

    let raceConditionOccurred;

    try {
      upsertedTimeseries = await createTimeseries(timeseriesToCreate);
    } catch (err) {
      if (err.name === 'TimeseriesAlreadyExists') {
        // If we get here it means there's been a race condition. For example multiple observations may arrive from the same timeseries at virtually the same time, if this is the first time we've seen observations from this timeseries then the first few observations will all trigger the creation of a timeseries. The first observation will be able to create the timeseries, but the rest will fail because of the unique index on the hash and will thus reach this point. We now need to get the newly created timeseries so we can give this observation the correct timeseries id.
        logger.debug(`Race condition occured when creating observation due to timeseries already existing (hash: ${hash})`);
        upsertedTimeseries = await findSingleTimeseriesUsingHash(hash);
        raceConditionOccurred = true;

      } else {
        throw err;
      }
    }

    // Now to create the observation
    createdObsCore = await observationService.saveObservation(obsCore, upsertedTimeseries.id);

    if (raceConditionOccurred) {
      // Now we know that the observation was created ok, let's see if we need to update the timeseries
      const updates: any = {};
      if (upsertedTimeseries.firstObs > obs.resultTime) {
        updates.first_obs = obs.resultTime;
      }
      if (upsertedTimeseries.lastObs < obs.resultTime) {
        updates.last_obs = obs.resultTime;
      }
      logger.debug('Updates for timeseries', updates);

      if (Object.keys(updates).length > 0) {
        logger.debug('Updating existing timeseries');
        upsertedTimeseries = await updateTimeseries(upsertedTimeseries.id, updates);
      }
    }

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
  timeseriesId: joi.alternatives().try(
    joi.string().alphanum(), // catches any accidental commas that might be present
    joi.object({
      in: joi.array().items(joi.string()).min(1),
      not: joi.object({
        in: joi.array().items(joi.string()).min(1).required(),
      })
    })
  ),
  resultTime: joi.object({
    lt: joi.string().isoDate(),
    lte: joi.string().isoDate(),
    gt: joi.string().isoDate(),
    gte: joi.string().isoDate()
  })
  .without('lt', 'lte')
  .without('gt', 'gte'),
  duration: joi.alternatives().try(
    joi.number().min(0),
    joi.object({
      lt: joi.number().min(0),
      lte: joi.number().min(0),
      gt: joi.number().min(0),
      gte: joi.number().min(0)
    })
    .without('lt', 'lte')
    .without('gt', 'gte'),
  ),
  valueType: joi.alternatives().try(
    joi.string().valid('number', 'text', 'boolean', 'json'),
    joi.object({
      in: joi.array().items(joi.string().valid('number', 'text', 'boolean', 'json')).min(1).required()
    }).min(1)
  ),
  madeBySensor: joi.alternatives().try(
    joi.string(),
    joi.object({
      in: joi.array().items(joi.string()).min(1).required()
    })
  ),
  hasDeployment: joi.alternatives().try(
    joi.string(), // find obs that belong to this deployment
    joi.object({
      in: joi.array().items(joi.string()).min(1), // obs that belong to any of these deployments
      exists: joi.boolean(),
      not: joi.alternatives().try(
        joi.string(),
        joi.object({
          in: joi.array().items(joi.string()).min(1).required()
        })
      )
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
  aggregation: joi.alternatives().try(
    joi.string(),
    joi.object({
      in: joi.array().items(joi.string()).min(1)
    }).min(1)
  ),
  unit: joi.alternatives().try(
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
    joi.array().items(joi.string()).min(1).custom((arr) => sortBy(arr)), // sort alphabetically to match database.
    joi.object({
      exists: joi.boolean(),
      not: joi.array().items(joi.string()).min(1).custom((arr) => sortBy(arr)), // sort alphabetically to match database.
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
  // TODO: Add the ability to find observations with a specific flag
  flags: joi.object({
    exists: joi.boolean()
  }),
  // Does the observation even have a location
  location: joi.object({
    exists: joi.boolean()
  }),
  // Spatial queries
  latitude: joi.object({
    lt: joi.number().min(-90).max(90),
    lte: joi.number().min(-90).max(90),
    gt: joi.number().min(-90).max(90),
    gte: joi.number().min(-90).max(90)
  }),
  longitude: joi.object({
    lt: joi.number().min(-180).max(180),
    lte: joi.number().min(-180).max(180),
    gt: joi.number().min(-180).max(180),
    gte: joi.number().min(-180).max(180)
  }),
  height: joi.object({
    lt: joi.number(),
    lte: joi.number(),
    gt: joi.number(),
    gte: joi.number()
  }),
  proximity: joi.object({
    centre: joi.object({
      latitude: joi.number().min(-90).max(90).required(),
      longitude: joi.number().min(-180).max(180).required()
    }).required(),
    radius: joi.number().min(0).required() // in metres
  })
})
.required();
// Decided not to have a minimum number of keys here, i.e. so that superusers or other microservices can get observations without limitations. The limitation will come from a pagination approach, whereby only so many observations can be returned per request.

const getObservationsOptionsSchema = joi.object({
  condense: joi.boolean(), // when true far less properties are included, e.g. all the timeseries properties are left out.
  limit: joi.number()
    .integer()
    .positive()
    .max(maxObsPerRequest)
    .default(maxObsPerRequest),
  offset: joi.number()
    .integer()
    .positive()
    .default(0),
  onePer: joi.string()
    .valid('sensor', 'timeseries'), // TODO: add hosted_by_path at some point? Although this is tricker given the nested structure of platforms and that a sensor may not be on any platform.
  sortBy: joi.string()
    .valid('timeseries', 'resultTime'),
  sortOrder: joi.string()
    .valid('asc', 'desc')
  // TODO: Provide the option to include/exclude the location objects.
})
.required();

export async function getObservations(where = {}, options = {}): Promise<{data: ObservationClient[]; meta: any}> {

  const {error: whereErr, value: whereValidated} = getObservationsWhereSchema.validate(where);
  if (whereErr) throw new BadRequest(whereErr.message);
  const {error: optionsErr, value: optionsValidated} = getObservationsOptionsSchema.validate(options);
  if (whereErr) throw new BadRequest(optionsErr.message);

  // Decode the hashed timeseries ids.
  if (check.assigned(whereValidated.timeseriesId)) {
    if (check.string(whereValidated.timeseriesId)) {
      whereValidated.timeseriesId = decodeTimeseriesId(whereValidated.timeseriesId);
    }
    if (check.array(whereValidated.timeseriesId.in)) {
      whereValidated.timeseriesId.in = whereValidated.timeseriesId.in.map(decodeTimeseriesId);
    }
    if (check.object(whereValidated.timeseriesId.not)) {
      if (check.array(whereValidated.timeseriesId.not.in)) {
        whereValidated.timeseriesId.not.in = whereValidated.timeseriesId.not.in.map(decodeTimeseriesId);
      }
    }
  }

  const observations = await observationService.getObservations(whereValidated, optionsValidated);

  const observationsForClient = observations.map(observationService.observationAppToClient);
  logger.debug(`Got ${observationsForClient.length} observations.`);
  return {
    data: observationsForClient,
    // TODO: At some point you may want to calculate and return the total number of observations available, e.g. for pagination, this information will go in this meta object. I just need to make sure I can calculate this efficiently.
    meta: {
      count: observationsForClient.length
    } 
  };

}


//-------------------------------------------------
// Update Observation
//-------------------------------------------------
const observationUpdatesSchema = joi.object({
  // There's not many properties that we want a client being able to update. For example we don't really want the client changing timeseries properties, e.g. madeBySensor, because this would completely change the timeseries it belongs to.
  hasResult: joi.object({
    // Provide a value of null when you want all flags removing.
    flags: joi.array().allow(null).min(1).items(joi.string())
  })
  // TODO: other properties you might want to consider supporting are the value, the resultTime, the location and the phenomenonTime properties.
})
.min(1)
.required();

export async function updateObservation(id: string, updates: any): Promise<ObservationClient> {

  logger.debug(`Updating observation '${id}'`, {updates});

  const {error: validationErr, value: validUpdates} = observationUpdatesSchema.validate(updates);
  if (validationErr) throw new BadRequest(validationErr.message);

  const flatUpdates = Object.assign({}, validUpdates, validUpdates.hasResult);
  delete flatUpdates.hasResult;

  const updatedObservation = await observationService.updateObservationByClientId(id, flatUpdates);
  logger.debug(`Observation '${id}' updated.`, updatedObservation);

  return observationService.observationAppToClient(updatedObservation);

}