import * as timeseriesService from './timeseries.service';
import hasher from '../../utils/hasher';
import * as logger from 'node-logger';
import * as joi from '@hapi/joi';
import {TimeseriesClient} from './timeseries-client.class';
import {BadRequest} from '../../errors/BadRequest';
import {deleteObservations, updateObservations} from '../observation/observation.service';
import * as Promise from 'bluebird';
import {minBy, maxBy} from 'lodash';

//-------------------------------------------------
// Get Single Timeseries
//-------------------------------------------------
export async function getSingleTimeseries(clientId: string): Promise<TimeseriesClient> {

  // Decode the hashed id.
  const databaseId = timeseriesService.decodeTimeseriesId(clientId);
  logger.debug(`Getting timeseries with database id '${databaseId}' (clientId: ${clientId})`);

  const timeseries = await timeseriesService.getTimeseries(databaseId);
  logger.debug('timeseries found', timeseries);
  return timeseriesService.timeseriesAppToClient(timeseries);

}



//-------------------------------------------------
// Get Multiple Timeseries
//-------------------------------------------------
const getMultipleTimeseriesWhereSchema = joi.object({
  id: joi.object({
    in: joi.array().items(joi.string()).min(1).required()
  }),
  firstObs: joi.object({
    lt: joi.string().isoDate(),
    lte: joi.string().isoDate(),
    gt: joi.string().isoDate(),
    gte: joi.string().isoDate()
  })
  .without('lt', 'lte')
  .without('gt', 'gte'),
  lastObs: joi.object({
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
  hasDeployment: joi.alternatives().try(
    joi.string(), // find obs that belong to this deployment (may belong to more)
    joi.object({
      in: joi.array().items(joi.string()).min(1), // obs that belong to any of these deployments
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
  aggregation: joi.alternatives().try(
    joi.string(),
    joi.object({
      in: joi.array().items(joi.string()).min(1),
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
  )
})
.required();
// Decided not to have a minimum number of keys here, i.e. so that superusers or other microservices can get timeseries without limitations. The limitation will come from a pagination approach, whereby only so many timeseries can be returned per request.

const getMultipleTimeseriesOptionsSchema = joi.object({
  limit: joi.number()
    .integer()
    .positive()
    .max(500)
    .default(100),
  offset: joi.number()
    .integer()
    .positive()
    .default(0),
  sortBy: joi.string()
    .valid('id') // TODO: Given that the id is hashed, this is a little pointless. Improve.
    .default('id'),
  sortOrder: joi.string()
    .valid('asc', 'desc')
    .default('asc')
})
.required();

export async function getMultipleTimeseries(where = {}, options = {}): Promise<{data: TimeseriesClient[]; meta: any}> {

  const {error: whereErr, value: whereValidated} = getMultipleTimeseriesWhereSchema.validate(where);
  if (whereErr) throw new BadRequest(whereErr.message);
  const {error: optionsErr, value: optionsValidated} = getMultipleTimeseriesOptionsSchema.validate(options);
  if (whereErr) throw new BadRequest(optionsErr.message);

  if (whereValidated.id && whereValidated.id.in) {
    whereValidated.id.in = whereValidated.id.in.map(timeseriesService.decodeTimeseriesId);
  }

  const timeseries = await timeseriesService.findTimeseries(whereValidated, optionsValidated);

  const timeseriesForClient = timeseries.map(timeseriesService.timeseriesAppToClient);
  logger.debug(`Got ${timeseriesForClient.length} timeseries.`);
  return {
    data: timeseriesForClient,
    // TODO: At some point you may want to calculate and return the total number of timeseries available, e.g. for pagination, this information will go in this meta object. I just need to make sure I can calculate this efficiently.
    meta: {
      count: timeseriesForClient.length
    } 
  };

}




//-------------------------------------------------
// Merge Timeseries
//-------------------------------------------------
export async function mergeTimeseries(goodIdToKeep: string, idsToMerge: string[]): Promise<any> {

  logger.debug(`Merging ${idsToMerge.length} timeseries into timeseries ${goodIdToKeep}`);

  if (idsToMerge.includes(goodIdToKeep)) {
    throw new BadRequest(`The id '${goodIdToKeep}' is provided as both a merger and mergee timeseries.`);
  }

  // Decode the hashed ids.
  const goodDbIdToKeep = timeseriesService.decodeTimeseriesId(goodIdToKeep);
  const dbIdsToMerge = idsToMerge.map(timeseriesService.decodeTimeseriesId);

  // First check that the goodId exists (throws an error if it doesn't)
  let goodTimeseries;
  try {
    goodTimeseries = await timeseriesService.getTimeseries(goodDbIdToKeep);
  } catch (error) {
    if (error.name === 'TimeseriesNotFound') {
      // If we don't do this the error message with have numerical database ID, not the nice client id.
      error.message = `A timeseries with id ${goodIdToKeep} could not be found`; 
      throw error;
    } else {
      throw error;
    }
  }

  // Get the old timeseries. I can't imagine there will ever be many timeseries to merge, so let's get them one by one, so that a meaningful error is generated if the timeseries doesn't exist.
  const timeseriesToMerge = await Promise.map(dbIdsToMerge, async (dbIdToMerge, idx) => {
    let timeseries;
    try {
      timeseries = await timeseriesService.getTimeseries(dbIdToMerge);
    } catch (error) {
      if (error.name === 'TimeseriesNotFound') {
        // If we don't do this the error message with have numerical database ID, not the nice client id.
        error.message = `A timeseries with id ${idsToMerge[idx]} could not be found`; 
        throw error;
      } else {
        throw error;
      }
    }
    return timeseries;
  });

  // We'll want to update the time of the first and last obs
  // @ts-ignore
  const {firstObs: minFirstObs} = minBy(timeseriesToMerge, 'firstObs');
  // @ts-ignore
  const {lastObs: maxLastObs} = maxBy(timeseriesToMerge, 'lastObs');

  const updates: any = {};
  if (minFirstObs < goodTimeseries.firstObs) {
    updates.firstObs = minFirstObs;
  }
  if (maxLastObs > goodTimeseries.lastObs) {
    updates.lastObs = maxLastObs;
  }

  if (Object.keys(updates).length > 0) {
    logger.debug('Updating the good timeseries', updates);
    await timeseriesService.updateTimeseries(goodDbIdToKeep, updates);
  }

  // Update the observations
  const updateObservationIds = await updateObservations({timeseries: {in: dbIdsToMerge}}, {timeseries: goodDbIdToKeep});
  const nObservationsMerged = updateObservationIds.length;
  logger.debug(`${updateObservationIds.length} observations have been reassigned to timeseries '${goodIdToKeep}'.`);

  // Delete the now merged timeseries
  await Promise.map(dbIdsToMerge, async (dbIdToMerge) => {
    await timeseriesService.deleteTimeseries(dbIdToMerge);
    return;
  });

  logger.debug(`Timeseries successfully merged`);

  // It will be useful for the end user to know how many observations were assigned a new timeseriesId.
  return {
    nObservationsMerged
  };

}


//-------------------------------------------------
// Delete Single Timeseries
//-------------------------------------------------
// Doesn't just delete the timeseries but deletes all its observations too, because the database won't let you delete a timeseries if there are still observations with that timeseries ID.
export async function deleteSingleTimeseries(clientId): Promise<void> {

  // Decode the hashed id.
  const databaseId = timeseriesService.decodeTimeseriesId(clientId);
  logger.debug(`Deleting timeseries with database id '${databaseId}' (clientId: ${clientId})`);

  // Delete the observations
  const nObsDeleted = await deleteObservations({timeseries: databaseId});
  logger.debug(`${nObsDeleted} observations deleted whilst deleting timeseries '${clientId}'.`);

  // Delete the timeseries
  await timeseriesService.deleteTimeseries(databaseId);
  logger.debug(`Timeseries successfully deleted (datebaseId: ${databaseId}, clientId: '${clientId}')`);

  return;

}
