import * as event from 'event-stream';
import {getSingleTimeseries, getMultipleTimeseries, deleteSingleTimeseries, mergeTimeseries} from './timeseries.controller';
import * as logger from 'node-logger';
import {Promise} from 'bluebird'; 
import {logCensorAndRethrow} from '../../events/handle-event-handler-error';
import * as joi from '@hapi/joi';
import {BadRequest} from '../../errors/BadRequest';
import {TimeseriesClient} from './timeseries-client.class';


export async function subscribeToTimeseriesEvents(): Promise<void> {

  const subscriptionFunctions = [
    subscribeToSingleTimeseriesGetRequests,
    subscribeToMultipleTimeseriesGetRequests,
    subscribeToSingleTimeseriesDeleteRequests,
    subscribeToTimeseriesMergeRequests
  ];

  // I don't want later subscriptions to be prevented, just because an earlier attempt failed, as I want my event-stream module to have all the event names and handler functions added to its list of subscriptions so it can add them again upon a reconnect.
  await Promise.mapSeries(subscriptionFunctions, async (subscriptionFunction): Promise<void> => {
    try {
      await subscriptionFunction();
    } catch (err) {
      if (err.name === 'NoEventStreamConnection') {
        // If it failed to subscribe because the event-stream connection isn't currently down, I still want it to continue adding the other subscriptions, so that the event-stream module has all the event names and handler functions added to its list of subscriptions so it can add them again upon a reconnect.
        logger.warn(`Failed to subscribe due to event-stream connection being down`);
      } else {
        throw err;
      }
    }
    return;
  });

  return;
}




//-------------------------------------------------
// Get Single Timeseries
//-------------------------------------------------
async function subscribeToSingleTimeseriesGetRequests(): Promise<any> {

  const eventName = 'single-timeseries.get.request';

  const singleTimeseriesGetRequestSchema = joi.object({
    where: joi.object({
      id: joi.string().required()
    })
  })
  .required();

  await event.subscribe(eventName, async (message): Promise<void> => {

    logger.debug(`New ${eventName} message.`, message);

    let timeseries: TimeseriesClient;
    try {
      const {error: err} = singleTimeseriesGetRequestSchema.validate(message);
      if (err) throw new BadRequest(`Invalid ${eventName} request: ${err.message}`);
      timeseries = await getSingleTimeseries(message.where.id);
    } catch (err) {
      logCensorAndRethrow(eventName, err);
    }

    return timeseries;
  });

  logger.debug(`Subscribed to ${eventName} requests`);
  return;  

}



//-------------------------------------------------
// Get Multiple Timeseries
//-------------------------------------------------
async function subscribeToMultipleTimeseriesGetRequests(): Promise<any> {

  const eventName = 'multiple-timeseries.get.request';

  const multipleTimeseriesGetRequestSchema = joi.object({
    where: joi.object({
      // let the controller check the where object
    }).unknown(),
    options: joi.object({
      // let the controller check this
    }).unknown()
  })
  .required();

  await event.subscribe(eventName, async (message): Promise<void> => {

    logger.debug(`New ${eventName} message.`, message);

    let timeseriesData: {data: any[]; meta: any};
    try {
      const {error: err} = multipleTimeseriesGetRequestSchema.validate(message);
      if (err) throw new BadRequest(`Invalid ${eventName} request: ${err.message}`);
      timeseriesData = await getMultipleTimeseries(message.where, message.options);
    } catch (err) {
      logCensorAndRethrow(eventName, err);
    }

    return timeseriesData;
  });

  logger.debug(`Subscribed to ${eventName} requests`);
  return;  

}


//-------------------------------------------------
// Merge Timeseries
//-------------------------------------------------
async function subscribeToTimeseriesMergeRequests(): Promise<any> {

  const eventName = 'timeseries.merge.request';

  const timeseriesMergeRequestSchema = joi.object({
    goodIdToKeep: joi.string(),
    idsToMerge: joi.array().items(joi.string()).min(1)
  })
  .required();

  await event.subscribe(eventName, async (message): Promise<void> => {

    logger.debug(`New ${eventName} message.`, message);

    let result;
    try {
      const {error: err} = timeseriesMergeRequestSchema.validate(message);
      if (err) throw new BadRequest(`Invalid ${eventName} request: ${err.message}`);
      result = await mergeTimeseries(message.goodIdToKeep, message.idsToMerge);
    } catch (err) {
      logCensorAndRethrow(eventName, err);
    }

    return result;
  });

  logger.debug(`Subscribed to ${eventName} requests`);
  return;  

}


//-------------------------------------------------
// Delete Single Timeseries
//-------------------------------------------------
async function subscribeToSingleTimeseriesDeleteRequests(): Promise<any> {

  const eventName = 'single-timeseries.delete.request';

  const singleTimeseriesDeleteRequestSchema = joi.object({
    where: joi.object({
      id: joi.string().required()
    })
  })
  .required();

  await event.subscribe(eventName, async (message): Promise<void> => {

    logger.debug(`New ${eventName} message.`, message);

    try {
      const {error: err} = singleTimeseriesDeleteRequestSchema.validate(message);
      if (err) throw new BadRequest(`Invalid ${eventName} request: ${err.message}`);
      await deleteSingleTimeseries(message.where.id);
    } catch (err) {
      logCensorAndRethrow(eventName, err);
    }

    return;
  });

  logger.debug(`Subscribed to ${eventName} requests`);
  return;  

}