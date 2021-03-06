import * as event from 'event-stream';
import {createObservation, getObservation, getObservations, updateObservation} from './observation.controller';
import * as logger from 'node-logger';
import {Promise} from 'bluebird'; 
import {logCensorAndRethrow} from '../../events/handle-event-handler-error';
import * as joi from '@hapi/joi';
import {BadRequest} from '../../errors/BadRequest';
import {ObservationClient} from './observation-client.class';


export async function subscribeToObservationEvents(): Promise<void> {

  const subscriptionFunctions = [
    subscribeToObservationCreateRequests,
    subscribeToObservationGetRequests,
    subscribeToObservationsGetRequests,
    subscribeToObservationUpdateRequest
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
// Create observation
//-------------------------------------------------
async function subscribeToObservationCreateRequests(): Promise<any> {
  
  // N.B. The event-stream package changes the configuration of a queue based on whether it contains the work 'request'. Here we leave it out because we want the queue to be durable and lazy to aid the processing of many observations in bulk.
  const eventName = 'observation.create';

  const observationCreateSchema = joi.object({
    new: joi.object({
      // We'll let the controller/model check this.
    })
    .unknown()
    .required()
  }).required();

  await event.subscribe(eventName, async (message): Promise<void> => {

    logger.debug(`New ${eventName} message.`, message);

    let createdObservation: ObservationClient;
    try {
      const {error: err} = observationCreateSchema.validate(message);
      if (err) throw new BadRequest(`Invalid ${eventName} request: ${err.message}`);    
      createdObservation = await createObservation(message.new);
    } catch (err) {
      logCensorAndRethrow(eventName, err);
    }

    return createdObservation;
  });

  logger.debug(`Subscribed to ${eventName} requests`);
  return;
}


//-------------------------------------------------
// Get Observation
//-------------------------------------------------
async function subscribeToObservationGetRequests(): Promise<any> {

  const eventName = 'observation.get.request';

  const observationGetRequestSchema = joi.object({
    where: joi.object({
      id: joi.string().required()
    })
  })
  .required();

  await event.subscribe(eventName, async (message): Promise<void> => {

    logger.debug(`New ${eventName} message.`, message);

    let observation: ObservationClient;
    try {
      const {error: err} = observationGetRequestSchema.validate(message);
      if (err) throw new BadRequest(`Invalid ${eventName} request: ${err.message}`);
      observation = await getObservation(message.where.id);
    } catch (err) {
      logCensorAndRethrow(eventName, err);
    }

    return observation;
  });

  logger.debug(`Subscribed to ${eventName} requests`);
  return;  

}



//-------------------------------------------------
// Get Observations
//-------------------------------------------------
async function subscribeToObservationsGetRequests(): Promise<any> {

  const eventName = 'observations.get.request';

  const observationsGetRequestSchema = joi.object({
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

    let observationsData: {data: any[]; meta: any};
    try {
      const {error: err} = observationsGetRequestSchema.validate(message);
      if (err) throw new BadRequest(`Invalid ${eventName} request: ${err.message}`);
      observationsData = await getObservations(message.where, message.options);
    } catch (err) {
      logCensorAndRethrow(eventName, err);
    }

    return observationsData;
  });

  logger.debug(`Subscribed to ${eventName} requests`);
  return;  

}



//-------------------------------------------------
// Update observation
//-------------------------------------------------
async function subscribeToObservationUpdateRequest(): Promise<any> {
  
  // N.B. The event-stream package changes the configuration of a queue based on whether it contains the work 'request'. Here we leave it out because we want the queue to be durable and lazy to aid the processing of many observations in bulk.
  const eventName = 'observation.update.request';

  const observationUpdateRequestSchema = joi.object({
    where: joi.object({
      id: joi.string().required()
    }).required(),
    updates: joi.object({}) // let the service check this
      .unknown()
      .min(1)
      .required()
  }).required();

  await event.subscribe(eventName, async (message): Promise<void> => {

    logger.debug(`New ${eventName} message.`, message);

    let updatedObservation: ObservationClient;
    try {
      const {error: err} = observationUpdateRequestSchema.validate(message);
      if (err) throw new BadRequest(`Invalid ${eventName} request: ${err.message}`);    
      updatedObservation = await updateObservation(message.where.id, message.updates);
    } catch (err) {
      logCensorAndRethrow(eventName, err);
    }

    return updatedObservation;
  });

  logger.debug(`Subscribed to ${eventName} requests`);
  return;
}