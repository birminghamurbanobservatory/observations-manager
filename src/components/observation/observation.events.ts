import * as event from 'event-stream';
import {createObservation} from './observation.controller';
import * as logger from 'node-logger';
import {Promise} from 'bluebird'; 
import {logCensorAndRethrow} from '../../events/handle-event-handler-error';
import * as joi from '@hapi/joi';
import {BadRequest} from '../../errors/BadRequest';
import {ObservationClient} from './observation-client.class';


export async function subscribeToObservationEvents(): Promise<void> {

  const subscriptionFunctions = [
    subscribeToObservationCreateRequests
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
  
  const eventName = 'observation.create.request';

  const observationCreateRequestSchema = joi.object({
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
      const {error: err} = observationCreateRequestSchema.validate(message);
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