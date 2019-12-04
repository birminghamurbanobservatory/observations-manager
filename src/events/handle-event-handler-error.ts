import {OperationalError} from '../errors/OperationalError';
import {UnexpectedError} from '../errors/UnexpectedError';
import * as logger from 'node-logger';

export function logCensorAndRethrow(eventName, err): any {
  
  //------------------------
  // Operational Errors
  //------------------------
  if (err instanceof OperationalError) {
    logger.warn(`Operational error whilst handling ${eventName} event.`, err);
    throw err;

  //------------------------
  // Programmer Errors
  //------------------------
  } else {
    logger.error(`Unexpected error whilst handling ${eventName} event.`, err);
    // We don't want the event stream to return programmer errors.
    throw new UnexpectedError();

  }

}