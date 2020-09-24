import {OperationalError} from '../errors/OperationalError';
import {UnexpectedError} from '../errors/UnexpectedError';
import * as logger from 'node-logger';
import {DatabaseError} from '../errors/DatabaseError';

export function logCensorAndRethrow(eventName, err): any {
  
  //------------------------
  // Operational Errors
  //------------------------
  if (err instanceof OperationalError) {
    
    if (err instanceof DatabaseError) {
      logger.error(`Database error whilst handling ${eventName} event.`, err);
    } else {
      // For example if a user requests a resource with the wrong ID I only want a 'warn' error not a full 'error' level error. 
      // TODO: Might want to fine tune this a bit, as I'm missing some important errors sometimes, e.g. GeometryMismatch
      logger.warn(`Operational error whilst handling ${eventName} event.`, err);
    }
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