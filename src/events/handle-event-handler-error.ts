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
      // TODO: I've set this back to .error not .warn because I was missing some serious errors e.g. GeometryMismatch, but I may want to fine tune this some more so that a user request for an obs/timeseries with the wrong ID doesn't trugger a full 'error' level.
      logger.error(`Operational error whilst handling ${eventName} event.`, err);
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