//-------------------------------------------------
// Dependencies
//-------------------------------------------------
import {config} from './config';
import * as logger from 'node-logger';
const appName = require('../package.json').name; // Annoyingly if i use import here, the built app doesn't update.
// import {initialiseEvents} from './events/initialise-events';
import {getCorrelationId} from './utils/correlator';
// Handle Uncaught Errors - Make sure the logger is already configured first.
import './utils/handle-uncaught-errors';
import {initialiseEvents} from './events/initialise-events';
import {initialiseDb} from './db/initialise-timescaledb';


//-------------------------------------------------
// Logging
//-------------------------------------------------
logger.configure(Object.assign({}, config.logger, {getCorrelationId}));
logger.info(`${appName} instance starting`);


(async(): Promise<void> => {

  //-------------------------------------------------
  // Database
  //-------------------------------------------------
  try {
    await initialiseDb();
    logger.info('Timescale DB has been initialised');  
  } catch (err) {
    logger.error('Failed to initialise Timescale DB', err);
  }



  //-------------------------------------------------
  // Events
  //-------------------------------------------------
  try {
    await initialiseEvents({
      url: config.events.url,
      appName,
      logLevel: config.events.logLevel,
      maxMessagesAtOnce: config.events.maxMessagesAtOnce
    });
  } catch (err) {
    logger.error('There was an issue whilst initialising events.', err);
  }
  return;


})();






