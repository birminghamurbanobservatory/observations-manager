//-------------------------------------------------
// Dependencies
//-------------------------------------------------
import {config} from './config';
import * as logger from 'node-logger';
const appName = require('../package.json').name; // Annoyingly if i use import here, the built app doesn't update.
import {connectDb} from './db/mongodb-service';
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
logger.warn(`${appName} restarted`);


(async(): Promise<void> => {

  //-------------------------------------------------
  // Database
  //-------------------------------------------------

  //------------------------
  // MongoDB
  //------------------------
  try {
    await connectDb(config.mongo.uri);
    logger.info('Initial connection to MongoDB database was successful');
  } catch (err) {
    logger.error(`Initial MongoDB database connection failed: ${err.message}`);
  }


  //------------------------
  // Timescale DB
  //------------------------
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
      logLevel: config.events.logLevel
    });
  } catch (err) {
    logger.error('There was an issue whilst initialising events.', err);
  }
  return;


})();






