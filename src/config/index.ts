// Load any environmental variables set in the .env file into process.env
import * as dotenv from 'dotenv';
dotenv.config();

// Retrieve each of our configuration components
import * as common from './components/common';
import * as logger from './components/logger';
import * as events from './components/events';
import * as timescale  from './components/timescale'; 
import * as obs from './components/obs';


// Export
export const config = Object.assign({}, common, logger, events, timescale, obs);

