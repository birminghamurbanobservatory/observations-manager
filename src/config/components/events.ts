//-------------------------------------------------
// Dependencies
//-------------------------------------------------
import * as joi from '@hapi/joi';


//-------------------------------------------------
// Validation Schema
//-------------------------------------------------
const schema = joi.object({
  EVENTS_URL: joi.string()
    .uri()
    .required(),
  EVENTS_LOG_LEVEL: joi.string()
  .valid('error', 'warn', 'info', 'debug')
  .default('info')      
}).unknown() // allows for extra fields (i.e that we don't check for) in the object being checked.
  .required();


//-------------------------------------------------
// Validate
//-------------------------------------------------
// i.e. check that process.env contains all the environmental variables we expect/need.
// It's important to use the 'value' that joi.validate spits out from now on, as joi has the power to do type conversion and add defaults, etc, and thus it may be different from the original process.env. 
const {error: err, value: envVars} = schema.validate(process.env);

if (err) {
  throw new Error(`An error occured whilst validating process.env: ${err.message}`);
}


//-------------------------------------------------
// Create config object
//-------------------------------------------------
// Pull out the properties we need to create this particular config object. 
export const events = {
  url: envVars.EVENTS_URL,
  logLevel: envVars.EVENTS_LOG_LEVEL
};
