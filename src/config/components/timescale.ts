//-------------------------------------------------
// Dependencies
//-------------------------------------------------
import * as joi from '@hapi/joi';


//-------------------------------------------------
// Validation Schema
//-------------------------------------------------
const schema = joi.object({
  TIMESCALE_HOST: joi.string()
    .required(),
  TIMESCALE_USER: joi.string()
    .required(),
  TIMESCALE_PASSWORD: joi.string()
    .required(),
  TIMESCALE_NAME: joi.string()
    .required()
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
export const timescale = {
  host: envVars.TIMESCALE_HOST,
  user: envVars.TIMESCALE_USER,
  password: envVars.TIMESCALE_PASSWORD,
  name: envVars.TIMESCALE_NAME
};
