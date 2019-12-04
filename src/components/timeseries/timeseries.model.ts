//-------------------------------------------------
// Dependencies
//-------------------------------------------------
import * as mongoose from 'mongoose';


//-------------------------------------------------
// Schema
//-------------------------------------------------

const schema = new mongoose.Schema({
  startDate: {
    type: Date,
    required: true
  },
  endDate: {
    type: Date,
    required: true
  },
  madeBySensor: {
    type: String,
    required: true
  },
  inDeployments: {
    type: [String]
  },
  hostedByPath: {
    type: [String] // came to the conclusion that an Array of Ancestors is easier than a materialized path.
  },
  observedProperty: {
    type: String
  },
  hasFeatureOfInterest: {
    type: String // according to SSN an observation is allowed only 1 featureOfInterest, hence this shouldn't ever be an array.
  },
  usedProcedures: {
    type: [String]
  }
});


//-------------------------------------------------
// Indexes
//-------------------------------------------------
// TODO: Probably a better index than this?
// Use my index-tester tool to find the best depending on the queries I'll make most often.
schema.index({inDeployments: 1, hostedByPath: 1, endDate: 1, startDate: 1});
schema.index({madeBySensor: 1});

//-------------------------------------------------
// Create Model (and expose it to our app)
//-------------------------------------------------
export default mongoose.model('Timeseries', schema);