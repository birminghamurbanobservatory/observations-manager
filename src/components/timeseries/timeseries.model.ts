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
// TODO: Use my index-tester tool to find the best depending on the queries I'll make most often.
// N.B. You CAN'T have an index with more than one array, as MongoDB thinks it'll get out of hand, which in fairness it could if you had a lot of elements in each.
schema.index({inDeployments: 1, endDate: 1, startDate: 1});
schema.index({hostedByPath: 1, endDate: 1, startDate: 1});
schema.index({madeBySensor: 1});

//-------------------------------------------------
// Create Model (and expose it to our app)
//-------------------------------------------------
export default mongoose.model('Timeseries', schema);