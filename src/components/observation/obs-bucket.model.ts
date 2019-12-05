//-------------------------------------------------
// Dependencies
//-------------------------------------------------
import * as mongoose from 'mongoose';


//-------------------------------------------------
// Schema
//-------------------------------------------------
const resultSchema = new mongoose.Schema({
  value: {
    type: {}, // i.e. mongodb's way of implying 'any'
    required: true
  },
  resultTime: {
    type: Date,
    required: true
  },
  flags: {
    type: [String],
    default: undefined // this stops the default from being an empty array
  }
},
{
  _id: false // this tells mongo not to bother adding a _id field to each of these result sub-documents, as we'll generate the id from the timeseries and the resultTime on the fly.
});

const schema = new mongoose.Schema({
  timeseries: {
    type: String,
    required: true
  },
  // Including the day should give us a pretty performant index
  day: {
    type: Date,
    required: true
  },
  nResults: {
    type: Number
  },
  startDate: {
    type: Date,
    required: true
  },
  endDate: {
    type: Date,
    required: true
  },
  results: {
    type: [resultSchema]
  }
});

// N.B. Makes use of size-based bucketing: https://www.mongodb.com/blog/post/time-series-data-and-mongodb-part-2-schema-design-best-practices


//-------------------------------------------------
// Indexes
//-------------------------------------------------
// Use my index-tester tool to find the best depending on the queries I'll make most often.
schema.index({timeseries:1, day:1, nResults:1});


//-------------------------------------------------
// Create Model (and expose it to our app)
//-------------------------------------------------
export default mongoose.model('Observations', schema);