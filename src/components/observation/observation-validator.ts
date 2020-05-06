import * as joi from '@hapi/joi';
import {validateGeometry} from '../../utils/geojson-validator';
import {ObservationClient} from './observation-client.class';
import {InvalidObservation} from './errors/InvalidObservation';
import {pascalCaseRegex} from '../../utils/regular-expressions';
import {cloneDeep, intersection} from 'lodash';
import {InvalidPhenomenonTime} from './errors/InvalidPhenomenonTime';
import * as check from 'check-types';



const validAggregations = [
  'Instant',
  'Average',
  'Maximum',
  'Minimum',
  'Range',
  'Sum',
  'Count',
  'Variance',
  'StandardDeviation'
];

const createObservationSchema = joi.object({
  madeBySensor: joi.string(),
  hasResult: joi.object({
    value: joi.any().required(),
    unit: joi.string(), 
    flags: joi.array().min(1).items(joi.string())
  }).required(),
  resultTime: joi.string().isoDate().required(),
  phenomenonTime: joi.object({
    hasBeginning: joi.string().isoDate(),
    hasEnd: joi.string().isoDate(),
    duration: joi.number().min(0)
  }).min(2),
  hasDeployment: joi.string(),
  hostedByPath: joi.array().min(1).items(joi.string()),
  hasFeatureOfInterest: joi.string(),
  observedProperty: joi.string().pattern(pascalCaseRegex),
  aggregation: joi.string().valid(...validAggregations).pattern(pascalCaseRegex),
  disciplines: joi.array().min(1).items(joi.string()),
  usedProcedures: joi.array().min(1).items(joi.string()),
  location: joi.object({
    id: joi.string().guid(), // this is the client_id, a uuid,
    validAt: joi.string().isoDate(),
    geometry: joi.object({
      // For now let's only allow Point locations, although the locations table can handle LineStrings and Polygons, allowing them would make some spatial queries a little trickier (e.g. ST_Y won't work with non-points without using ST_CENTROID). Perhaps later down the line I'll need to support LineStrings and Polygons, e.g. for a rainfall radar image so I can capture it's spatial extent. Also consider serving observations to a user in a csv file, it's far easier just to have a column for lat and long than serve them some GeoJSON.
      type: joi.string().valid('Point').required(),
      coordinates: joi.array().required()
    })
    .custom((value) => {
      validateGeometry(value); // throws an error if invalid
      return value;
    })
    .required()
  })
}).required();



export function validateObservation(observation: ObservationClient): ObservationClient {

  const {error: validationErr, value: validObservation} = createObservationSchema.validate(observation);
  if (validationErr) {
    throw new InvalidObservation(`Observation is invalid. Reason: ${validationErr.message}`);
  }

  // Complete the phenomenonTime object if required
  if (validObservation.phenomenonTime) {
    validObservation.phenomenonTime = checkAndCompleteClientPhenomenonTimeObject(validObservation.phenomenonTime);
  }

  const acceptedInstantValue = 'Instant';

  if (!validObservation.phenomenonTime) {
    if (validObservation.aggregation) {
      if (validObservation.aggregation !== acceptedInstantValue) {
        throw new InvalidObservation(`When no phenomenonTime object is provided, the only acceptable value for the 'aggregation' property is '${acceptedInstantValue}'.`);
      }
    } else {
      validObservation.aggregation = acceptedInstantValue;
    }
  }

  return validObservation;

}




export function checkAndCompleteClientPhenomenonTimeObject(obj: {hasBeginning?: string; hasEnd?: string; duration?: number}): any {

  const importantKeys = ['hasBeginning', 'hasEnd', 'duration'];
  const intersectingKeys = intersection(Object.keys(obj), importantKeys);

  if (intersectingKeys.length < 2) {
    throw new InvalidPhenomenonTime(`phenomenonTime object must have at least 2 keys out of ${importantKeys.join(',')}`);
  }

  const complete = cloneDeep(obj);

  if (intersectingKeys.length === 3) {
    const expectedDuration = (new Date(complete.hasEnd).getTime() - new Date(complete.hasBeginning).getTime()) / 1000;
    if (Math.abs(expectedDuration - complete.duration) > 1) {
      throw new InvalidPhenomenonTime(`The duration value of ${complete.duration} has been calulated incorrectly given the supplied hasBeginning and hasEnd properties. Expected: ${expectedDuration}`);
    }

  } else if (check.assigned(complete.hasBeginning) && check.assigned(complete.hasEnd)) {
    complete.duration = (new Date(complete.hasEnd).getTime() - new Date(complete.hasBeginning).getTime()) / 1000;

  } else if (check.assigned(complete.hasBeginning) && check.assigned(complete.duration)) {
    complete.hasEnd = new Date(new Date(complete.hasBeginning).getTime() + (complete.duration * 1000)).toISOString();

  } else if (check.assigned(complete.hasEnd) && check.assigned(complete.duration)) {
    complete.hasBeginning = new Date(new Date(complete.hasEnd).getTime() - (complete.duration * 1000)).toISOString();

  } else {
    throw new Error('Reached unexpected point in checkAndCompleteClientPhenomenonTimeObject function');
  }

  return complete;

}
