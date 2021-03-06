import {TimeseriesProps} from './timeseries-props.class';
import * as check from 'check-types';
import {TimeseriesApp} from './timeseries-app.class';
import {TimeseriesDb} from './timeseries-db.class';
import {sortBy, cloneDeep, isEqual} from 'lodash';
import {TimeseriesNotFound} from './errors/TimeseriesNotFound';
import {GetTimeseriesFail} from './errors/GetTimeseriesFail';
import {GetTimeseriesUsingIdsFail} from './errors/GetTimeseriesUsingIdsFail';
import {knex} from '../../db/knex';
import {convertKeysToSnakeCase, convertKeysToCamelCase} from '../../utils/case-converters';
import {stripNullProperties} from '../../utils/strip-null-properties';
import {TimeseriesWhere} from './timeseries-where.class';
import {arrayToLtreeString, ltreeStringToArray, platformIdToAnywhereLquery} from '../../db/db-helpers';
import * as logger from 'node-logger';
import {UpdateTimeseriesFail} from './errors/UpdateTimeseriesFail';
import {CreateTimeseriesFail} from './errors/CreateTimeseriesFail';
import hasher from '../../utils/hasher';
import {TimeseriesClient} from './timeseries-client.class';
import {arrayToPostgresArrayString} from '../../utils/postgresql-helpers';
import {InvalidTimeseriesId} from './errors/InvalidTimeseriesId';
import {DeleteTimeseriesFail} from './errors/DeleteTimeseriesFail';
import {InvalidObservationId} from '../observation/errors/InvalidObservationId';
import objectHash from 'object-hash';
import * as joi from '@hapi/joi';
import {GetSingleTimeseriesUsingHashFail} from './errors/GetSingleTimeseriesUsingHashFail';
import {TimeseriesAlreadyExists} from './errors/TimeseriesAlreadyExists';



export async function createTimeseriesTable(): Promise<void> {

  await knex.schema.createTable('timeseries', (table): void => {
    table.increments('id');
    table.timestamp('first_obs', {useTz: true}).notNullable();
    table.timestamp('last_obs', {useTz: true}).notNullable();
    table.string('made_by_sensor'); // I've made this nullable for the sake of derived observations with no sensor.
    table.string('has_deployment');
    table.specificType('hosted_by_path', 'ltree');
    table.string('observed_property');
    table.string('aggregation'); // decided string offered more flexibility than an enum.
    table.string('unit');
    table.string('has_feature_of_interest');
    table.specificType('disciplines', 'TEXT[]');
    table.specificType('used_procedures', 'TEXT[]');
    table.string('hash').notNullable();
  });

  // Create a unique index for the hash
  await knex.raw('CREATE UNIQUE INDEX timeseries_uniq_hash_index ON timeseries(hash)');
  // Add a GIST index for hosted_by_path ltree column
  await knex.raw('CREATE INDEX timeseries_hosted_by_path_index ON timeseries USING GIST (hosted_by_path);');
  // This index should come in handy for queries wanting to display observations on a map.
  await knex.raw('CREATE INDEX index_for_map_queries ON timeseries(has_deployment, observed_property, aggregation)');
  // Any more indexes worth adding? E.g. worth having any with the first_obs and last_obs included?

  return;
}


export async function createTimeseries(timeseries: TimeseriesApp): Promise<TimeseriesApp> {

  const timeseriesDb = timeseriesAppToDb(timeseries);

  let createdTimeseries: TimeseriesDb;
  try {
    const result = await knex('timeseries')
    .insert(timeseriesDb)
    .returning('*');
    createdTimeseries = result[0];
  } catch (err) {
    if (err.code === '23505') {
      // N.B. that this code can also be reached if there's a collision with the table's primary key, i.e. the 'id' column, this can occured if the auto-incrementing is messed up because the auto-assigned nextval() assigned a new id lower than that of max id in the table. This happened to me when migrating from Timescale Cloud to Forge. 
      // Stack overflow has some info on how to resolve this.
      // https://stackoverflow.com/questions/244243/how-to-reset-postgres-primary-key-sequence-when-it-falls-out-of-sync
      throw new TimeseriesAlreadyExists(`A timeseries with this set of properties (hash: ${timeseries.hash}) already exists.`);
    } else {
      throw new CreateTimeseriesFail(undefined, err.message);
    }
  }  

  return timeseriesDbToApp(createdTimeseries);

}


export async function updateTimeseries(id: number, updates: any): Promise<TimeseriesApp> {

  const updatesFormatted = timeseriesAppToDb(updates);

  let updatedRows;
  try {
    updatedRows = await knex('timeseries')
    .returning('*') // TODO: need to check this works as expected
    .update(updatesFormatted)
    .where({id}); 
  } catch (err) {
    throw new UpdateTimeseriesFail(undefined, err.message);
  } 

  if (updatedRows.length === 0) {
    throw new Error(`Failed to find a timeseries with id '${id}' to update.`);
  }

  const updatedTimeseries = timeseriesDbToApp(updatedRows[0]);
  return updatedTimeseries;

}


export async function getTimeseries(id: number): Promise<TimeseriesApp> {

  let timeseriesDb: TimeseriesDb;
  try {
    timeseriesDb = await knex('timeseries')
    .select()
    .where({id})
    .first();
  } catch (err) {
    throw new GetTimeseriesFail(undefined, err.message);
  }

  if (!timeseriesDb) {
    throw new TimeseriesNotFound(`A timeseries with id '${id}' could not be found`);
  }

  return timeseriesDbToApp(timeseriesDb);

}

// i.e. find multiple timeseries
export async function findTimeseries(
  where: TimeseriesWhere, 
  options: {
    limit?: number; 
    offset?: number; 
    sortBy?: string; 
    sortOrder?: string;
  } = {}): Promise<{data: TimeseriesApp[]; count: number; total: number}> {

  let timeseries: TimeseriesDb[];

  try {
    timeseries = await knex('timeseries')
    .select('*', knex.raw('count(*) OVER() AS total'))
    .where((builder) => {

      // Matching ids
      if (check.assigned(where.id)) {
        if (check.nonEmptyObject(where.id)) {
          if (check.nonEmptyArray(where.id.in)) {
            builder.whereIn('id', where.id.in);
          }   
        }
      }

      // resultTime
      // This is how we exclude timeseries that have no chance of overlapping with the resultTime range we're interested in.
      if (check.assigned(where.resultTime)) {
        if (check.nonEmptyString(where.resultTime) || check.date(where.resultTime)) {
          // This is in case I allow clients to request observations at an exact resultTime  
          builder.where('first_obs', '<=', where.resultTime);
          builder.where('last_obs', '>=', where.resultTime);
        }
        if (check.nonEmptyObject(where.resultTime)) {
          if (check.assigned(where.resultTime.gte)) {
            builder.where('last_obs', '>=', where.resultTime);
          }
          if (check.assigned(where.resultTime.gt)) {
            builder.where('last_obs', '>', where.resultTime);
          }
          if (check.assigned(where.resultTime.lte)) {
            builder.where('first_obs', '<=', where.resultTime);
          }      
          if (check.assigned(where.resultTime.lt)) {
            builder.where('first_obs', '<', where.resultTime);
          }      

        }
      }

      // firstObs
      if (check.assigned(where.firstObs)) {
        if (check.nonEmptyString(where.firstObs) || check.date(where.firstObs)) { 
          builder.where('first_obs', where.firstObs);
        }
        if (check.nonEmptyObject(where.firstObs)) {
          if (check.assigned(where.firstObs.gte)) {
            builder.where('first_obs', '>=', where.firstObs.gte);
          }
          if (check.assigned(where.firstObs.gt)) {
            builder.where('first_obs', '>', where.firstObs.gt);
          }
          if (check.assigned(where.firstObs.lte)) {
            builder.where('first_obs', '<=', where.firstObs.lte);
          }      
          if (check.assigned(where.firstObs.lt)) {
            builder.where('first_obs', '<', where.firstObs.lt);
          }      
        }
      }

      // lastObs
      if (check.assigned(where.lastObs)) {
        if (check.nonEmptyString(where.lastObs) || check.date(where.lastObs)) {
          builder.where('last_obs', where.lastObs);
        }
        if (check.nonEmptyObject(where.lastObs)) {
          if (check.assigned(where.lastObs.gte)) {
            builder.where('last_obs', '>=', where.lastObs.gte);
          }
          if (check.assigned(where.lastObs.gt)) {
            builder.where('last_obs', '>', where.lastObs.gt);
          }
          if (check.assigned(where.lastObs.lte)) {
            builder.where('last_obs', '<=', where.lastObs.lte);
          }      
          if (check.assigned(where.lastObs.lt)) {
            builder.where('last_obs', '<', where.lastObs.lt);
          }      
        }
      }

      // madeBySensor
      if (check.assigned(where.madeBySensor)) {
        if (check.nonEmptyString(where.madeBySensor)) {
          builder.where('made_by_sensor', where.madeBySensor);
        }
        if (check.nonEmptyObject(where.madeBySensor)) {
          if (check.nonEmptyArray(where.madeBySensor.in)) {
            builder.whereIn('made_by_sensor', where.madeBySensor.in);
          }
          if (check.boolean(where.madeBySensor.exists)) {
            if (where.madeBySensor.exists === true) {
              builder.whereNotNull('made_by_sensor');
            } 
            if (where.madeBySensor.exists === false) {
              builder.whereNull('made_by_sensor');
            }
          }     
        }
      }


      // hasDeployment
      if (check.assigned(where.hasDeployment)) {
        if (check.nonEmptyString(where.hasDeployment)) {
          builder.where('has_deployment', where.hasDeployment);
        }
        if (check.nonEmptyObject(where.hasDeployment)) {
          if (check.nonEmptyArray(where.hasDeployment.in)) {
            builder.whereIn('has_deployment', where.hasDeployment.in);
          }
          if (check.boolean(where.hasDeployment.exists)) {
            if (where.hasDeployment.exists === true) {
              builder.whereNotNull('has_deployment');
            } 
            if (where.hasDeployment.exists === false) {
              builder.whereNull('has_deployment');
            }
          }     
        }
      }
 

      // hostedByPath (used for finding exact matches)
      if (check.assigned(where.hostedByPath)) {
        if (check.nonEmptyArray(where.hostedByPath)) {
          builder.where('hosted_by_path', arrayToLtreeString(where.hostedByPath));
        }
        if (check.nonEmptyObject(where.hostedByPath)) {
          if (check.nonEmptyArray(where.hostedByPath.in)) {
            const ltreeStrings = where.hostedByPath.in.map(arrayToLtreeString);
            builder.where('hosted_by_path', '?', ltreeStrings);
          }
          if (check.boolean(where.hostedByPath.exists)) {
            if (where.hostedByPath.exists === true) {
              builder.whereNotNull('hosted_by_path');
            } 
            if (where.hostedByPath.exists === false) {
              builder.whereNull('hosted_by_path');
            }              
          }
        }
      }

      // isHostedBy
      if (check.assigned(where.isHostedBy)) {
        if (check.nonEmptyString(where.isHostedBy)) {
          builder.where('hosted_by_path', '~', platformIdToAnywhereLquery(where.isHostedBy));
        }
        if (check.nonEmptyObject(where.isHostedBy)) {
          if (check.nonEmptyArray(where.isHostedBy.in)) {
            const ltreeStrings = where.isHostedBy.in.map(platformIdToAnywhereLquery);
            builder.where('hosted_by_path', '?', ltreeStrings);
          }
        }
      }

      // hostedByPathSpecial
      if (check.assigned(where.hostedByPathSpecial)) {
        if (check.nonEmptyString(where.hostedByPathSpecial)) {
          builder.where('hosted_by_path', '~', where.hostedByPathSpecial);
        }
        if (check.nonEmptyObject(where.hostedByPathSpecial)) {
          if (check.nonEmptyArray(where.hostedByPathSpecial.in)) {
            builder.where('hosted_by_path', '?', where.hostedByPathSpecial.in);
          }
        }
      }

      // observedProperty
      if (check.assigned(where.observedProperty)) {
        if (check.nonEmptyString(where.observedProperty)) {
          builder.where('observed_property', where.observedProperty);
        }
        if (check.nonEmptyObject(where.observedProperty)) {
          if (check.nonEmptyArray(where.observedProperty.in)) {
            builder.whereIn('observed_property', where.observedProperty.in);
          }
          if (check.boolean(where.observedProperty.exists)) {
            if (where.observedProperty.exists === true) {
              builder.whereNotNull('observed_property');
            } 
            if (where.observedProperty.exists === false) {
              builder.whereNull('observed_property');
            }
          }     
        }
      }

      // aggregation
      if (check.assigned(where.aggregation)) {
        if (check.nonEmptyString(where.aggregation)) {
          builder.where('aggregation', where.aggregation);
        }
        if (check.nonEmptyObject(where.aggregation)) {
          if (check.nonEmptyArray(where.aggregation.in)) {
            builder.whereIn('aggregation', where.aggregation.in);
          }
          if (check.boolean(where.aggregation.exists)) {
            if (where.aggregation.exists === true) {
              builder.whereNotNull('aggregation');
            } 
            if (where.aggregation.exists === false) {
              builder.whereNull('aggregation');
            }
          }     
        }
      }

      // unit
      if (check.assigned(where.unit)) {
        if (check.nonEmptyString(where.unit)) {
          builder.where('unit', where.unit);
        }
        if (check.nonEmptyObject(where.unit)) {
          if (check.nonEmptyArray(where.unit.in)) {
            builder.whereIn('unit', where.unit.in);
          }
          if (check.boolean(where.unit.exists)) {
            if (where.unit.exists === true) {
              builder.whereNotNull('unit');
            } 
            if (where.unit.exists === false) {
              builder.whereNull('unit');
            }
          }     
        }
      }

      // hasFeatureOfInterest
      if (check.assigned(where.hasFeatureOfInterest)) {
        if (check.nonEmptyString(where.hasFeatureOfInterest)) {
          builder.where('has_feature_of_interest', where.hasFeatureOfInterest);
        }
        if (check.nonEmptyObject(where.hasFeatureOfInterest)) {
          if (check.nonEmptyArray(where.hasFeatureOfInterest.in)) {
            builder.whereIn('has_feature_of_interest', where.hasFeatureOfInterest.in);
          }
          if (check.boolean(where.hasFeatureOfInterest.exists)) {
            if (where.hasFeatureOfInterest.exists === true) {
              builder.whereNotNull('has_feature_of_interest');
            } 
            if (where.hasFeatureOfInterest.exists === false) {
              builder.whereNull('has_feature_of_interest');
            }
          }     
        }
      }

      // discipline
      if (check.assigned(where.discipline)) {
        if (check.nonEmptyString(where.discipline)) {
          builder.where('disciplines', '&&', [where.discipline]);
        }
        if (check.nonEmptyObject(where.discipline)) {
          if (check.nonEmptyArray(where.discipline.in)) {
            // i.e. looking for any overlap
            builder.where('disciplines', '&&', where.discipline.in);
          }
          if (check.boolean(where.discipline.exists)) {
            if (where.discipline.exists === true) {
              builder.whereNotNull('disciplines');
            } 
            if (where.discipline.exists === false) {
              builder.whereNull('disciplines');
            }              
          }
        }
      }      

      // disciplines - for an exact match (after sorting alphabetically)
      if (check.assigned(where.disciplines)) {
        if (check.nonEmptyArray(where.disciplines)) {
          builder.where('disciplines', where.disciplines);
        }
        if (check.nonEmptyObject(where.disciplines)) {
          // Don't yet support the 'in' property here, as not sure how to do an IN with any array of arrays.
          if (check.boolean(where.disciplines.exists)) {
            if (where.disciplines.exists === true) {
              builder.whereNotNull('disciplines');
            } 
            if (where.disciplines.exists === false) {
              builder.whereNull('disciplines');
            }              
          }
          if (check.nonEmptyArray(where.disciplines.not)) {
            // The following approach adds parentheses around these two statements, which is important, otherwise it would return any timeseries with a NULL disciplines value ignoring all the other filters, which is not what we want.
            builder.where((qb) => {
              qb.whereNot('disciplines', where.disciplines.not).orWhereNull('disciplines');
            });
          } 
        }
      }   

      // usedProcedure
      if (check.assigned(where.usedProcedure)) {
        if (check.nonEmptyString(where.usedProcedure)) {
          // Find any timeseries whose used_procedure array contains this one procedure (if there are others in the array then it will still match)
          builder.where('used_procedures', '&&', [where.usedProcedure]);
        }
        if (check.nonEmptyObject(where.usedProcedure)) {
          if (check.nonEmptyArray(where.usedProcedure.in)) {
            // i.e. looking for any overlap
            builder.where('used_procedures', '&&', where.usedProcedure.in);
          }
          if (check.boolean(where.usedProcedure.exists)) {
            if (where.usedProcedure.exists === true) {
              builder.whereNotNull('used_procedures');
            } 
            if (where.usedProcedure.exists === false) {
              builder.whereNull('used_procedures');
            }              
          }
        }
      }  

      // usedProcedures (for an exact match)
      if (check.assigned(where.usedProcedures)) {
        if (check.nonEmptyArray(where.usedProcedures)) {
          builder.where('used_procedures', where.usedProcedures);
        }
        if (check.nonEmptyObject(where.usedProcedures)) {
          // Don't yet support the 'in' property here, as not sure how to do an IN with any array of arrays.
          if (check.boolean(where.usedProcedures.exists)) {
            if (where.usedProcedures.exists === true) {
              builder.whereNotNull('used_procedures');
            } 
            if (where.usedProcedures.exists === false) {
              builder.whereNull('used_procedures');
            }              
          }
        }
      }
      
    })
    .limit(options.limit || 10000)
    .offset(options.offset || 0)
    .orderBy([{column: options.sortBy || 'id', order: options.sortOrder || 'asc'}]);

  } catch (err) {
    throw new GetTimeseriesFail(undefined, err.message);
  }

  const first = timeseries[0];

  const result = {
    data: timeseries.map(timeseriesDbToApp),
    count: timeseries.length,
    total: 0
  };

  if (first) {
    result.total = Number(first.total);
  }

  return result;

}




// Simply acts as a wrapper around the findTimeseries function, and either picks the first row, or if no results were found it returns undefined.
// It expects the where argument to have already passed through the convertPropsToExactWhere function.
export async function findSingleMatchingTimeseries(where: TimeseriesWhere): Promise<TimeseriesApp | void> {

  // Let's check every property we'll match by has been specified
  const requiredProps = ['madeBySensor', 'hasDeployment', 'hostedByPath', 'observedProperty', 'aggregation', 'unit', 'hasFeatureOfInterest', 'disciplines', 'usedProcedures'];
  requiredProps.forEach((prop) => {
    if (check.not.assigned(where[prop])) {
      throw new Error(`The '${prop} property of the where object must be assigned.'`);
    }
  });

  const {data: timeseriesArray} = await findTimeseries(where);

  if (timeseriesArray.length === 0) {
    return undefined;
  } else if (timeseriesArray.length === 1) {
    return timeseriesArray[0];
  } else {
    logger.warn(`This where object should only ever find 1 matching timeseries, however ${timeseriesArray.length} where found.`, {whereObjectUser: where, timeseriesFound: timeseriesArray});
    return timeseriesArray[0]; // let's still return the first match anyway
  }

}



export async function deleteTimeseries(id: number): Promise<void> {

  let nRowsAffected: number;
  try {
    nRowsAffected = await knex('timeseries')
    .where({id})
    .del();
  } catch (err) {
    throw new DeleteTimeseriesFail(undefined, err.message);
  }

  if (nRowsAffected === 0) {
    throw new TimeseriesNotFound(`A timeseries with id '${id}' could not be found`);
  }

  if (nRowsAffected > 1) {
    logger.error(`${nRowsAffected} rows were deleted when attempting to delete timeseries ${id}. Only 1 row should have been deleted.`);
  }

  return;

}



// I.e. to produce a where object aimed at finding a single timeseries.
// This is important, e.g. for making sure that if a property isn't provided then the where object has {exists: false} for it. It also properly handles properties that are an array.
export function convertPropsToExactWhere(props: TimeseriesProps): any {

  const findQuery: any = {};
  const potentialProps = ['madeBySensor', 'hasDeployment', 'hostedByPath', 'observedProperty', 'aggregation', 'unit', 'hasFeatureOfInterest', 'disciplines', 'usedProcedures'];
  const orderNotImportantProps = ['disciplines'];
  // For the disciplines array the order has no meaning, and thus we should sort the array just in case they are every provided in a different order at some point. It's crucial these array properties are also sorted in the same order before saving a new timeseries row.

  potentialProps.forEach((propKey) => {

    if (check.assigned(props[propKey])) {
      if (orderNotImportantProps.includes(propKey)) {
        findQuery[propKey] = sortBy(props[propKey]);
      } else {
        findQuery[propKey] = props[propKey];
      }
    } else {
      findQuery[propKey] = {exists: false};
    }

  });

  return findQuery;
}



export async function findTimeseriesUsingIds(ids: number[]): Promise<TimeseriesApp[]> {

  let timeseries;
  try {
    timeseries = await knex('timeseries')
    .select()
    .whereIn('id', ids);
  } catch (err) {
    throw new GetTimeseriesUsingIdsFail(undefined, err.message);
  }

  return timeseries.map(timeseriesDbToApp);  

}


export async function findSingleTimeseriesUsingHash(hash: string): Promise<TimeseriesApp> {

  check.assert.nonEmptyString(hash);

  let timeseriesDb: TimeseriesDb;
  try {
    timeseriesDb = await knex('timeseries')
    .select()
    .where({hash})
    .first();
  } catch (err) {
    throw new GetSingleTimeseriesUsingHashFail(undefined, err.message);
  }

  if (!timeseriesDb) {
    throw new TimeseriesNotFound(`A timeseries with hash '${hash}' could not be found`);
  }

  return timeseriesDbToApp(timeseriesDb);

}



const generateHashTimeseriesSchema = joi.object({
  madeBySensor: joi.string(),
  hasDeployment: joi.string(),
  hostedByPath: joi.array().min(1).items(joi.string()),
  hasFeatureOfInterest: joi.string(),
  observedProperty: joi.string(),
  aggregation: joi.string(),
  disciplines: joi.array().min(1).items(joi.string()),
  usedProcedures: joi.array().min(1).items(joi.string()),
  unit: joi.string()
})
.min(1)
.required();

// Decided to build the hash from properties in app form, i.e. camel case, rather than the database's snake case
export function generateHashFromTimeseriesProps(timeseriesProps: TimeseriesProps): string {

  const {error: err, value: validProps} = generateHashTimeseriesSchema.validate(timeseriesProps);
  if (err) {
    throw Error(`Invalid timeseries props. ${err.message}`);
  }

  // Make sure that any array fields, where the order has no meaning, are sorted.
  const orderNotImportantProps = ['disciplines'];
  orderNotImportantProps.forEach((prop) => {
    if (check.assigned(validProps[prop])) {
      validProps[prop] = sortBy(validProps[prop]);
    }
  });

  const hash = objectHash(validProps);
  return hash;

}



export function timeseriesAppToDb(timeseriesApp: TimeseriesApp): TimeseriesDb {

  const timeseriesDb: any = convertKeysToSnakeCase(timeseriesApp);

  // Make sure hostedByPath is in ltree format
  if (timeseriesDb.hosted_by_path) {
    timeseriesDb.hosted_by_path = arrayToLtreeString(timeseriesDb.hosted_by_path);
  }

  // Make sure that any array fields, where the order has no meaning, are sorted.
  const orderNotImportantProps = ['disciplines'];
  orderNotImportantProps.forEach((prop) => {
    if (check.assigned(timeseriesDb[prop])) {
      timeseriesDb[prop] = sortBy(timeseriesDb[prop]);
    }
  });


  return timeseriesDb;

}



export function timeseriesDbToApp(timeseriesDb: TimeseriesDb): TimeseriesApp {
  const timeseriesApp = convertKeysToCamelCase(stripNullProperties(timeseriesDb));
  if (timeseriesApp.hostedByPath) {
    timeseriesApp.hostedByPath = ltreeStringToArray(timeseriesApp.hostedByPath);
  }
  delete timeseriesApp.total;
  return timeseriesApp;
}


export function timeseriesAppToClient(timeseriesApp: TimeseriesApp): TimeseriesClient {
  const timeseriesClient: any = cloneDeep(timeseriesApp); 
  timeseriesClient.id = encodeTimeseriesId(timeseriesClient.id);
  timeseriesClient.firstObs = timeseriesClient.firstObs.toISOString();
  timeseriesClient.lastObs = timeseriesClient.lastObs.toISOString();
  delete timeseriesClient.hash; // keep this within just this microservice.
  return timeseriesClient;
}


export function encodeTimeseriesId(databaseId: number): string {
  const encoded = hasher.encode(databaseId);
  return encoded;
}


export function decodeTimeseriesId(clientId): number {
  const decodedId = Number(hasher.decode(clientId));
  // If a client just enters a random string, this .decode method will typically return 0.
  if (decodedId < 1) {
    throw new InvalidTimeseriesId;
  }
  // It's possible that the client could provide a long string as client id. When decoded such a large number would be written in the exponent form (e.g. 1.4216729617808857e+29) when it's converted to a string, which is what knex/PostgreSQL does. Therefore we want to catch this here, rather than letting postgresql catch it.
  if (decodedId.toString().includes('e+')) {
    throw new InvalidTimeseriesId;
  }
  // Because our timeseries IDs are held in an integer column they can only ever go up to 2,147,483,647 anyway
  if (decodedId > 2147483647) {
    throw new InvalidTimeseriesId;
  }
  return decodedId;
}