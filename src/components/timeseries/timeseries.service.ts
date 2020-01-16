import {TimeseriesProps} from './timeseries-props.class';
import * as check from 'check-types';
import {TimeseriesApp} from './timeseries-app.class';
import {TimeseriesRow} from './timeseries-row.class';
import {sortBy, cloneDeep} from 'lodash';
import {TimeseriesNotFound} from './errors/TimeseriesNotFound';
import {GetTimeseriesFail} from './errors/GetTimeseriesFail';
import {GetTimeseriesUsingIdsFail} from './errors/GetTimeseriesUsingIdsFail';
import {knex} from '../../db/knex';
import {convertKeysToSnakeCase, convertKeysToCamelCase} from '../../utils/class-converters';
import {stripNullProperties} from '../../utils/strip-null-properties';
import {TimeseriesWhere} from './timeseries-where.class';
import {arrayToLtreeString, ltreeStringToArray, platformIdToAnywhereLquery} from '../../db/db-helpers';
import * as logger from 'node-logger';
import {UpdateTimeseriesFail} from './errors/UpdateTimeseriesFail';
import {CreateTimeseriesFail} from './errors/CreateTimeseriesFail';



export async function createTimeseriesTable(): Promise<void> {

  await knex.schema.createTable('timeseries', (table): void => {
    table.increments('id');
    table.timestamp('first_obs', {useTz: true}).notNullable();
    table.timestamp('last_obs', {useTz: true}).notNullable();
    table.string('made_by_sensor').notNullable();
    table.specificType('in_deployments', 'TEXT[]');
    table.specificType('hosted_by_path', 'ltree');
    table.string('has_feature_of_interest');
    table.string('observed_property');
    table.specificType('used_procedures', 'TEXT[]');

  });

  // TODO: Add some more indexes. Potentially incorporating the start_date and end_date.
  // Add a GIST index for hosted_by_path ltree column
  await knex.raw('CREATE INDEX timeseries_hosted_by_path_index ON timeseries USING GIST (hosted_by_path);');
  // Add a GIN index for in_deployments
  await knex.raw('CREATE INDEX timeseries_in_deployments_index ON timeseres USING GIN(in_deployments)');

  return;
}


export async function createTimeseries(timeseries: TimeseriesApp): Promise<TimeseriesApp> {

  const timeseriesRow = timeseriesAppToDb(timeseries);

  let createdTimeseries: TimeseriesRow;
  try {
    const result = await knex('timeseries')
    .insert(timeseriesRow)
    .returning('*');
    createdTimeseries = result[0];
  } catch (err) {
    throw new CreateTimeseriesFail(undefined, err.message);
  }  

  return timeseriesDbToApp(createdTimeseries);

}


export async function updateTimeseries(id: string, updates: any): Promise<TimeseriesApp> {

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


export async function getTimeseries(id: string): Promise<TimeseriesApp> {

  let timeseriesRow: TimeseriesRow;
  try {
    timeseriesRow = await knex('timeseires')
    .select()
    .where({id})
    .first();
  } catch (err) {
    throw new GetTimeseriesFail(undefined, err.message);
  }

  if (!timeseriesRow) {
    throw new TimeseriesNotFound(`A timeseries with id '${id}' could not be found`);
  }

  return timeseriesDbToApp(timeseriesRow);

}

// i.e. find multiple timeseries
export async function findTimeseries(where: TimeseriesWhere): Promise<TimeseriesApp[]> {

  let timeseries: TimeseriesRow[];

  try {
    timeseries = await knex('timeseries')
    .select()
    .where((builder) => {

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

      // inDeployment
      if (check.assigned(where.inDeployment)) {
        if (check.nonEmptyString(where.inDeployment)) {
          // Find any timeseries whose in_deployments array contains this one deployment (if there are others in the array then it will still match)
          builder.where('in_deployments', '&&', [where.inDeployment]);
        }
        if (check.nonEmptyObject(where.inDeployment)) {
          if (check.nonEmptyArray(where.inDeployment.in)) {
            // i.e. looking for any overlap
            builder.where('in_deployments', '&&', where.inDeployment.in);
          }
          if (check.boolean(where.inDeployment.exists)) {
            if (where.inDeployment.exists === true) {
              builder.whereNotNull('in_deployments');
            } 
            if (where.inDeployment.exists === false) {
              builder.whereNull('in_deployments');
            }              
          }
        }
      }      

      // inDeployments - for an exact match (after sorting alphabetically)
      if (check.assigned(where.inDeployments)) {
        if (check.nonEmptyArray(where.inDeployments)) {
          builder.where('in_deployments', sortBy(where.inDeployments));
        }
        if (check.nonEmptyObject(where.inDeployments)) {
          // Don't yet support the 'in' property here, as not sure how to do an IN with any array of arrays.
          if (check.boolean(where.inDeployments.exists)) {
            if (where.inDeployments.exists === true) {
              builder.whereNotNull('in_deployments');
            } 
            if (where.inDeployments.exists === false) {
              builder.whereNull('in_deployments');
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

      // usedProcedure
      if (check.assigned(where.usedProcedure)) {
        if (check.nonEmptyString(where.usedProcedure)) {
          // Find any timeseries whose used_procedures array contains this one procedure (if there are others in the array then it will still match)
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
    });

  } catch (err) {
    throw new GetTimeseriesFail(undefined, err.message);
  }

  return timeseries.map(timeseriesDbToApp);

}

 

// Simply acts as a wrapper around the findTimeseries function, and either picks the first row, or if no results were found it returns undefined.
// It expects the where argument to have already passed through the convertPropsToExactWhere function.
export async function findSingleMatchingTimeseries(where: TimeseriesWhere): Promise<TimeseriesApp | void> {

  // Let's check every property we'll match by has been specified
  const requiredProps = ['madeBySensor', 'inDeployments', 'hostedByPath', 'observedProperty', 'hasFeatureOfInterest', 'usedProcedures'];
  requiredProps.forEach((prop) => {
    if (check.not.assigned(where[prop])) {
      throw new Error(`The '${prop} propety of the where object must be assigned.'`);
    }
  });

  const timeseriesArray = await findTimeseries(where);

  if (timeseriesArray.length === 0) {
    return undefined;
  } else if (timeseriesArray.length === 1) {
    return timeseriesArray[0];
  } else {
    logger.warn(`This where object should only ever find 1 matching timeseries, however ${timeseriesArray.length} where found.`, {whereObjectUser: where, timeseriesFound: timeseriesArray});
    return timeseriesArray[0]; // let's still return the first match anyway
  }

}




// I.e. to produce a where object aimed at finding a single timeseries.
// This is important, e.g. for making sure that if a property isn't provided then the where object has {exists: false} for it. It also properly handles properties that are an array.
export function convertPropsToExactWhere(props: TimeseriesProps): any {

  const findQuery: any = {};
  const potentialProps = ['madeBySensor', 'inDeployments', 'hostedByPath', 'observedProperty', 'hasFeatureOfInterest', 'usedProcedures'];

  // For the inDeployments array the order has no meaning, and thus we should sort the array just in case the deployments are provided out of order at some point.

  potentialProps.forEach((propKey) => {

    if (check.assigned(props[propKey])) {
      if (propKey === 'inDeployments') {
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




export async function findTimeseriesUsingIds(ids: string[]): Promise<TimeseriesApp[]> {

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


export function timeseriesAppToDb(timeseriesApp: TimeseriesApp): TimeseriesRow {

  const timeseriesRow: any = convertKeysToSnakeCase(timeseriesApp);

  // Make sure hostedByPath is in ltree format
  if (timeseriesRow.hostedByPath) {
    timeseriesRow.hostedByPath = arrayToLtreeString(timeseriesRow.hostedByPath);
  }

  // Make sure inDeployments is sorted, makes it easier to search for exact matches.
  if (timeseriesRow.inDeployments) {
    timeseriesRow.inDeployments = sortBy(timeseriesRow.inDeployments);
  }

  return timeseriesRow;

}



export function timeseriesDbToApp(timeseriesRow: TimeseriesRow): TimeseriesApp {
  // TODO: For some reason stripNullProperties is changing {inDeployments: ['something']} to undefined. 
  const timeseriesApp = convertKeysToCamelCase(stripNullProperties(timeseriesRow));
  if (timeseriesApp.hostedByPath) {
    timeseriesApp.hostedByPath = ltreeStringToArray(timeseriesApp.hostedByPath);
  }
  return timeseriesApp;
}