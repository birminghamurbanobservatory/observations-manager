import {ObservationCore} from './observation-core.class';
import {ObservationApp} from './observation-app.class';
import {pick, cloneDeep, sortBy} from 'lodash';
import {TimeseriesProps} from '../timeseries/timeseries-props.class';
import {ObservationClient} from './observation-client.class';
import {ObservationDb} from './observation-db.class';
import {GetObservationByIdFail} from './errors/GetObservationByIdFail';
import {ObservationNotFound} from './errors/ObservationNotFound';
import {ObservationAlreadyExists} from './errors/ObservationAlreadyExists';
import {knex} from '../../db/knex';
import * as check from 'check-types';
import {CreateObservationFail} from './errors/CreateObservationFail';
import {GetObservationsFail} from './errors/GetObservationsFail';
import {ObservationsWhere} from './observations-where.class';
import {stripNullProperties} from '../../utils/strip-null-properties';
import {convertKeysToCamelCase} from '../../utils/class-converters';
import {locationAppToClient} from '../location/location.service';
import {ltreeStringToArray, platformIdToAnywhereLquery, arrayToLtreeString} from '../../db/db-helpers';



export async function createObservationsTable(): Promise<void> {

  await knex.schema.createTable('observations', (table): void => {

    table.specificType('id', 'BIGSERIAL'); // Don't set this as primary or else create_hypertable won't work.
    table.integer('timeseries', 24).notNullable(); // 24 is the length of a Mongo ObjectID string
    table.timestamp('result_time', {useTz: true}).notNullable();
    table.timestamp('has_beginning', {useTz: true});
    table.timestamp('has_end', {useTz: true});
    table.bigInteger('location');
    table.specificType('value_number', 'numeric');
    table.boolean('value_boolean');
    table.text('value_text');
    table.jsonb('value_json');
    table.specificType('flags', 'text ARRAY'); // https://www.postgresql.org/docs/9.1/arrays.html

    table.foreign('location')
    .references('id')
    .inTable('locations')
    .withKeyName('location_id');

  });

  // Create the hypertable
  await knex.raw(`SELECT create_hypertable('observations', 'result_time');`);
  // TODO: By default this will create an index called observations_result_time_idx, should I delete this given that I'll create one below that also uses the timeseries and will be used instead anyway? 
  // N.B. if you want a custom primary key, or a unique index, then you must include the result_time.
  // docs: https://docs.timescale.com/latest/using-timescaledb/schema-management#indexing
  await knex.raw('CREATE UNIQUE INDEX timeseries_spatiotemporal_uniq_idx ON observations (timeseries, result_time, location DESC)');
  // N.B. we need the COALESCE for instances when no location is provided, i.e. the column stores a NULL value. Without it two identical locationless observations wouldn't trigger a duplicate error, because NULL counts as distinct each time.

  return;
}


//-------------------------------------------------
// Save Observation
//-------------------------------------------------
export async function saveObservation(obsCore: ObservationCore, timeseriesId: number): Promise<any> {

  const observationDb = buildObservationDb(obsCore, timeseriesId);

  let createdObservation: ObservationDb;
  try {
    const result = await knex('observations')
    .insert(observationDb)
    .returning('*');
    createdObservation = result[0];
  } catch (err) {
    if (err.code === '23505') {
      throw new ObservationAlreadyExists(`An observation with a resultTime of ${obsCore.resultTime.toISOString()} and location id '${obsCore.location}' already exists for the timeseries with id: '${timeseriesId}'.`);
    } else {
      throw new CreateObservationFail(undefined, err.message);
    }
  }

  return observationDbToCore(createdObservation);

}


const columnsToSelectDuringJoin = [
  'observations.id AS id',
  'observations.timeseries as timeseries_id',
  'observations.location as location_id',
  'observations.result_time',
  'observations.has_beginning',
  'observations.has_end',
  'observations.value_number',
  'observations.value_boolean',
  'observations.value_text',
  'observations.value_json',
  'observations.flags',
  'timeseries.made_by_sensor',
  'timeseries.in_deployments',
  'timeseries.hosted_by_path',
  'timeseries.has_feature_of_interest',
  'timeseries.observed_property',
  'timeseries.used_procedures',
  'locations.client_id as location_client_id',
  'locations.geojson as location_geojson',
  'locations.valid_at as location_valid_at'    
];


//-------------------------------------------------
// Get Observation
//-------------------------------------------------
export async function getObservationByClientId(id: string): Promise<ObservationApp> {

  const {timeseriesId, resultTime, locationId} = deconstructObservationId(id);

  let foundObservation;
  try {
    const result = await knex('observations')
    .select(columnsToSelectDuringJoin)
    .leftJoin('timeseries', 'observations.timeseries', 'timeseries.id')
    .leftJoin('locations', 'observations.location', 'locations.id')
    .where((builder) => {
      builder.where('timeseries', timeseriesId);
      builder.where('result_time', resultTime);
      if (locationId) {
        builder.where('location', locationId);
      } else {
        builder.whereNull('location');
      }
    });
    foundObservation = result[0];
  } catch (err) {
    throw new GetObservationByIdFail(undefined, err.message);
  }

  if (!foundObservation) {
    throw new ObservationNotFound(`Failed to find an observation with ID '${id}'`);
  }

  return observationDbToApp(foundObservation);

}


export async function getObservations(where: ObservationsWhere, options: {limit?: number, offset?: number}): Promise<ObservationApp[]> {

  let observations;
  try {
    observations = await knex('observations')
    .select(columnsToSelectDuringJoin)
    .leftJoin('timeseries', 'observations.timeseries', 'timeseries.id')
    .leftJoin('locations', 'observations.location', 'locations.id')
    .where((builder) => {

      // resultTime
      if (check.assigned(where.resultTime)) {
        if (check.nonEmptyString(where.resultTime) || check.date(where.resultTime)) {
          // This is in case I allow clients to request observations at an exact resultTime  
          builder.where('observations.resultTime', where.resultTime);
        }
        if (check.nonEmptyObject(where.resultTime)) {
          if (check.assigned(where.resultTime.gte)) {
            builder.where('observations.resultTime', '>=', where.resultTime);
          }
          if (check.assigned(where.resultTime.gt)) {
            builder.where('observations.resultTime', '>', where.resultTime);
          }
          if (check.assigned(where.resultTime.lte)) {
            builder.where('observations.resultTime', '<=', where.resultTime);
          }      
          if (check.assigned(where.resultTime.lt)) {
            builder.where('observations.resultTime', '<', where.resultTime);
          }      

        }
      }

      // madeBySensor
      if (check.assigned(where.madeBySensor)) {
        if (check.nonEmptyString(where.madeBySensor)) {
          builder.where('timeseries.made_by_sensor', where.madeBySensor);
        }
        if (check.nonEmptyObject(where.madeBySensor)) {
          if (check.nonEmptyArray(where.madeBySensor.in)) {
            builder.whereIn('timeseries.made_by_sensor', where.madeBySensor.in);
          }
          if (check.boolean(where.madeBySensor.exists)) {
            if (where.madeBySensor.exists === true) {
              builder.whereNotNull('timeseries.made_by_sensor');
            } 
            if (where.madeBySensor.exists === false) {
              builder.whereNull('timeseries.made_by_sensor');
            }
          }     
        }
      }

      // inDeployment
      if (check.assigned(where.inDeployment)) {
        if (check.nonEmptyString(where.inDeployment)) {
          // Find any timeseries whose in_deployments array contains this one deployment (if there are others in the array then it will still match)
          builder.where('timeseries.in_deployments', '&&', [where.inDeployment]);
        }
        if (check.nonEmptyObject(where.inDeployment)) {
          if (check.nonEmptyArray(where.inDeployment.in)) {
            // i.e. looking for any overlap
            builder.where('timeseries.in_deployments', '&&', where.inDeployment.in);
          }
          if (check.boolean(where.inDeployment.exists)) {
            if (where.inDeployment.exists === true) {
              builder.whereNotNull('timeseries.in_deployments');
            } 
            if (where.inDeployment.exists === false) {
              builder.whereNull('timeseries.in_deployments');
            }              
          }
        }
      }      

      // inDeployments - for an exact match (after sorting alphabetically)
      if (check.assigned(where.inDeployments)) {
        if (check.nonEmptyArray(where.inDeployments)) {
          builder.where('timeseries.in_deployments', sortBy(where.inDeployments));
        }
        if (check.nonEmptyObject(where.inDeployments)) {
          // Don't yet support the 'in' property here, as not sure how to do an IN with any array of arrays.
          if (check.boolean(where.inDeployments.exists)) {
            if (where.inDeployments.exists === true) {
              builder.whereNotNull('timeseries.in_deployments');
            } 
            if (where.inDeployments.exists === false) {
              builder.whereNull('timeseries.in_deployments');
            }              
          }
        }
      }      

      // hostedByPath (used for finding exact matches)
      if (check.assigned(where.hostedByPath)) {
        if (check.nonEmptyArray(where.hostedByPath)) {
          builder.where('timeseries.hosted_by_path', arrayToLtreeString(where.hostedByPath));
        }
        if (check.nonEmptyObject(where.hostedByPath)) {
          if (check.nonEmptyArray(where.hostedByPath.in)) {
            const ltreeStrings = where.hostedByPath.in.map(arrayToLtreeString);
            builder.where('timeseries.hosted_by_path', '?', ltreeStrings);
          }
          if (check.boolean(where.hostedByPath.exists)) {
            if (where.hostedByPath.exists === true) {
              builder.whereNotNull('timeseries.hosted_by_path');
            } 
            if (where.hostedByPath.exists === false) {
              builder.whereNull('timeseries.hosted_by_path');
            }              
          }
        }
      }

      // isHostedBy
      if (check.assigned(where.isHostedBy)) {
        if (check.nonEmptyString(where.isHostedBy)) {
          builder.where('timeseries.hosted_by_path', '~', platformIdToAnywhereLquery(where.isHostedBy));
        }
        if (check.nonEmptyObject(where.isHostedBy)) {
          if (check.nonEmptyArray(where.isHostedBy.in)) {
            const ltreeStrings = where.isHostedBy.in.map(platformIdToAnywhereLquery);
            builder.where('timeseries.hosted_by_path', '?', ltreeStrings);
          }
        }
      }

      // hostedByPathSpecial
      if (check.assigned(where.hostedByPathSpecial)) {
        if (check.nonEmptyString(where.hostedByPathSpecial)) {
          builder.where('timeseries.hosted_by_path', '~', where.hostedByPathSpecial);
        }
        if (check.nonEmptyObject(where.hostedByPathSpecial)) {
          if (check.nonEmptyArray(where.hostedByPathSpecial.in)) {
            builder.where('timeseries.hosted_by_path', '?', where.hostedByPathSpecial.in);
          }
        }
      }


      // hasFeatureOfInterest
      if (check.assigned(where.hasFeatureOfInterest)) {
        if (check.nonEmptyString(where.hasFeatureOfInterest)) {
          builder.where('timeseries.has_feature_of_interest', where.hasFeatureOfInterest);
        }
        if (check.nonEmptyObject(where.hasFeatureOfInterest)) {
          if (check.nonEmptyArray(where.hasFeatureOfInterest.in)) {
            builder.whereIn('timeseries.has_feature_of_interest', where.hasFeatureOfInterest.in);
          }
          if (check.boolean(where.hasFeatureOfInterest.exists)) {
            if (where.hasFeatureOfInterest.exists === true) {
              builder.whereNotNull('timeseries.has_feature_of_interest');
            } 
            if (where.hasFeatureOfInterest.exists === false) {
              builder.whereNull('timeseries.has_feature_of_interest');
            }
          }     
        }
      }

      // observedProperty
      if (check.assigned(where.observedProperty)) {
        if (check.nonEmptyString(where.observedProperty)) {
          builder.where('timeseries.observed_property', where.observedProperty);
        }
        if (check.nonEmptyObject(where.observedProperty)) {
          if (check.nonEmptyArray(where.observedProperty.in)) {
            builder.whereIn('timeseries.observed_property', where.observedProperty.in);
          }
          if (check.boolean(where.observedProperty.exists)) {
            if (where.observedProperty.exists === true) {
              builder.whereNotNull('timeseries.observed_property');
            } 
            if (where.observedProperty.exists === false) {
              builder.whereNull('timeseries.observed_property');
            }
          }     
        }
      }

      // usedProcedure
      if (check.assigned(where.usedProcedure)) {
        if (check.nonEmptyString(where.usedProcedure)) {
          // Find any timeseries whose used_procedures array contains this one procedure (if there are others in the array then it will still match)
          builder.where('timeseries.used_procedures', '&&', [where.usedProcedure]);
        }
        if (check.nonEmptyObject(where.usedProcedure)) {
          if (check.nonEmptyArray(where.usedProcedure.in)) {
            // i.e. looking for any overlap
            builder.where('timeseries.used_procedures', '&&', where.usedProcedure.in);
          }
          if (check.boolean(where.usedProcedure.exists)) {
            if (where.usedProcedure.exists === true) {
              builder.whereNotNull('timeseries.used_procedures');
            } 
            if (where.usedProcedure.exists === false) {
              builder.whereNull('timeseries.used_procedures');
            }              
          }
        }
      }  

      // usedProcedures (for an exact match)
      if (check.assigned(where.usedProcedures)) {
        if (check.nonEmptyArray(where.usedProcedures)) {
          builder.where('timeseries.used_procedures', where.usedProcedures);
        }
        if (check.nonEmptyObject(where.usedProcedures)) {
          // Don't yet support the 'in' property here, as not sure how to do an IN with any array of arrays.
          if (check.boolean(where.usedProcedures.exists)) {
            if (where.usedProcedures.exists === true) {
              builder.whereNotNull('timeseries.used_procedures');
            } 
            if (where.usedProcedures.exists === false) {
              builder.whereNull('timeseries.used_procedures');
            }              
          }
        }
      }

      // TODO: add spatial queries
      // TODO: filter by flags
      // Allow =, >=, <, etc on the numeric values.

    })
    .limit(options.limit || 100000)
    .offset(options.offset || 0);
  } catch (err) {
    throw new GetObservationsFail(undefined, err.message);
  }

  return observations.map(observationDbToApp);

}



export function extractCoreFromObservation(observation: ObservationApp): ObservationCore {
  const obsCore: ObservationCore = {
    value: observation.hasResult.value,
    resultTime: observation.resultTime
  };
  if (observation.hasResult.flags) {
    obsCore.flags = observation.hasResult.flags;
  }
  if (observation.phenomenonTime) {
    if (observation.phenomenonTime.hasBeginning) {
      obsCore.hasBeginning = observation.phenomenonTime.hasBeginning;
    }
    if (observation.phenomenonTime.hasEnd) {
      obsCore.hasEnd = observation.phenomenonTime.hasEnd;
    }
  }
  return obsCore;
}



export function extractTimeseriesPropsFromObservation(observation: ObservationApp): TimeseriesProps {

  const props = pick(observation, [
    'madeBySensor',
    'inDeployments',
    'hostedByPath',
    'observedProperty',
    'hasFeatureOfInterest',
    'usedProcedures'
  ]);

  return props;
}



// N.B: This approach won't work if there's ever a situation when you get more that one observation from a given timeseries at the same resultTime and location. The most likely reason you'd have two observations at the same time is if you apply a procedure that manipulated the data in some way, however this would change the userProcedures array, and therefore the timeseriesId, so this particular example doesn't pose any issues to using this approach.
// N.B. The location id is included, because perhaps you have a sensor that can make simulataneous observations as several locations, if you don't incorporate the location then it will only allow you to save a single observation as they'll all have the same resultTime.
// N.B. we default to the locationId to 0 if no locationId is given, e.g. the obs didn't have a specific location.
export function generateObservationId(timeseriesId: number, resultTime: string | Date, locationId?): string {
  return `${timeseriesId}-${locationId || 0}-${new Date(resultTime).toISOString()}`;
}



export function deconstructObservationId(id: string): {timeseriesId: number; resultTime: Date; locationId?: number} {
  const firstSplit = nthIndex(id, '-', 1);
  const secondSplit = nthIndex(id, '-', 2);
  const components: any = {
    timeseriesId: Number(id.slice(0, firstSplit)),
    resultTime: new Date(id.slice(secondSplit + 1, id.length))
  }; 
  const locationId = Number(id.slice(firstSplit + 1, secondSplit));
  if (locationId !== 0) {
    components.locationId = locationId;
  }
  return components;
}


function nthIndex(str: string, subString: string, nthOccurrence: number): number {
  const L = str.length;
  let i = -1;
  let n = nthOccurrence;
  while (n-- && i++ < L) {
    i = str.indexOf(subString, i);
    if (i < 0) break;
  }
  return i;
}



export function observationDbToCore(observationDb: ObservationDb): ObservationCore {

  const obsCore: ObservationCore = {
    timeseries: Number(observationDb.timeseries),
    resultTime: new Date(observationDb.result_time)
  };
  if (observationDb.flags) {
    obsCore.flags = observationDb.flags;
  }
  if (observationDb.has_beginning) {
    obsCore.hasBeginning = new Date(observationDb.has_beginning);
  }
  if (observationDb.has_end) {
    obsCore.hasEnd = new Date(observationDb.has_end);
  }

  // Find the column that's not null
  if (observationDb.value_number !== null) {
    // For some reason the values from the value_number column are coming back as strings, so lets convert them back to a number here.
    obsCore.value = Number(observationDb.value_number);
  } else if (observationDb.value_boolean !== null) {
    obsCore.value = observationDb.value_boolean;
  } else if (observationDb.value_text !== null) {
    obsCore.value = observationDb.value_text;
  } else if (observationDb.value_json !== null) {
    obsCore.value = observationDb.value_json;
  }

  return obsCore;

}


export function buildObservationDb(obsCore: ObservationCore, timeseriesId: number): ObservationDb {
  
  const observationDb: ObservationDb = {
    result_time: obsCore.resultTime.toISOString(),
    timeseries: timeseriesId
  };
  if (obsCore.flags && obsCore.flags.length > 0) {
    observationDb.flags = obsCore.flags;
  }
  if (obsCore.location) {
    observationDb.location = obsCore.location;
  }
  if (obsCore.hasBeginning) {
    observationDb.has_beginning = obsCore.hasBeginning.toISOString();
  }
  if (obsCore.hasEnd) {
    observationDb.has_end = obsCore.hasEnd.toISOString();
  }

  if (check.number(obsCore.value)) {
    observationDb.value_number = obsCore.value;

  } else if (check.boolean(obsCore.value)) {
    observationDb.value_boolean = obsCore.value;

  } else if (check.string(obsCore.value)) {
    observationDb.value_text = obsCore.value;

  } else if (check.object(obsCore.value) || check.array(obsCore.value)) {
    observationDb.value_json = obsCore.value;

  } else {
    throw new Error(`Unexpected observation value: ${obsCore.value}`);
  }

  return observationDb;

}


export function observationDbToApp(observationDb): ObservationApp {

  const observationApp: any = convertKeysToCamelCase(observationDb);

  observationApp.id = Number(observationApp.id); // comes back as string for some reason.
  observationApp.resultTime = new Date(observationApp.resultTime);
  observationApp.clientId = generateObservationId(
    observationApp.timeseriesId, 
    observationApp.resultTime, 
    observationApp.locationId
  );

  if (observationApp.locationId) {
    observationApp.location = {
      id: observationApp.locationId,
      clientId: observationApp.locationClientId,
      geometry: observationApp.locationGeojson,
      validAt: new Date(observationApp.locationValidAt)
    };
  }

  delete observationApp.locationId;
  delete observationApp.locationClientId;
  delete observationApp.locationGeojson;
  delete observationApp.locationValidAt;

  if (observationApp.hasBeginning || observationApp.hasEnd) {
    observationApp.phenomenonTime = {};
    if (observationApp.hasBeginning) {
      observationApp.phenomenonTime.hasBeginning = new Date(observationApp.hasBeginning);
      delete observationApp.hasBeginning;
    }
    if (observationApp.hasEnd) {
      observationApp.phenomenonTime.hasEnd = new Date(observationApp.hasEnd);
      delete observationApp.hasEnd;
    }
  }

  observationApp.hasResult = {};

  // Find the value column that's not null
  if (observationApp.valueNumber !== null) {
    // For some reason the values from the value_number column are coming back as strings, so lets convert them back to a number here.
    observationApp.hasResult.value = Number(observationApp.valueNumber);
  } else if (observationApp.valueBoolean !== null) {
    observationApp.hasResult.value = observationApp.valueBoolean;
  } else if (observationApp.valueText !== null) {
    observationApp.hasResult.value = observationApp.valueText;
  } else if (observationApp.valueJson !== null) {
    observationApp.hasResult.value = observationApp.valueJson;
  }  

  delete observationApp.valueNumber;
  delete observationApp.valueBoolean;
  delete observationApp.valueText;
  delete observationApp.valueJson;

  if (observationApp.flags) {
    observationApp.hasResult.flags = observationApp.flags;
  }
  delete observationApp.flags;

  if (observationApp.hostedByPath) {
    observationApp.hostedByPath = ltreeStringToArray(observationApp.hostedByPath);
  }

  const observationAppNoNulls = stripNullProperties(observationApp);
  return observationAppNoNulls;

}


export function observationClientToApp(observationClient: ObservationClient): ObservationApp {
  const observationApp: any = cloneDeep(observationClient);
  observationApp.resultTime = new Date(observationApp.resultTime);
  if (observationApp.phenomenonTime) {
    if (observationApp.phenomenonTime.hasBeginning) {
      observationApp.phenomenonTime.hasBeginning = new Date(observationApp.phenomenonTime.hasBeginning);
    }
    if (observationApp.phenomenonTime.hasEnd) {
      observationApp.phenomenonTime.hasEnd = new Date(observationApp.phenomenonTime.hasEnd);
    }
  }
  return observationApp;
}


export function observationAppToClient(observationApp: ObservationApp): ObservationClient {
  const observationClient: any = cloneDeep(observationApp);
  observationClient.id = observationClient.clientId;
  observationClient.resultTime = observationClient.resultTime.toISOString();
  delete observationClient.clientId;
  delete observationClient.timeseriesId;
  if (observationClient.location) {
    observationClient.location = locationAppToClient(observationClient.location);
  }
  if (observationClient.phenomenonTime) {
    if (observationClient.phenomenonTime.hasBeginning) {
      observationClient.phenomenonTime.hasBeginning = observationClient.phenomenonTime.hasBeginning.toISOString();
    }
    if (observationClient.phenomenonTime.hasEnd) {
      observationClient.phenomenonTime.hasEnd = observationClient.phenomenonTime.hasEnd.toISOString();
    }
  }
  return observationClient;
}