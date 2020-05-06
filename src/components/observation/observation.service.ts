import {ObservationCore} from './observation-core.class';
import {ObservationApp} from './observation-app.class';
import {pick, cloneDeep, pullAll} from 'lodash';
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
import {convertKeysToCamelCase} from '../../utils/case-converters';
import {locationAppToClient} from '../location/location.service';
import {ltreeStringToArray, platformIdToAnywhereLquery, arrayToLtreeString} from '../../db/db-helpers';
import knexPostgis from 'knex-postgis';
import hasher from '../../utils/hasher';

const st = knexPostgis(knex);

export async function createObservationsTable(): Promise<void> {

  await knex.schema.createTable('observations', (table): void => {

    table.specificType('id', 'BIGSERIAL'); // Don't set this as primary or else create_hypertable won't work.
    table.integer('timeseries', 24).notNullable(); // 24 is the length of a Mongo ObjectID string
    table.timestamp('result_time', {useTz: true}).notNullable();
    table.timestamp('has_beginning', {useTz: true}).notNullable(); // makes queries easier when force to give a value
    table.timestamp('has_end', {useTz: true}).notNullable(); // makes queries easier when force to give a value
    table.specificType('duration', 'numeric').notNullable(); // makes queries easier when force to give a value
    table.bigInteger('location');
    table.specificType('value_number', 'numeric');
    table.boolean('value_boolean');
    table.text('value_text');
    table.jsonb('value_json');
    table.specificType('flags', 'text ARRAY'); // https://www.postgresql.org/docs/9.1/arrays.html

    table.foreign('timeseries')
    .references('id')
    .inTable('timeseries')
    .withKeyName('timeseries_id');

    table.foreign('location')
    .references('id')
    .inTable('locations')
    .withKeyName('location_id');

  });

  // Create the hypertable
  await knex.raw(`SELECT create_hypertable('observations', 'result_time');`);
  // By default this will create an index called observations_result_time_idx, according to the TimescaleDB tech team it's worth keeping this index even though I create another below that'll use far more often. 
  // N.B. if you want a custom primary key, or a unique index, then you must include the result_time.
  // docs: https://docs.timescale.com/latest/using-timescaledb/schema-management#indexing
  await knex.raw('CREATE UNIQUE INDEX timeseries_spatiotemporal_uniq_idx ON observations (timeseries, result_time, COALESCE(location, 0) DESC)');
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
  'observations.duration',
  'observations.value_number',
  'observations.value_boolean',
  'observations.value_text',
  'observations.value_json',
  'observations.flags',
  'timeseries.made_by_sensor',
  'timeseries.has_deployment',
  'timeseries.hosted_by_path',
  'timeseries.has_feature_of_interest',
  'timeseries.observed_property',
  'timeseries.aggregation',
  'timeseries.unit',
  'timeseries.disciplines',
  'timeseries.used_procedures',
  'locations.client_id as location_client_id',
  'locations.geojson as location_geojson',
  'locations.valid_at as location_valid_at'    
];

const columnsToSelectDuringJoinCondensed = pullAll(cloneDeep(columnsToSelectDuringJoin), [
  'timeseries.has_deployment',
  'timeseries.hosted_by_path',
  'timeseries.has_feature_of_interest',
  'timeseries.observed_property',
  'timeseries.aggregation',
  'timeseries.unit',
  'timeseries.disciplines',
  'timeseries.used_procedures',
]);


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


export async function getObservations(where: ObservationsWhere, options: {limit?: number; offset?: number; onePer: string; sortBy: string; sortOrder: string; condense: boolean}): Promise<ObservationApp[]> {

  // If the request is for "onePer" then it ends up being a fundamentally different SQL query, because we need to use a lateral join instead.
  const onePerEnabled = check.assigned(options.onePer);

  let observations;
  try {

    //------------------------
    // "onePer" requests
    //------------------------
    if (onePerEnabled) {

      const mappings = {
        sensor: 'made_by_sensor',
        timeseries: 'id'
      };

      const distinctByColumn = mappings[options.onePer];
      if (!distinctByColumn) {
        throw new Error(`Invalid onePer value. Value is '${options.onePer}'.`);
      }

      const selectedTimeseriesAlias = 'subtimeseries';
      const lateralDataAlias = 'lateraldata';

      const extraClauses = buildExtraOnPerClauses(where);

      const onePerSelectColumns = [
        `${lateralDataAlias}.id`,
        `${lateralDataAlias}.timeseries as timeseries_id`,
        `${lateralDataAlias}.location as location_id`,
        `${lateralDataAlias}.result_time`,
        `${lateralDataAlias}.has_beginning`,
        `${lateralDataAlias}.has_end`,
        `${lateralDataAlias}.duration`,
        `${lateralDataAlias}.value_number`,
        `${lateralDataAlias}.value_boolean`,
        `${lateralDataAlias}.value_text`,
        `${lateralDataAlias}.value_json`,
        `${lateralDataAlias}.flags`,
        `${selectedTimeseriesAlias}.made_by_sensor`,
        `${selectedTimeseriesAlias}.has_deployment`,
        `${selectedTimeseriesAlias}.hosted_by_path`,
        `${selectedTimeseriesAlias}.has_feature_of_interest`,
        `${selectedTimeseriesAlias}.observed_property`,
        `${selectedTimeseriesAlias}.aggregation`,
        `${selectedTimeseriesAlias}.unit`,
        `${selectedTimeseriesAlias}.disciplines`,
        `${selectedTimeseriesAlias}.used_procedures`,
        `${lateralDataAlias}.location_client_id`,
        `${lateralDataAlias}.location_geojson`,
        `${lateralDataAlias}.location_valid_at` 
      ];

      const onePerSelectColumnsCondensed = pullAll(cloneDeep(onePerSelectColumns), [
        `${selectedTimeseriesAlias}.made_by_sensor`,
        `${selectedTimeseriesAlias}.has_deployment`,
        `${selectedTimeseriesAlias}.hosted_by_path`,
        `${selectedTimeseriesAlias}.has_feature_of_interest`,
        `${selectedTimeseriesAlias}.observed_property`,
        `${selectedTimeseriesAlias}.aggregation`,
        `${selectedTimeseriesAlias}.unit`,
        `${selectedTimeseriesAlias}.disciplines`,
        `${selectedTimeseriesAlias}.used_procedures`,
      ]);
      
      observations = await knex
      .select(options.condense ? onePerSelectColumnsCondensed : onePerSelectColumns)
      .distinctOn(`${selectedTimeseriesAlias}.${distinctByColumn}`)
      .from(function() {
        this.select('*').from('timeseries')
        .where((builder) => {
          // These are basically the same as for a normal request, except we can only filter by columns that the timeseries table has.

          // timeseries ids
          if (check.assigned(where.timeseriesId)) {
            if (check.integer(where.timeseriesId)) {
              builder.where('timeseries.id', where.timeseriesId);
            }
            if (check.nonEmptyObject(where.timeseriesId)) {
              if (check.nonEmptyArray(where.timeseriesId.in)) {
                builder.whereIn('timeseries.id', where.timeseriesId.in);
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

          // hasDeployment
          if (check.assigned(where.hasDeployment)) {
            if (check.nonEmptyString(where.hasDeployment)) {
              builder.where('timeseries.has_deployment', where.hasDeployment);
            }
            if (check.nonEmptyObject(where.hasDeployment)) {
              if (check.nonEmptyArray(where.hasDeployment.in)) {
                builder.whereIn('timeseries.has_deployment', where.hasDeployment.in);
              }
              if (check.boolean(where.hasDeployment.exists)) {
                if (where.hasDeployment.exists === true) {
                  builder.whereNotNull('timeseries.has_deployment');
                } 
                if (where.hasDeployment.exists === false) {
                  builder.whereNull('timeseries.has_deployment');
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

          // aggregation
          if (check.assigned(where.aggregation)) {
            if (check.nonEmptyString(where.aggregation)) {
              builder.where('timeseries.aggregation', where.aggregation);
            }
            if (check.nonEmptyObject(where.aggregation)) {
              if (check.nonEmptyArray(where.aggregation.in)) {
                builder.whereIn('timeseries.aggregation', where.aggregation.in);
              }  
            }
          }

          // unit
          if (check.assigned(where.unit)) {
            if (check.nonEmptyString(where.unit)) {
              builder.where('timeseries.unit', where.unit);
            }
            if (check.nonEmptyObject(where.unit)) {
              if (check.nonEmptyArray(where.unit.in)) {
                builder.whereIn('timeseries.unit', where.unit.in);
              }
              if (check.boolean(where.unit.exists)) {
                if (where.unit.exists === true) {
                  builder.whereNotNull('timeseries.unit');
                } 
                if (where.unit.exists === false) {
                  builder.whereNull('timeseries.unit');
                }
              }     
            }
          }

          // discipline
          if (check.assigned(where.discipline)) {
            if (check.nonEmptyString(where.discipline)) {
              // Find any timeseries whose discipline array contains this one discipline (if there are others in the array then it will still match)
              builder.where('timeseries.disciplines', '&&', [where.discipline]);
            }
            if (check.nonEmptyObject(where.discipline)) {
              if (check.nonEmptyArray(where.discipline.in)) {
                // i.e. looking for any overlap
                builder.where('timeseries.disciplines', '&&', where.discipline.in);
              }
              if (check.boolean(where.discipline.exists)) {
                if (where.discipline.exists === true) {
                  builder.whereNotNull('timeseries.disciplines');
                } 
                if (where.discipline.exists === false) {
                  builder.whereNull('timeseries.disciplines');
                }              
              }
            }
          }  

          // disciplines
          if (check.assigned(where.disciplines)) {
            if (check.nonEmptyArray(where.disciplines)) {
              builder.where('timeseries.disciplines', where.disciplines);
            }
            if (check.nonEmptyObject(where.disciplines)) {
              // Don't yet support the 'in' property here, as not sure how to do an IN with any array of arrays.
              if (check.boolean(where.disciplines.exists)) {
                if (where.disciplines.exists === true) {
                  builder.whereNotNull('timeseries.disciplines');
                } 
                if (where.disciplines.exists === false) {
                  builder.whereNull('timeseries.disciplines');
                }              
              }
            }
          }

          // usedProcedure
          if (check.assigned(where.usedProcedure)) {
            if (check.nonEmptyString(where.usedProcedure)) {
              // Find any timeseries whose discipline array contains this one discipline (if there are others in the array then it will still match)
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

        })
        .as(selectedTimeseriesAlias);
      })
      .joinRaw(`
        INNER JOIN LATERAL (
          SELECT *
          FROM observations
          LEFT JOIN (
            SELECT 
              id AS loc_id, 
              client_id AS location_client_id,
              geo AS location_geo,
              geojson AS location_geojson,
              valid_at AS location_valid_at 
            FROM locations
          ) locs
          ON observations.location = locs.loc_id
          WHERE observations.timeseries = ${selectedTimeseriesAlias}.id
          ${extraClauses}
          ORDER BY result_time DESC LIMIT 1
        ) AS ${lateralDataAlias}
        ON TRUE
      `)
      .orderBy(`${selectedTimeseriesAlias}.${distinctByColumn}`, 'asc')
      .limit(options.limit || 100000)
      .offset(options.offset || 0);

    }

    //------------------------
    // Normal requests
    //------------------------
    if (!onePerEnabled) {

      let orderByArray;
      if (options.sortBy === 'timeseries') {
        orderByArray = [
          {column: 'observations.timeseries', order: options.sortOrder || 'asc'},
          {column: 'observations.result_time', order: options.sortOrder || 'asc'}
          // I think having the sort order of the result_time set the same as the sort order of the timeseries column will be the more efficient, as it should be the order that the index is in.
        ];
      } else {
        orderByArray = [
          {column: 'observations.result_time', order: options.sortOrder || 'asc'}
        ];
      } 

      observations = await knex('observations')
      .select(options.condense ? columnsToSelectDuringJoinCondensed : columnsToSelectDuringJoin)
      .leftJoin('timeseries', 'observations.timeseries', 'timeseries.id')
      .leftJoin('locations', 'observations.location', 'locations.id')
      .where((builder) => {

        // resultTime
        if (check.assigned(where.resultTime)) {
          if (check.nonEmptyString(where.resultTime) || check.date(where.resultTime)) {
            // This is in case I allow clients to request observations at an exact resultTime  
            builder.where('observations.result_time', where.resultTime);
          }
          if (check.nonEmptyObject(where.resultTime)) {
            if (check.assigned(where.resultTime.gte)) {
              builder.where('observations.result_time', '>=', where.resultTime.gte);
            }
            if (check.assigned(where.resultTime.gt)) {
              builder.where('observations.result_time', '>', where.resultTime.gt);
            }
            if (check.assigned(where.resultTime.lte)) {
              builder.where('observations.result_time', '<=', where.resultTime.lte);
            }      
            if (check.assigned(where.resultTime.lt)) {
              builder.where('observations.result_time', '<', where.resultTime.lt);
            }      
          }
        }

        // duration
        if (check.assigned(where.duration)) {
          if (check.number(where.duration)) {
            // This is in case I allow clients to request observations at an exact resultTime  
            builder.where('observations.duration', where.duration);
          }
          if (check.nonEmptyObject(where.duration)) {
            if (check.assigned(where.duration.gte)) {
              builder.where('observations.duration', '>=', where.duration.gte);
            }
            if (check.assigned(where.duration.gt)) {
              builder.where('observations.duration', '>', where.duration.gt);
            }
            if (check.assigned(where.duration.lte)) {
              builder.where('observations.duration', '<=', where.duration.lte);
            }      
            if (check.assigned(where.duration.lt)) {
              builder.where('observations.duration', '<', where.duration.lt);
            }  
          }
        }

        // timeseries ids
        if (check.assigned(where.timeseriesId)) {
          if (check.integer(where.timeseriesId)) {
            builder.where('timeseries.id', where.timeseriesId);
          }
          if (check.nonEmptyObject(where.timeseriesId)) {
            if (check.nonEmptyArray(where.timeseriesId.in)) {
              builder.whereIn('timeseries.id', where.timeseriesId.in);
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

        // hasDeployment
        if (check.assigned(where.hasDeployment)) {
          if (check.nonEmptyString(where.hasDeployment)) {
            builder.where('timeseries.has_deployment', where.hasDeployment);
          }
          if (check.nonEmptyObject(where.hasDeployment)) {
            if (check.nonEmptyArray(where.hasDeployment.in)) {
              builder.whereIn('timeseries.has_deployment', where.hasDeployment.in);
            }
            if (check.boolean(where.hasDeployment.exists)) {
              if (where.hasDeployment.exists === true) {
                builder.whereNotNull('timeseries.has_deployment');
              } 
              if (where.hasDeployment.exists === false) {
                builder.whereNull('timeseries.has_deployment');
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

        // aggregation
        if (check.assigned(where.aggregation)) {
          if (check.nonEmptyString(where.aggregation)) {
            builder.where('timeseries.aggregation', where.aggregation);
          }
          if (check.nonEmptyObject(where.aggregation)) {
            if (check.nonEmptyArray(where.aggregation.in)) {
              builder.whereIn('timeseries.aggregation', where.aggregation.in);
            }  
          }
        }

        // unit
        if (check.assigned(where.unit)) {
          if (check.nonEmptyString(where.unit)) {
            builder.where('timeseries.unit', where.unit);
          }
          if (check.nonEmptyObject(where.unit)) {
            if (check.nonEmptyArray(where.unit.in)) {
              builder.whereIn('timeseries.unit', where.unit.in);
            }
            if (check.boolean(where.unit.exists)) {
              if (where.unit.exists === true) {
                builder.whereNotNull('timeseries.unit');
              } 
              if (where.unit.exists === false) {
                builder.whereNull('timeseries.unit');
              }
            }     
          }
        }

        // discipline
        if (check.assigned(where.discipline)) {
          if (check.nonEmptyString(where.discipline)) {
            // Find any timeseries whose discipline array contains this one discipline (if there are others in the array then it will still match)
            builder.where('timeseries.disciplines', '&&', [where.discipline]);
          }
          if (check.nonEmptyObject(where.discipline)) {
            if (check.nonEmptyArray(where.discipline.in)) {
              // i.e. looking for any overlap
              builder.where('timeseries.disciplines', '&&', where.discipline.in);
            }
            if (check.boolean(where.discipline.exists)) {
              if (where.discipline.exists === true) {
                builder.whereNotNull('timeseries.disciplines');
              } 
              if (where.discipline.exists === false) {
                builder.whereNull('timeseries.disciplines');
              }              
            }
          }
        }  

        // disciplines
        if (check.assigned(where.disciplines)) {
          if (check.nonEmptyArray(where.disciplines)) {
            builder.where('timeseries.disciplines', where.disciplines);
          }
          if (check.nonEmptyObject(where.disciplines)) {
            // Don't yet support the 'in' property here, as not sure how to do an IN with any array of arrays.
            if (check.boolean(where.disciplines.exists)) {
              if (where.disciplines.exists === true) {
                builder.whereNotNull('timeseries.disciplines');
              } 
              if (where.disciplines.exists === false) {
                builder.whereNull('timeseries.disciplines');
              }              
            }
          }
        }

        // usedProcedure
        if (check.assigned(where.usedProcedure)) {
          if (check.nonEmptyString(where.usedProcedure)) {
            // Find any timeseries whose discipline array contains this one discipline (if there are others in the array then it will still match)
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

        // Flags
        if (check.assigned(where.flags)) {
          if (check.nonEmptyObject(where.flags)) {
            if (check.boolean(where.flags.exists)) {
              if (where.flags.exists === true) {
                builder.whereNotNull('observations.flags');
              } 
              if (where.flags.exists === false) {
                builder.whereNull('observations.flags');
              } 
            }
          }
        }

        // I could use something like ST_CONTAINS and ST_MakeEnvelope to make a bounding box if i have lat and lon values that represent all 4 sides, but given that I'm currently just dealing with points the approach below is easier to code and hopefully not any slower to query?

        // longitude
        if (check.assigned(where.longitude)) {
          if (check.assigned(where.longitude.gte)) {
            // N.B: If you have anything other than points in your geo column then this won't work, it won't like the ST_Y command, you would need to use ST_CENTROID first.
            builder.where(st.x(st.geometry('locations.geo')), '>=', where.longitude.gte);
          }
          if (check.assigned(where.longitude.gt)) {
            builder.where(st.x(st.geometry('locations.geo')), '>', where.longitude.gt);
          }
          if (check.assigned(where.longitude.lte)) {
            builder.where(st.x(st.geometry('locations.geo')), '<=', where.longitude.lte);
          }      
          if (check.assigned(where.longitude.lt)) {
            builder.where(st.x(st.geometry('locations.geo')), '<', where.longitude.lt);
          }      
        }

        // latitude
        if (check.assigned(where.latitude)) {
          if (check.assigned(where.latitude.gte)) {
            builder.where(st.y(st.geometry('locations.geo')), '>=', where.latitude.gte);
          }
          if (check.assigned(where.latitude.gt)) {
            builder.where(st.y(st.geometry('locations.geo')), '>', where.latitude.gt);
          }
          if (check.assigned(where.latitude.lte)) {
            builder.where(st.y(st.geometry('locations.geo')), '<=', where.latitude.lte);
          }      
          if (check.assigned(where.latitude.lt)) {
            builder.where(st.y(st.geometry('locations.geo')), '<', where.latitude.lt);
          }      
        }

        // height
        if (check.assigned(where.height)) {
          if (check.assigned(where.height.gte)) {
            builder.where(st.z(st.geometry('locations.geo')), '>=', where.height.gte);
          }
          if (check.assigned(where.height.gt)) {
            builder.where(st.z(st.geometry('locations.geo')), '>', where.height.gt);
          }
          if (check.assigned(where.height.lte)) {
            builder.where(st.z(st.geometry('locations.geo')), '<=', where.height.lte);
          }      
          if (check.assigned(where.height.lt)) {
            builder.where(st.z(st.geometry('locations.geo')), '<', where.height.lt);
          }      
        }

        // Proximity
        if (check.object(where.proximity) && check.number(where.proximity.radius) && check.number(where.proximity.centre.latitude) && check.number(where.proximity.centre.longitude)) {
          builder.where(
            st.dwithin(
              'geo', 
              st.geography(st.setSRID(st.makePoint(where.proximity.centre.longitude, where.proximity.centre.latitude), 4326)),
              where.proximity.radius
            )
          );
        }

        // TODO: Allow =, >=, <, etc on value_number.

      })
      .limit(options.limit || 100000)
      .offset(options.offset || 0)
      .orderBy(orderByArray);

    }

  } catch (err) {
    throw new GetObservationsFail(undefined, err.message);
  }

  return observations.map(observationDbToApp);

}


export function buildExtraOnPerClauses(where): string {

  let sql = '';

  // resultTime
  if (check.assigned(where.resultTime)) {
    if (check.nonEmptyString(where.resultTime)) {
      // This is in case I allow clients to request observations at an exact resultTime 
      sql += ` AND observations.result_time = '${where.resultTime}'`;
    }
    if (check.nonEmptyObject(where.resultTime)) {
      if (check.assigned(where.resultTime.gte)) {
        sql += `AND observations.result_time >= '${where.resultTime.gte}'`;
      }
      if (check.assigned(where.resultTime.gt)) {
        sql += `AND observations.result_time > '${where.resultTime.gt}'`;
      }
      if (check.assigned(where.resultTime.lte)) {
        sql += `AND observations.result_time <= '${where.resultTime.lte}'`;
      }      
      if (check.assigned(where.resultTime.lt)) {
        sql += `AND observations.result_time < '${where.resultTime.lt}'`;
      }      

    }
  }

  // duration
  if (check.assigned(where.duration)) {
    if (check.number(where.duration)) {
      sql += ` AND observations.duration = '${where.duration}'`;
    }
    if (check.nonEmptyObject(where.duration)) {
      if (check.assigned(where.duration.gte)) {
        sql += `AND observations.duration >= '${where.duration.gte}'`;
      }
      if (check.assigned(where.duration.gt)) {
        sql += `AND observations.duration > '${where.duration.gt}'`;
      }
      if (check.assigned(where.duration.lte)) {
        sql += `AND observations.duration <= '${where.duration.lte}'`;
      }      
      if (check.assigned(where.duration.lt)) {
        sql += `AND observations.duration < '${where.duration.lt}'`;
      }      

    }
  }

  // flags
  if (check.assigned(where.flags)) {
    if (check.nonEmptyObject(where.flags)) {
      if (check.boolean(where.flags.exists)) {
        if (where.flags.exists === true) {
          sql += ` AND observations.flags IS NOT NULL`;
        }
        if (where.flags.exists === false) {
          sql += ` AND observations.flags IS NULL`;
        }
      }
    }
  }
  // N.B. if you add the ability to filter by specific flags, then you might want do a regex check on the flags to watch for any SQL injection.

  // longitude
  if (check.object(where.longitude)) {
    if (check.nonEmptyObject(where.longitude)) {
      if (check.number(where.longitude.gte)) {
        sql += `AND ST_X(location_geo::geometry) >= '${where.longitude.gte}'`;
      }
      if (check.number(where.longitude.gt)) {
        sql += `AND ST_X(location_geo::geometry) > '${where.longitude.gt}'`;
      }
      if (check.number(where.longitude.lte)) {
        sql += `AND ST_X(location_geo::geometry) <= '${where.longitude.lte}'`;
      }      
      if (check.number(where.longitude.lt)) {
        sql += `AND ST_X(location_geo::geometry) < '${where.longitude.lt}'`;
      }      
    }
  }

  // latitude
  if (check.object(where.latitude)) {
    if (check.nonEmptyObject(where.latitude)) {
      if (check.number(where.latitude.gte)) {
        sql += `AND ST_Y(location_geo::geometry) >= '${where.latitude.gte}'`;
      }
      if (check.number(where.latitude.gt)) {
        sql += `AND ST_Y(location_geo::geometry) > '${where.latitude.gt}'`;
      }
      if (check.number(where.latitude.lte)) {
        sql += `AND ST_Y(location_geo::geometry) <= '${where.latitude.lte}'`;
      }      
      if (check.number(where.latitude.lt)) {
        sql += `AND ST_Y(location_geo::geometry) < '${where.latitude.lt}'`;
      } 
    }
  }

  // height
  // N.B. if a row doesn't have a Z coordinate then the row will not be returned upon making these height queries.
  if (check.object(where.height)) {
    if (check.nonEmptyObject(where.height)) {
      if (check.number(where.height.gte)) {
        sql += `AND ST_Z(location_geo::geometry) >= '${where.height.gte}'`;
      }
      if (check.number(where.height.gt)) {
        sql += `AND ST_Z(location_geo::geometry) > '${where.height.gt}'`;
      }
      if (check.number(where.height.lte)) {
        sql += `AND ST_Z(location_geo::geometry) <= '${where.height.lte}'`;
      }      
      if (check.number(where.height.lt)) {
        sql += `AND ST_Z(location_geo::geometry) < '${where.height.lt}'`;
      }      
    }
  }

  // proximity
  // important to have plenty of type checking here to prevent SQL injection
  if (check.object(where.proximity) && check.number(where.proximity.radius) && check.number(where.proximity.centre.latitude) && check.number(where.proximity.centre.longitude)) {
    sql += `AND ST_DWithin(location_geo, ST_SetSRID(ST_MakePoint(${where.proximity.centre.longitude}, ${where.proximity.centre.latitude}), 4326)::geography, ${where.proximity.radius})`;
  }


  return sql;
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
    if (observation.phenomenonTime.duration) {
      obsCore.duration = observation.phenomenonTime.duration;
    }
  }
  return obsCore;
}



export function extractTimeseriesPropsFromObservation(observation: ObservationApp): TimeseriesProps {

  const props: any = pick(observation, [
    'madeBySensor',
    'hasDeployment',
    'hostedByPath',
    'observedProperty',
    'aggregation',
    'hasFeatureOfInterest',
    'disciplines',
    'usedProcedures'
  ]);

  // unit is a special case because it's inside the hasResult object
  const unit = observation.hasResult.unit;
  if (unit) {
    props.unit = unit;
  }

  return props;
}



// N.B: This approach won't work if there's ever a situation when you get more that one observation from a given timeseries at the same resultTime and location. The most likely reason you'd have two observations at the same time is if you apply a procedure that manipulated the data in some way, however this would change the usedProcedure array, and therefore the timeseriesId, so this particular example doesn't pose any issues to using this approach.
// N.B. The location id is included, because perhaps you have a sensor that can make simulataneous observations as several locations, if you don't incorporate the location then it will only allow you to save a single observation as they'll all have the same resultTime.
// N.B. we default to the locationId to 0 if no locationId is given, e.g. the obs didn't have a specific location.
export function generateObservationId(timeseriesId: number, resultTime: string | Date, locationId?): string {
  const resultTimeInMilliseconds = new Date(resultTime).getTime();
  const id = hasher.encode(resultTimeInMilliseconds, timeseriesId, locationId || 0);
  return id;
}



export function deconstructObservationId(id: string): {timeseriesId: number; resultTime: Date; locationId?: number} {
  const [resultTimeInMilliseconds, timeseriesId, locationId] = hasher.decode(id);

  const components: any = {
    resultTime: new Date(Number(resultTimeInMilliseconds)),
    timeseriesId,
  };

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
  if (observationDb.duration) {
    obsCore.duration = observationDb.duration;
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

  // For the sake of making queries easier, if hasBeginning, hasEnd or duration have not been set then we'll set defaults for them. E.g. it's far easier to set duration as 0 and have the gte,lt,... queries work nicely than try to add the correctly bracketed OR sql query using knex.
  observationDb.has_beginning = obsCore.hasBeginning ? obsCore.hasBeginning.toISOString() : observationDb.result_time;
  observationDb.has_end = obsCore.hasEnd ? obsCore.hasEnd.toISOString() : observationDb.result_time;
  observationDb.duration = check.assigned(obsCore.duration) ? obsCore.duration : 0;

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

  observationApp.phenomenonTime = {};
  if (observationApp.hasBeginning) {
    observationApp.phenomenonTime.hasBeginning = new Date(observationApp.hasBeginning);
    delete observationApp.hasBeginning;
  }
  if (observationApp.hasEnd) {
    observationApp.phenomenonTime.hasEnd = new Date(observationApp.hasEnd);
    delete observationApp.hasEnd;
  }
  if (check.assigned(observationApp.duration)) {
    observationApp.phenomenonTime.duration = observationApp.duration;
    delete observationApp.duration;
  }
  
  const includePhenomenonTimeObject = (check.assigned(observationDb.duration) && observationDb.duration > 0) ||
    (observationDb.has_beginning && observationDb.has_beginning !== observationDb.result_time) ||
    (observationDb.has_end && observationDb.has_end !== observationDb.result_time);
  
  if (!includePhenomenonTimeObject) {
    delete observationApp.phenomenonTime;
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

  if (observationApp.unit) {
    observationApp.hasResult.unit = observationApp.unit;
  }
  delete observationApp.unit;

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
  // Let's hash the timeseriesId so the user can't workout how many timeseries there are in the database
  if (observationClient.timeseriesId) {
    observationClient.timeseriesId = hasher.encode(observationClient.timeseriesId);
  }
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

