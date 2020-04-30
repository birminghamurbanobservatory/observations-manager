import * as timeseriesService from './timeseries.service';
import hasher from '../../utils/hasher';
import * as logger from 'node-logger';
import * as joi from '@hapi/joi';
import {TimeseriesClient} from './timeseries-client.class';
import {BadRequest} from '../../errors/BadRequest';



//-------------------------------------------------
// Get Single Timeseries
//-------------------------------------------------
export async function getSingleTimeseries(clientId: string): Promise<TimeseriesClient> {

  // Decode the hashed id.
  const databaseId = Number(hasher.decode(clientId));
  logger.debug(`Getting timeseries with database id '${databaseId}' (clientId: ${clientId})`);

  const timeseries = await timeseriesService.getTimeseries(databaseId);
  logger.debug('timeseries found', timeseries);
  return timeseriesService.timeseriesAppToClient(timeseries);

}



//-------------------------------------------------
// Get Multiple Timeseries
//-------------------------------------------------
const getMultipleTimeseriesWhereSchema = joi.object({
  id: joi.object({
    in: joi.array().items(joi.string()).min(1).required()
  }),
  firstObs: joi.object({
    lt: joi.string().isoDate(),
    lte: joi.string().isoDate(),
    gt: joi.string().isoDate(),
    gte: joi.string().isoDate()
  })
  .without('lt', 'lte')
  .without('gt', 'gte'),
  lastObs: joi.object({
    lt: joi.string().isoDate(),
    lte: joi.string().isoDate(),
    gt: joi.string().isoDate(),
    gte: joi.string().isoDate()
  })
  .without('lt', 'lte')
  .without('gt', 'gte'),
  madeBySensor: joi.alternatives().try(
    joi.string(),
    joi.object({
      in: joi.array().items(joi.string()).min(1).required()
    })
  ),
  hasDeployment: joi.alternatives().try(
    joi.string(), // find obs that belong to this deployment (may belong to more)
    joi.object({
      in: joi.array().items(joi.string()).min(1), // obs that belong to any of these deployments
      exists: joi.boolean()
    }).min(1)
  ),
  // For exact matches
  hostedByPath: joi.alternatives().try(
    joi.array().items(joi.string()).min(1),
    joi.object({
      in: joi.array().min(1).items(
        joi.array().items(joi.string()).min(1)
      ),
      exists: joi.boolean()
    }).min(1)
  ),
  // For when the platformId can occur anywhere in the path
  isHostedBy: joi.alternatives().try(
    joi.string(),
    joi.object({
      in: joi.array().items(joi.string()).min(1).required()
    })
  ),
  // For lquery strings, e.g. 'building-1.room-1.*' or {in: ['building-1.*', 'building-2.*']}
  hostedByPathSpecial: joi.alternatives().try(
    joi.string(),
    joi.object({
      in: joi.array().items(joi.string()).min(1).required()
    })
  ),  
  observedProperty: joi.alternatives().try(
    joi.string(),
    joi.object({
      in: joi.array().items(joi.string()).min(1),
      exists: joi.boolean()
    }).min(1)
  ),
  unit: joi.alternatives().try(
    joi.string(),
    joi.object({
      in: joi.array().items(joi.string()).min(1),
      exists: joi.boolean()
    }).min(1)
  ),
  hasFeatureOfInterest: joi.alternatives().try(
    joi.string(),
    joi.object({
      in: joi.array().items(joi.string()).min(1),
      exists: joi.boolean()
    }).min(1)
  ),
  discipline: joi.alternatives().try(
    joi.string(),
    joi.object({
      in: joi.array().items(joi.string()).min(1)
    }).min(1)
  ),
  disciplines: joi.alternatives().try( // for an exact match of the whole array
    joi.array().items(joi.string()).min(1),
    joi.object({
      exists: joi.boolean()
    }).min(1)
  ),
  usedProcedure: joi.alternatives().try(
    joi.string(),
    joi.object({
      in: joi.array().items(joi.string()).min(1)
    }).min(1)
  ),
  usedProcedures: joi.alternatives().try( // for an exact match of the whole array
    joi.array().items(joi.string()).min(1),
    joi.object({
      exists: joi.boolean()
    }).min(1)
  )
})
.required();
// Decided not to have a minimum number of keys here, i.e. so that superusers or other microservices can get timeseries without limitations. The limitation will come from a pagination approach, whereby only so many timeseries can be returned per request.

const getMultipleTimeseriesOptionsSchema = joi.object({
  limit: joi.number()
    .integer()
    .positive()
    .max(500)
    .default(100),
  offset: joi.number()
    .integer()
    .positive()
    .default(0),
  sortBy: joi.string()
    .valid('id') // TODO: Given that the id is hashed, this is a little pointless. Improve.
    .default('id'),
  sortOrder: joi.string()
    .valid('asc', 'desc')
    .default('asc')
})
.required();

export async function getMultipleTimeseries(where = {}, options = {}): Promise<{data: TimeseriesClient[]; meta: any}> {

  const {error: whereErr, value: whereValidated} = getMultipleTimeseriesWhereSchema.validate(where);
  if (whereErr) throw new BadRequest(whereErr.message);
  const {error: optionsErr, value: optionsValidated} = getMultipleTimeseriesOptionsSchema.validate(options);
  if (whereErr) throw new BadRequest(optionsErr.message);

  if (whereValidated.id && whereValidated.id.in) {
    whereValidated.id.in = whereValidated.id.in.map((clientId) => Number(hasher.decode(clientId)));
  }

  const timeseries = await timeseriesService.findTimeseries(whereValidated, optionsValidated);

  const timeseriesForClient = timeseries.map(timeseriesService.timeseriesAppToClient);
  logger.debug(`Got ${timeseriesForClient.length} timeseries.`);
  return {
    data: timeseriesForClient,
    // TODO: At some point you may want to calculate and return the total number of timeseries available, e.g. for pagination, this information will go in this meta object. I just need to make sure I can calculate this efficiently.
    meta: {
      count: timeseriesForClient.length
    } 
  };

}
