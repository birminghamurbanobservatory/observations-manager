import {ObservationClient} from './observation-client.class';
import {addResult, extractTimeseriesPropsFromObservation, extractResultFromObservation, buildObservation, observationClientToApp, observationAppToClient} from './observation.service';
import {upsertTimeseries} from '../timeseries/timeseries.service';
import * as logger from 'node-logger';


export async function createObservation(observation: ObservationClient): Promise<ObservationClient> {

  logger.debug('Creating observation');
  const obs = observationClientToApp(observation);

  const props = extractTimeseriesPropsFromObservation(obs);
  const result = extractResultFromObservation(obs);

  const timeseries = await upsertTimeseries(props, result.resultTime);
  logger.debug(`Corresponding timeseries (id: ${timeseries.id}) upserted.`);

  await addResult(result, timeseries.id);
  logger.debug('Observation has been added to database');

  const createdObservation = buildObservation(result, timeseries);
  logger.debug('Complete observation', createdObservation);

  return observationAppToClient(createdObservation);

}