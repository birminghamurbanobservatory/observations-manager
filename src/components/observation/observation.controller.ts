import {ObservationClient} from './observation-client.class';
import {addResult, extractTimeseriesPropsFromObservation, extractResultFromObservation, buildObservation, observerationClientToApp, observerationAppToClient} from './observation.service';
import {upsertTimeseries} from '../timeseries/timeseries.service';


export async function createObservation(observation: ObservationClient): Promise<ObservationClient> {

  const obs = observerationClientToApp(observation);

  const props = extractTimeseriesPropsFromObservation(obs);
  const result = extractResultFromObservation(obs);

  const timeseries = await upsertTimeseries(props, result.resultTime);

  await addResult(result, timeseries.id);

  const createdObservation = buildObservation(result, timeseries);

  return observerationAppToClient(createdObservation);

}