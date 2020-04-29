import {subscribeToObservationEvents} from '../components/observation/observation.events';
import {subscribeToTimeseriesEvents} from '../components/timeseries/timeseries.events';


export async function invokeAllSubscriptions(): Promise<void> {

  await subscribeToObservationEvents();
  await subscribeToTimeseriesEvents();

}


