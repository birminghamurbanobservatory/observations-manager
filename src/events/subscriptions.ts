import {subscribeToObservationEvents} from '../components/observation/observation.events';


export async function invokeAllSubscriptions(): Promise<void> {

  await subscribeToObservationEvents();
  
}


