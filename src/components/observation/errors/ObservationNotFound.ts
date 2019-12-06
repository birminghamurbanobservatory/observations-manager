import {NotFound} from '../../../errors/NotFound';

export class ObservationNotFound extends NotFound {

  public constructor(message = 'Observation could not be found') {
    super(message); // 'Error' breaks prototype chain here
    Object.setPrototypeOf(this, new.target.prototype); // restore prototype chain   
  }

}