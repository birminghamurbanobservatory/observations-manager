import {Conflict} from '../../../errors/Conflict';

export class ObservationAlreadyExists extends Conflict {

  public constructor(message = 'Observation already exists') {
    super(message); // 'Error' breaks prototype chain here
    Object.setPrototypeOf(this, new.target.prototype); // restore prototype chain   
  }

}