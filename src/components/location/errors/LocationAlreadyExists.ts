import {Conflict} from '../../../errors/Conflict';

export class LocationAlreadyExists extends Conflict {

  public constructor(message = 'Location already exists') {
    super(message); // 'Error' breaks prototype chain here
    Object.setPrototypeOf(this, new.target.prototype); // restore prototype chain   
  }

}