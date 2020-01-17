import {NotFound} from '../../../errors/NotFound';

export class LocationNotFound extends NotFound {

  public constructor(message = 'Location could not be found') {
    super(message); // 'Error' breaks prototype chain here
    Object.setPrototypeOf(this, new.target.prototype); // restore prototype chain   
  }

}