import {BadRequest} from '../../../errors/BadRequest';

export class InvalidObservation extends BadRequest {

  public constructor(message = 'Invalid observation') {
    super(message); // 'Error' breaks prototype chain here
    Object.setPrototypeOf(this, new.target.prototype); // restore prototype chain
  }

}