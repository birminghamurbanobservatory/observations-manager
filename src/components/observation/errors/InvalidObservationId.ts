import {BadRequest} from '../../../errors/BadRequest';

export class InvalidObservationId extends BadRequest {

  public constructor(message = 'Invalid observation id') {
    super(message); // 'Error' breaks prototype chain here
    Object.setPrototypeOf(this, new.target.prototype); // restore prototype chain
  }

}