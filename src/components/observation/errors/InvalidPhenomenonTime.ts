import {BadRequest} from '../../../errors/BadRequest';

export class InvalidPhenomenonTime extends BadRequest {

  public constructor(message = 'Invalid phenomenon time object') {
    super(message); // 'Error' breaks prototype chain here
    Object.setPrototypeOf(this, new.target.prototype); // restore prototype chain
  }

}