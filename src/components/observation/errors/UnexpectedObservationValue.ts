import {BadRequest} from '../../../errors/BadRequest';

export class UnexpectedObservationValue extends BadRequest {

  public constructor(message = 'Unexpected observation value.') {
    super(message); // 'Error' breaks prototype chain here
    Object.setPrototypeOf(this, new.target.prototype); // restore prototype chain
  }

}