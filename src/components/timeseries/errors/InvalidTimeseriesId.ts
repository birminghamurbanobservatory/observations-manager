import {BadRequest} from '../../../errors/BadRequest';

export class InvalidTimeseriesId extends BadRequest {

  public constructor(message = 'Invalid timeseries id') {
    super(message); // 'Error' breaks prototype chain here
    Object.setPrototypeOf(this, new.target.prototype); // restore prototype chain
  }

}