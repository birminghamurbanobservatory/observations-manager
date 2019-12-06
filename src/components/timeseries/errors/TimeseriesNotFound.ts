import {NotFound} from '../../../errors/NotFound';

export class TimeseriesNotFound extends NotFound {

  public constructor(message = 'Timeseries could not be found') {
    super(message); // 'Error' breaks prototype chain here
    Object.setPrototypeOf(this, new.target.prototype); // restore prototype chain   
  }

}