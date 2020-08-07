import {Conflict} from '../../../errors/Conflict';

export class TimeseriesAlreadyExists extends Conflict {

  public constructor(message = 'Timeseries already exists') {
    super(message); // 'Error' breaks prototype chain here
    Object.setPrototypeOf(this, new.target.prototype); // restore prototype chain   
  }

}