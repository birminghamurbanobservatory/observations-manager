import {DatabaseError} from '../../../errors/DatebaseError';

export class UpsertTimeseriesFail extends DatabaseError {

  public privateMessage: string;

  public constructor(message = 'Failed to upsert timeseries', privateMessage?: string) {
    super(message); // 'Error' breaks prototype chain here
    Object.setPrototypeOf(this, new.target.prototype); // restore prototype chain
    // Add a private message, which can for logged for extra detail, but should not be sent to the client.
    this.privateMessage = privateMessage;    
  }

}