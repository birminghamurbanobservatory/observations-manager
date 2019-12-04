import {OperationalError} from './OperationalError';

export class BadRequest extends OperationalError {

  public constructor(message = 'Bad request') {
    super(message); // 'Error' breaks prototype chain here
    Object.setPrototypeOf(this, new.target.prototype); // restore prototype chain   
    // Add a statusCode, useful when converting an error object to a HTTP response
    this.statusCode = 400;    
  }

}