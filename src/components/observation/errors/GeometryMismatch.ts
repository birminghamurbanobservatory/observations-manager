import {BadRequest} from '../../../errors/BadRequest';

export class GeometryMismatch extends BadRequest {

  public constructor(message = 'The geometry of the new observation does not match the geometry refrenced by the location id provided.') {
    super(message); // 'Error' breaks prototype chain here
    Object.setPrototypeOf(this, new.target.prototype); // restore prototype chain
  }

}