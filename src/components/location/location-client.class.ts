import {Geometry} from './geometry.class';

export class LocationClient {
  public id?: string; // this is actually the client_id
  public geometry: Geometry
  public validAt?: string;
}
