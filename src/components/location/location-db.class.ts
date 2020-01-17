import {Geometry} from './geometry.class';

export class LocationDb {
  public id?: number;
  public client_id?: string;
  public geo?: Geometry; // saved as a geography type
  public geojson?: Geometry; // save as a jsonb type
}