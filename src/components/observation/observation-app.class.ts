import {LocationApp} from '../location/location-app.class';

export class ObservationApp {
  public id?: string;
  public madeBySensor?: string;
  public hasResult?: Result;
  public resultTime?: string | Date;
  public inDeployments?: string[];
  public hostedByPath?: string[];
  public hasFeatureOfInterest?: string;
  public observedProperty?: string;
  public usedProcedures?: string[];
  public Location?: LocationApp;
}

class Result {
  public value: any;
  public flags?: string[];
}