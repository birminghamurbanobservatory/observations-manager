import {LocationApp} from '../location/location-app.class';

export class ObservationApp {
  public id?: number;
  public clientId?: string;
  public timeseriesId?: number;
  public madeBySensor?: string;
  public hasResult?: Result;
  public resultTime?: string | Date;
  public inDeployments?: string[];
  public hostedByPath?: string[];
  public hasFeatureOfInterest?: string;
  public observedProperty?: string;
  public usedProcedures?: string[];
  public location?: LocationApp;
}

class Result {
  public value: any;
  public flags?: string[];
}