import {LocationApp} from '../location/location-app.class';

export class ObservationApp {
  public id?: number;
  public clientId?: string;
  public timeseriesId?: number;
  public madeBySensor?: string;
  public hasResult?: Result;
  public phenomenonTime?: PhenomenonTime
  public resultTime?: Date;
  public inDeployments?: string[];
  public hostedByPath?: string[];
  public hasFeatureOfInterest?: string;
  public observedProperty?: string;
  public disciplines?: string[];
  public usedProcedures?: string[];
  public location?: LocationApp;
}

class Result {
  public value: any;
  public unit?: string;
  public flags?: string[];
}

class PhenomenonTime {
  public hasBeginning: Date;
  public hasEnd: Date;
}
