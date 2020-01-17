import {LocationClient} from '../location/location-client.class';

export class ObservationClient {
  public id?: string;
  public madeBySensor?: string;
  public hasResult?: Result;
  public resultTime?: string | Date;
  public inDeployments?: string[];
  public hostedByPath?: string[];
  public hasFeatureOfInterest?: string;
  public observedProperty?: string;
  public usedProcedures?: string[];
  public location?: LocationClient
}

class Result {
  value: any;
  flags?: string[];
}