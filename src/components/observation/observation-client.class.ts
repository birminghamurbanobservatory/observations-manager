import {LocationClient} from '../location/location-client.class';

export class ObservationClient {
  public id?: string;
  public madeBySensor?: string;
  public hasResult?: Result;
  public resultTime?: string;
  public phenomenonTime?: PhenomenonTime;
  public inDeployments?: string[];
  public hostedByPath?: string[];
  public hasFeatureOfInterest?: string;
  public observedProperty?: string;
  public discipline?: string[];
  public usedProcedure?: string[];
  public location?: LocationClient
}

class Result {
  value: any;
  flags?: string[];
}

class PhenomenonTime {
  hasBeginning: string;
  hasEnd: string;
}