import {LocationClient} from '../location/location-client.class';

export class ObservationClient {
  public id?: string;
  public timeseriesId?: string;
  public madeBySensor?: string;
  public hasResult?: Result;
  public resultTime?: string;
  public phenomenonTime?: PhenomenonTime;
  public hasDeployment?: string;
  public hostedByPath?: string[];
  public hasFeatureOfInterest?: string;
  public observedProperty?: string;
  public aggregation?: string;
  public disciplines?: string[];
  public usedProcedures?: string[];
  public location?: LocationClient
}

class Result {
  value: any;
  unit?: string;
  flags?: string[];
}

class PhenomenonTime {
  hasBeginning: string;
  hasEnd: string;
  duration: number;
}