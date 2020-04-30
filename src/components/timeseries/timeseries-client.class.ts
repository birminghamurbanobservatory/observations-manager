export class TimeseriesClient {
  public id?: string;
  public firstObs?: Date;
  public lastObs?: Date;
  public madeBySensor?: string;
  public hasDeployment?: string;
  public hostedByPath?: string[];
  public unit?: string;
  public hasFeatureOfInterest?: string;
  public observedProperty?: string;
  public disciplines?: string[];
  public usedProcedures?: string[];
}