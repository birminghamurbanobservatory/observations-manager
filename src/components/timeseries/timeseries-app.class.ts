export class TimeseriesApp {
  public id?: number;
  public firstObs?: Date;
  public lastObs?: Date;
  public hash?: string;
  public madeBySensor?: string;
  public hasDeployment?: string;
  public hostedByPath?: string[];
  public unit?: string;
  public hasFeatureOfInterest?: string;
  public observedProperty?: string;
  public aggregation?: string;
  public disciplines?: string[];
  public usedProcedures?: string[];
}