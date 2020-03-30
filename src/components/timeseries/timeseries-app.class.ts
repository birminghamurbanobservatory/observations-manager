export class TimeseriesApp {
  public id?: number;
  public firstObs?: Date;
  public lastObs?: Date;
  public madeBySensor?: string;
  public inDeployments?: string[];
  public hostedByPath?: string[];
  public unit?: string;
  public hasFeatureOfInterest?: string;
  public observedProperty?: string;
  public discipline?: string[];
  public usedProcedure?: string[];
}