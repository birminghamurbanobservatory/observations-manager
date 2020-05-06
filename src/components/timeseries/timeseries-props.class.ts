// i.e. the Timeseries Properties that can be pulled out of an incoming observation.
export class TimeseriesProps {
  public madeBySensor?: string;
  public hasDeployment?: string;
  public hostedByPath?: string[];
  public hasFeatureOfInterest?: string;
  public observedProperty?: string;
  public aggregation?: string;
  public discipline?: string[];
  public usedProcedure?: string[];
}

