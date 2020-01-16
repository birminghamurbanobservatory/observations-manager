// i.e. the Timeseries Properties that can be pulled out of an incoming observation.
export class TimeseriesProps {
  public madeBySensor?: string;
  public inDeployments?: string[];
  public hostedByPath?: string[];
  public hasFeatureOfInterest?: string;
  public observedProperty?: string;
  public usedProcedures?: string[];
}

