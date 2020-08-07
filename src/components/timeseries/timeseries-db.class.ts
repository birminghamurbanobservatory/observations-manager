export class TimeseriesDb {
  public id?: number;
  public first_obs?: any;
  public last_obs?: any;
  public hash?: string;
  public made_by_sensor?: string;
  public has_deployment?: string;
  public hosted_by_path?: string;
  // TODO: Need to have isHostedBy here too? For the sake of observation locations updates?
  public observed_property?: string;
  public aggregation?: string;
  public unit?: string;
  public has_feature_of_interest?: string;
  public disciplines?: string[];
  public used_procedures?: string[];
  public total?: number; // used when counting the total number of timeseries
}