export class TimeseriesRow {
  public id?: number;
  public first_obs?: any;
  public last_obs?: any;
  public made_by_sensor: string;
  public in_deployments: string[];
  public hosted_by_path: string[];
  // TODO: Need to have isHostedBy here too? For the sake of observation locations updates?
  public has_feature_of_interest: string;
  public observed_property: string;
  public used_procedures: string[];
}