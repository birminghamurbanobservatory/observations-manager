export class ObservationRow {
  public id?: number;
  public timeseries?: string;
  public result_time?: string;
  public value_number?: number;
  public value_boolean?: boolean;
  public value_text?: string;
  public value_json?: any;
  public flags?: string[];
}