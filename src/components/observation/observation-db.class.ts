export class ObservationDb {
  public id?: number;
  public timeseries?: number;
  public result_time?: string;
  public has_beginning?: string;
  public has_end?: string;
  public duration?: number;
  public location?: number;
  public value_number?: number;
  public value_boolean?: boolean;
  public value_text?: string;
  public value_json?: any;
  public flags?: string[];
}