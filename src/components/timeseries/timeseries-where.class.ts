// passed in as an argument to the function that finds Timeseries based on certain criteria.
// Either based on the where object that comes in from the event-stream when a client asks for observations.
// Or derived from incoming observations in order to find timeseries that this obs would belong to.
// Because of these two sources you have both usedProcedure, and usedProcedures, for example.
export class TimeseriesWhere {
  public resultTime?: ResultTime;
  public madeBySensor?: any; // i'd like to write this as "string | InObject" but I end up with issues.
  public inDeployment?: any;
  public inDeployments?: any;
  public hostedByPath?: any; // for exact matches
  public isHostedBy?: any; // for when the platform id can occur anywhere in the path
  public hostedByPathSpecial?: any; // allows lquery syntax
  public hasFeatureOfInterest?: any;
  public observedProperty?: any;
  public usedProcedure?: any;
  public usedProcedures?: any;
}

class ResultTime {
  public gt?: Date;
  public gte?: Date;
  public lt?: Date;
  public lte?: Date;
}

class InObject {
  public in: string[]
}