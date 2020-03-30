export class ObservationsWhere {
  public resultTime?: ResultTime;
  public madeBySensor?: any; // i'd like to write this as "string | InObject" but I end up with issues.
  public inDeployment?: any;
  public inDeployments?: any;
  public hostedByPath?: any; // for exact matches
  public isHostedBy?: any; // for when the platform id can occur anywhere in the path
  public hostedByPathSpecial?: any; // allows lquery syntax
  public hasFeatureOfInterest?: any;
  public observedProperty?: any;
  public unit: any;
  public discipline?: any;
  public disciplines?: any;
  public usedProcedure?: any;
  public usedProcedures?: any;
  public flags?: any;
  // TODO: geospatial filters
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