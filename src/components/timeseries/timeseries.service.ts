import {TimeseriesProps} from './timeseries-props.class';


export async function upsertTimeseries(props: TimeseriesProps, newTime: Date): Promise<TimeseriesApp> {

  // TODO: use $min and $max for updating the start and end dates

  // TODO: Should we sort the inDeployments and usedProcedures alphabetically before saving and query so we don't need to use the costly $size and $all operators?
  // https://stackoverflow.com/questions/29774032/mongodb-find-exact-array-match-but-order-doesnt-matter 
  // Don't do this for hostedByPath for which it's already sorted by hierachy. Is it possible that the order of the usedProcedures could be important, and thus this shouldn't be sorted?

}


// This is important, for example, for making sure we add {$exists: false} for props that are not provided, and for properly handling properties that are an array.
export function convertPropsToFindQuery(props: TimeseriesProps): any {



}