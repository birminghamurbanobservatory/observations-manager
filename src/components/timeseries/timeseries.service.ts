import {TimeseriesProps} from './timeseries-props.class';
import Timeseries from './timeseries.model';
import {UpsertTimeseriesFail} from './errors/UpsertTimeseriesFail';
import * as check from 'check-types';
import {TimeseriesApp} from './timeseries-app.class';
import {sortBy} from 'lodash';
import {TimeseriesNotFound} from './errors/TimeseriesNotFound';
import {GetTimeseriesFail} from './errors/GetTimeseriesFail';


export async function upsertTimeseries(props: TimeseriesProps, resultTime: Date): Promise<TimeseriesApp> {

  const findQuery = convertPropsToFindQuery(props);
  // This updates object need to be the full document for sake of an insert rather than just an update.
  const updates = Object.assign({}, props, {
    $min: {startDate: resultTime},
    $max: {endDate: resultTime}
  });

  let upsertedTimeseries;

  try {
    upsertedTimeseries = await Timeseries.findOneAndUpdate(findQuery, updates, {
      upsert: true, 
      new: true
    }).exec();
  } catch (err) {
    throw new UpsertTimeseriesFail(undefined, err.message);  
  }

  return timeseriesDbToApp(upsertedTimeseries);

}


export async function getTimeseries(id: string): Promise<TimeseriesApp> {

  let timeseries;
  try {
    timeseries = await Timeseries.findById(id).exec();
  } catch (err) {
    throw new GetTimeseriesFail(undefined, err.message);
  }

  if (!timeseries || timeseries.deletedAt) {
    throw new TimeseriesNotFound(`A timeseries with id '${id}' could not be found`);
  }

  return timeseriesDbToApp(timeseries);

}


// This is important, for example, for making sure we add {$exists: false} for props that are not provided, and for properly handling properties that are an array.
export function convertPropsToFindQuery(props: TimeseriesProps): any {

  const findQuery: any = {};
  const potentialProps = ['madeBySensor', 'inDeployments', 'hostedByPath', 'observedProperty', 'hasFeatureOfInterest', 'hasFeatureOfInterest', 'usedProcedures'];

  // For the inDeployments array the order has no meaning, and thus we should sort the array just in case the deployments are provided out of order at some point. Having this sort here saves us having to use costly $size and &all operators.
  // https://stackoverflow.com/questions/29774032/mongodb-find-exact-array-match-but-order-doesnt-matter 
  // The order of item in the hostedByPath array indicates the hierarchy and thus we don't want to reorder it. 
  // The order of the procedures implies the order the procedures were applied, and thus we don't want to reorder them as they have some meaning.

  potentialProps.forEach((propKey) => {

    if (check.assigned(props[propKey])) {
      if (propKey === 'inDeployments') {
        findQuery[propKey] = sortBy(props[propKey]);
      } else {
        findQuery[propKey] = props[propKey];
      }
    } else {
      findQuery[propKey] = {$exists: false};
    }

  });

  return findQuery;
}


export function timeseriesDbToApp(timeseriesDb: any): TimeseriesApp {
  const timeseriesApp = timeseriesDb.toObject();
  timeseriesApp.id = timeseriesApp._id.toString();
  delete timeseriesApp._id;
  delete timeseriesApp.__v;
  return timeseriesApp;
}