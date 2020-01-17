import * as geojsonhint from '@mapbox/geojsonhint';
import * as check from 'check-types';
import {InvalidGeometry} from './InvalidGeometry';


export function validateGeometry (geometry: any): void {

  if (check.not.nonEmptyObject(geometry)) {
    throw new InvalidGeometry('Must be a non-empty object');
  }

  if (check.assigned(geometry.geometry)) {
    throw new InvalidGeometry(`Object must not contain a key called 'geometry'`);
  }

  if (check.assigned(geometry.properties)) {
    throw new InvalidGeometry(`Object must not contain a key called 'properties'`);
  }  

  const result = geojsonhint.hint(geometry);

  // Passed
  if (result.length === 0) {
    return;

  // Failed
  } else {
    // There may be more than one error, but let's just return the first.
    throw new InvalidGeometry(result[0].message);

  }

}


