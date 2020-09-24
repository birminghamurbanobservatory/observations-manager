import {cloneDeep, isEqual, round} from 'lodash';


// This function was created in response to an issue I had where different systems had slightly different numbers of decimal places and thus the comparison was failing even though the readings were virtuall identical. Rather than a strict isEqual comparison this will perform some rounding on the coordinates to avoid such issues.
export function areGeometriesTheSame(geometry1: any, geometry2: any): boolean {

  const geometry1Rounded = cloneDeep(geometry1);
  const geometry2Rounded = cloneDeep(geometry2);
  
  const decimalPlacesThreshold = 7;

  // TODO: Support geometries other than points?
  if (geometry1.type === 'Point') {
    geometry1Rounded.coordinates[0] = round(geometry1Rounded.coordinates[0], decimalPlacesThreshold);
    geometry1Rounded.coordinates[1] = round(geometry1Rounded.coordinates[1], decimalPlacesThreshold);
  }
  if (geometry2.type === 'Point') {
    geometry2Rounded.coordinates[0] = round(geometry2Rounded.coordinates[0], decimalPlacesThreshold);
    geometry2Rounded.coordinates[1] = round(geometry2Rounded.coordinates[1], decimalPlacesThreshold);
  }
  
  return isEqual(geometry1Rounded, geometry2Rounded);

}