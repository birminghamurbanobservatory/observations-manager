import {cloneDeep, isNull} from 'lodash';

// N.b. only a shallow strip, won't check nested properties.
export function stripNullProperties(obj: any): any {

  const newObj = cloneDeep(obj);

  Object.keys(newObj).forEach((key): any => {
    
    if (isNull(newObj[key])) {
      delete newObj[key];
    }

  });

  return newObj;  

}

