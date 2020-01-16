
export function stripNullProperties(obj: any): any {

  const newObj = {};

  Object.keys(obj).forEach((key): any => {
    if (
      obj[key] && 
      typeof obj[key] === 'object' &&
      !(obj[key] instanceof Date) &&
      !(Array.isArray(obj[key]))
    ) {
      newObj[key] = removeEmpty(obj[key]); // recurse
    } else if (obj[key] !== null) {
      newObj[key] = obj[key]; // copy value
    }
  });

  return newObj;  

}

function removeEmpty(obj: any): any {
  return Object.keys(obj).forEach((key): any => {
    obj[key] === null && delete obj[key];
  });
}