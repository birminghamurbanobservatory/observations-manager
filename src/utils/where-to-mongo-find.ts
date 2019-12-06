import * as check from 'check-types';

export function whereToMongoFind(where: any): any {

  if (check.not.object(where)) {
    throw new Error('where argument must be an object');
  }

  const find = {};

  const mappings = {
    exists: '$exists',
    gte: '$gte',
    gt: '$gt',
    lte: '$lte',
    lt: '$lt'
  };

  Object.keys(where).forEach((propKey) => {

    if (check.object(where[propKey])) {

      const propObj = where[propKey];
      Object.keys(propObj).forEach((opKey) => {

        if (check.assigned(mappings[opKey])) {
          const findKey = mappings[opKey];
          if (check.not.assigned(find[propKey])) {
            find[propKey] = {};
          } 
          find[propKey][findKey] = propObj[opKey];
        } else {
          throw new Error(`Unknown key for where object called: '${opKey}'`);
        }

      });

    } else {
      find[propKey] = where[propKey];
    }

  });

  return find;

}