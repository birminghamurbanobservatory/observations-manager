import camelCaseKeys from 'camelcase-keys';
import snakeCaseKeys from 'snakecase-keys';

export function convertKeysToCamelCase(inputObject: any): any {
  return camelCaseKeys(inputObject);
}


export function convertKeysToSnakeCase(inputObject: any): any {
  return snakeCaseKeys(inputObject);
}