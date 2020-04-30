import {convertKeysToCamelCase, convertKeysToSnakeCase} from './case-converters';


//-------------------------------------------------
// Tests
//-------------------------------------------------
describe('Converting keys to camel case', () => {

  test('Converts from snake case', () => {
    const input = {has_deployment: 6};
    const expected = {hasDeployment: 6};
    expect(convertKeysToCamelCase(input)).toEqual(expected);
  });

  test('Check it does not mutate the original object', () => {
    const input = {has_deployment: 6};
    const expected = {hasDeployment: 6};
    expect(convertKeysToCamelCase(input)).toEqual(expected);
    expect(input).toEqual({has_deployment: 6});
  });  

});


describe('Converting keys to snake case', () => {

  test('Converts from camel case', () => {
    const input = {hasDeployment: 6};
    const expected = {has_deployment: 6};
    expect(convertKeysToSnakeCase(input)).toEqual(expected);
  });

  test('Check it does not mutate the original object', () => {
    const input = {hasDeployment: 6};
    const expected = {has_deployment: 6};
    expect(convertKeysToSnakeCase(input)).toEqual(expected);
    expect(input).toEqual({hasDeployment: 6});
  });    

});