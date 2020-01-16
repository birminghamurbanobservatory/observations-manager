import {convertKeysToCamelCase, convertKeysToSnakeCase} from './class-converters';


//-------------------------------------------------
// Tests
//-------------------------------------------------
describe('Converting keys to camel case', () => {

  test('Converts from snake case', () => {
    const input = {in_deployment: 6};
    const expected = {inDeployment: 6};
    expect(convertKeysToCamelCase(input)).toEqual(expected);
  });

  test('Check it does not mutate the original object', () => {
    const input = {in_deployment: 6};
    const expected = {inDeployment: 6};
    expect(convertKeysToCamelCase(input)).toEqual(expected);
    expect(input).toEqual({in_deployment: 6});
  });  

});


describe('Converting keys to snake case', () => {

  test('Converts from camel case', () => {
    const input = {inDeployment: 6};
    const expected = {in_deployment: 6};
    expect(convertKeysToSnakeCase(input)).toEqual(expected);
  });

  test('Check it does not mutate the original object', () => {
    const input = {inDeployment: 6};
    const expected = {in_deployment: 6};
    expect(convertKeysToSnakeCase(input)).toEqual(expected);
    expect(input).toEqual({inDeployment: 6});
  });    

});