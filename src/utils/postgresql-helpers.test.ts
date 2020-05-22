import {arrayToPostgresArrayString} from './postgresql-helpers';

describe('Testing of arrayToPostgresArrayString function', () => {

  test('Converts array of strings correctly', () => {
    
    const array = ['meteorology', 'hydrology'];
    const expected = '{meteorology,hydrology}';
    const result = arrayToPostgresArrayString(array);
    expect(result).toBe(expected);

  });


  test('Converts array with a single element', () => {
    
    const array = ['meteorology'];
    const expected = '{meteorology}';
    const result = arrayToPostgresArrayString(array);
    expect(result).toBe(expected);

  });

});