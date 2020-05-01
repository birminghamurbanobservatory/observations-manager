import {arrayToPostgresArrayString} from './postgresql-helpers';

describe('Testing of arrayToPostgresArrayString function', () => {

  test('Converts array of strings correctly', () => {
    
    const array = ['Meteorology', 'Hydrology'];
    const expected = '{Meteorology,Hydrology}';
    const result = arrayToPostgresArrayString(array);
    expect(result).toBe(expected);

  });


  test('Converts array with a single element', () => {
    
    const array = ['Meteorology'];
    const expected = '{Meteorology}';
    const result = arrayToPostgresArrayString(array);
    expect(result).toBe(expected);

  });

});