import {arrayToLtreeString, ltreeStringToArray, platformIdToAnywhereLquery} from './db-helpers';

describe('arrayToLtreeString function tests', () => {

  test('Converts simple string array correctly', () => {
    const arr = ['a', 'b', 'c'];
    const expected = 'a.b.c';
    const result = arrayToLtreeString(arr);
    expect(result).toBe(expected);
  });

  test('Handle hypens correctly', () => {
    const arr = ['platform-1', 'building', 'platform-number-two'];
    const expected = 'platform_1.building.platform_number_two';
    const result = arrayToLtreeString(arr);
    expect(result).toBe(expected);
  });  

});



describe('ltreeStringToArray function tests', () => {

  test('Converts simple string correctly', () => {
    const str = 'a.b.c';
    const expected = ['a', 'b', 'c'];
    const result = ltreeStringToArray(str);
    expect(result).toEqual(expected);    
  });


  test('Handles underscores correctly', () => {
    const str = 'platform_1.building.platform_number_two';
    const expected = ['platform-1', 'building', 'platform-number-two'];
    const result = ltreeStringToArray(str);
    expect(result).toEqual(expected);    
  });  

});


describe('platformIdToAnywhereLquery function tests', () => {

  test('Converts correctly', () => {
    const platformId = 'platform-number-two';
    const expected = '*.platform_number_two.*';
    const result = platformIdToAnywhereLquery(platformId);
    expect(result).toBe(expected);
  });

});