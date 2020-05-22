import {validateObservation, checkAndCompleteClientPhenomenonTimeObject} from './observation-validator';
import {InvalidPhenomenonTime} from './errors/InvalidPhenomenonTime';
import * as check from 'check-types';
import {InvalidObservation} from './errors/InvalidObservation';


describe('Testing validateObservation function', () => {

  test('Validates a simple observation', () => {
    
    const observation = {
      madeBySensor: 'sensor-1',
      resultTime: '2020-05-05T12:32:00.124Z',
      hasResult: {
        value: 12.2,
        unit: 'degree-celsius'
      },
      observedProperty: 'air-temperature',
      aggregation: 'instant'
    };

    const validObservation = validateObservation(observation);
    expect(check.nonEmptyObject(validObservation)).toEqual(true);

  });


  test('Allows an observation without a madeBySensor property', () => {
    // E.g. for derived observations that don't have a single specific sensor
    
    const observation = {
      resultTime: '2020-05-05T12:32:00.124Z',
      hasResult: {
        value: 12.2,
        unit: 'degree-celsius'
      },
      observedProperty: 'dew-point-temperature'
    };

    const validObservation = validateObservation(observation);
    expect(check.nonEmptyObject(validObservation)).toEqual(true);

  });


  test('Allows an observation that is a maximum over a time interval', () => {
    // E.g. for derived observations that don't have a single specific sensor
    
    const observation = {
      madeBySensor: 'sensor-1',
      resultTime: '2020-05-05T12:32:00.124Z',
      hasResult: {
        value: 12.2,
        unit: 'degree-celsius'
      },
      observedProperty: 'air-temperature',
      aggregation: 'maximum',
      phenomenonTime: {
        hasBeginning: '2020-04-28T11:30:00.000Z',
        hasEnd: '2020-04-28T11:35:00.000Z',
        duration: 300
      }
    };

    const validObservation = validateObservation(observation);
    expect(check.nonEmptyObject(validObservation)).toEqual(true);

  });


  test('Adds a default aggregation property when not specified', () => {
    
    const observation = {
      madeBySensor: 'sensor-1',
      resultTime: '2020-05-05T12:32:00.124Z',
      hasResult: {
        value: 12.2,
        unit: 'degree-celsius'
      },
      observedProperty: 'air-temperature'
    };

    const validObservation = validateObservation(observation);
    expect(check.nonEmptyObject(validObservation)).toEqual(true);
    expect(validObservation.aggregation).toBe('instant');

  });


  test('Should throw error if a aggregation type is given that requires a phenomenonTime object', () => {
    
    const observation = {
      madeBySensor: 'sensor-1',
      resultTime: '2020-05-05T12:32:00.124Z',
      hasResult: {
        value: 12.2,
        unit: 'degree-celsius'
      },
      aggregation: 'maximum',
      observedProperty: 'air-temperature'
    };

    expect(() => {
      validateObservation(observation)
    }).toThrowError(InvalidObservation);

  });


});





describe('Testing of checkAndCompleteClientPhenomenonTimeObject function', () => {

  test('Will pass a correct phenomenon time object through ok', () => {
    
    const obj = {
      hasBeginning: '2020-04-28T11:30:00.000Z',
      hasEnd: '2020-04-28T11:40:00.000Z',
      duration: 600
    };

    const expected = {
      hasBeginning: '2020-04-28T11:30:00.000Z',
      hasEnd: '2020-04-28T11:40:00.000Z',
      duration: 600
    };

    const validObj = checkAndCompleteClientPhenomenonTimeObject(obj);
    expect(validObj).toEqual(expected);

  });


  test('Calculates the duration correctly', () => {
    
    const obj = {
      hasBeginning: '2020-04-28T11:30:00.000Z',
      hasEnd: '2020-04-28T11:40:00.000Z',
    };

    const expected = {
      hasBeginning: '2020-04-28T11:30:00.000Z',
      hasEnd: '2020-04-28T11:40:00.000Z',
      duration: 600
    };

    const validObj = checkAndCompleteClientPhenomenonTimeObject(obj);
    expect(validObj).toEqual(expected);

  });


  test('Calculates hasBeginning correctly', () => {
    
    const obj = {
      hasEnd: '2020-04-28T11:40:00.000Z',
      duration: 600
    };

    const expected = {
      hasBeginning: '2020-04-28T11:30:00.000Z',
      hasEnd: '2020-04-28T11:40:00.000Z',
      duration: 600
    };

    const validObj = checkAndCompleteClientPhenomenonTimeObject(obj);
    expect(validObj).toEqual(expected);

  });


  test('Calculates hasEnd correctly', () => {
    
    const obj = {
      hasBeginning: '2020-04-28T11:30:00.000Z',
      duration: 600
    };

    const expected = {
      hasBeginning: '2020-04-28T11:30:00.000Z',
      hasEnd: '2020-04-28T11:40:00.000Z',
      duration: 600
    };

    const validObj = checkAndCompleteClientPhenomenonTimeObject(obj);
    expect(validObj).toEqual(expected);

  });


  test('Throws error when the client provided incorrect duration', () => {
    
    const obj = {
      hasBeginning: '2020-04-28T11:30:00.000Z',
      hasEnd: '2020-04-28T11:40:00.000Z',
      duration: 310
    };

    expect(() => {
      checkAndCompleteClientPhenomenonTimeObject(obj);
    }).toThrowError(InvalidPhenomenonTime);

  });


  test('Throws error when insuffient data provided', () => {
    
    const obj = {
      hasBeginning: '2020-04-28T11:30:00.000Z'
    };

    expect(() => {
      checkAndCompleteClientPhenomenonTimeObject(obj);
    }).toThrowError(InvalidPhenomenonTime);

  });


  test('Throws an error when the end is before the beginning', () => {
    
    const obj = {
      hasBeginning: '2020-04-28T11:30:00.000Z',
      hasEnd: '2020-04-28T11:20:00.000Z'
    };

    expect(() => {
      checkAndCompleteClientPhenomenonTimeObject(obj);
    }).toThrowError(InvalidPhenomenonTime);

  });


  test('Throws an error when the end is the same as the beginning', () => {
    
    const obj = {
      hasBeginning: '2020-04-28T11:30:00.000Z',
      hasEnd: '2020-04-28T11:30:00.000Z'
    };

    expect(() => {
      checkAndCompleteClientPhenomenonTimeObject(obj);
    }).toThrowError(InvalidPhenomenonTime);

  });


});
