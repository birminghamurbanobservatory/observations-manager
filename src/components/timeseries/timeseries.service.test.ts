import {convertPropsToExactWhere, timeseriesAppToDb, encodeTimeseriesId, decodeTimeseriesId} from './timeseries.service';
import {TimeseriesApp} from './timeseries-app.class';
import {TimeseriesDb} from './timeseries-db.class';
import * as check from 'check-types';
import {InvalidTimeseriesId} from './errors/InvalidTimeseriesId';

describe('Testing of convertPropsToExactWhere function', () => {

  test('Converts a full set of props', () => {
    
    const props = {
      madeBySensor: 'sensor-123',
      hasDeployment: 'deployment-1',
      hostedByPath: ['platform-1'],
      observedProperty: 'air-temperature',
      aggregation: 'instant',
      unit: 'degree-celsius',
      hasFeatureOfInterest: 'earth-atmosphere',
      disciplines: ['meteorology'],
      usedProcedures: ['point-sample']
    };

    const expected = {
      madeBySensor: 'sensor-123',
      hasDeployment: 'deployment-1',
      hostedByPath: ['platform-1'],
      observedProperty: 'air-temperature',
      aggregation: 'instant',
      unit: 'degree-celsius',
      hasFeatureOfInterest: 'earth-atmosphere',
      disciplines: ['meteorology'],
      usedProcedures: ['point-sample']      
    };

    expect(convertPropsToExactWhere(props)).toEqual(expected);

  });

  test('Converts a missing props to exists operators', () => {
    
    const props = {
      madeBySensor: 'sensor-123',
      hasDeployment: 'deployment-1',
      observedProperty: 'air-temperature',
      aggregation: 'instant',
      hasFeatureOfInterest: 'earth-atmosphere'
    };

    const expected = {
      madeBySensor: 'sensor-123',
      hasDeployment: 'deployment-1',
      hostedByPath: {exists: false},
      observedProperty: 'air-temperature',
      aggregation: 'instant',
      unit: {exists: false},
      hasFeatureOfInterest: 'earth-atmosphere',
      disciplines: {exists: false},
      usedProcedures: {exists: false}     
    };

    expect(convertPropsToExactWhere(props)).toEqual(expected);

  });



  test('Re-orderes the appropriate arrays', () => {
    
    const props = {
      madeBySensor: 'sensor-123',
      hasDeployment: 'deployment-a',
      hostedByPath: ['lamppost-12', 'beta-weather-station'],
      observedProperty: 'air-temperature',
      aggregation: 'instant',
      unit: 'degree-celsius',
      hasFeatureOfInterest: 'earth-atmosphere',
      disciplines: ['meteorology', 'climatology'],
      usedProcedures: ['point-sample', 'averaged']     
    };

    const expected = {
      madeBySensor: 'sensor-123',
      hasDeployment: 'deployment-a',
      hostedByPath: ['lamppost-12', 'beta-weather-station'], // don't want this re-ordered, as the order means something
      observedProperty: 'air-temperature',
      aggregation: 'instant',
      unit: 'degree-celsius',
      hasFeatureOfInterest: 'earth-atmosphere',
      disciplines: ['climatology', 'meteorology'], // do want this reordered.
      usedProcedures: ['point-sample', 'averaged']  // don't want this re-ordered, as the order means something
    };

    expect(convertPropsToExactWhere(props)).toEqual(expected);

  });


  describe('timeseriesAppToDb function testing', () => {
  
    test('Converts correctly', () => {
      const timeseriesApp: TimeseriesApp = {
        firstObs: new Date('2020-01-17T10:33:21.620Z'),
        lastObs: new Date('2020-01-17T14:43:21.420Z'),
        madeBySensor: 'sensor-1',
        hasDeployment: 'deployment-1',
        hostedByPath: ['building-1', 'room-no-3', 'device'],
        hasFeatureOfInterest: 'buildings',
        observedProperty: 'air-temperature',
        aggregation: 'instant',
        unit: 'degree-celsius',
        disciplines: ['meteorology'],
        usedProcedures: ['point-sample']        
      };
      const expected: TimeseriesDb = {
        first_obs: new Date('2020-01-17T10:33:21.620Z'),
        last_obs: new Date('2020-01-17T14:43:21.420Z'),
        made_by_sensor: 'sensor-1',
        has_deployment: 'deployment-1',
        hosted_by_path: 'building_1.room_no_3.device',
        has_feature_of_interest: 'buildings',
        observed_property: 'air-temperature',
        aggregation: 'instant',
        unit: 'degree-celsius',
        disciplines: ['meteorology'],
        used_procedures: ['point-sample']
      };
      const timeseriesDb = timeseriesAppToDb(timeseriesApp);
      expect(timeseriesDb).toEqual(expected);
    });
  
  });


});



describe('Testing of timeseries id encoding and decoding function', () => {

  test('Should encode and decode and get the same component values back (with input resultTime as string)', () => {
    const timeseriesId = 543;
    const encoded = encodeTimeseriesId(timeseriesId);
    expect(check.nonEmptyString(encoded)).toBe(true);
    const decoded = decodeTimeseriesId(encoded);
    const expected = 543;
    expect(decoded).toEqual(expected);
  });

  test('Should throw an error if you give it a really random string to try and decode', () => {
    const clientId = 'fhyewgrYEHEYHUUEIDJJE333333';
    expect(() => {
      const result = decodeTimeseriesId(clientId);
    }).toThrowError(InvalidTimeseriesId);
  });  

});