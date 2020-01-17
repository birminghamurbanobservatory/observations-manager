import {extractTimeseriesPropsFromObservation, buildObservation, generateObservationId, deconstructObservationId} from './observation.service';


describe('Testing of extractTimeseriesPropsFromObservation function', () => {

  test('Should correctly extract the timeseries props from an observation', () => {
    const observation = {
      madeBySensor: 'sensor-123',
      hasResult: {
        value: '12'
      },
      resultTime: '2019-12-04T17:26:23.205Z',
      hasFeatureOfInterest: 'weather',
      observedProperty: 'air-temp',
    };
    const expected = {
      madeBySensor: 'sensor-123',
      hasFeatureOfInterest: 'weather',
      observedProperty: 'air-temp'     
    };
    expect(extractTimeseriesPropsFromObservation(observation)).toEqual(expected);
  });

});


describe('Testing of extractTimeseriesPropsFromObservation function', () => {

  test('Should correctly extract the timeseries props from a already full observation', () => {
    const observation = {
      madeBySensor: 'sensor-123',
      hasResult: {
        value: '12'
      },
      resultTime: '2019-12-04T17:26:23.205Z',
      inDeployments: ['deployment-1'],
      hostedByPath: ['platform-1'],
      hasFeatureOfInterest: 'weather',
      observedProperty: 'air-temp', 
      usedProcedures: ['point-sample']
    };
    const expected = {
      madeBySensor: 'sensor-123',
      inDeployments: ['deployment-1'],
      hostedByPath: ['platform-1'],
      hasFeatureOfInterest: 'weather',
      observedProperty: 'air-temp', 
      usedProcedures: ['point-sample']      
    };
    expect(extractTimeseriesPropsFromObservation(observation)).toEqual(expected);
  });

});


describe('Testing of buildObservation function', () => {

  test('Builds observation as expected', () => {
    
    const result = {
      value: 22.3,
      resultTime: new Date('2019-12-04T17:26:23.205Z'),
      flags: ['persistence']
    };

    const timeseries = {
      id: 5432,
      startDate: new Date('2019-12-04T13:22:23.133Z'),
      endDate: new Date('2019-12-04T17:26:23.205Z'),
      madeBySensor: 'sensor-123',
      inDeployments: ['deployment-1'],
      hostedByPath: ['platform-1'],
      hasFeatureOfInterest: 'weather',
      observedProperty: 'air-temp', 
      usedProcedures: ['point-sample']          
    };

    const expected = {
      id: '5432-0-2019-12-04T17:26:23.205Z',
      madeBySensor: 'sensor-123',
      inDeployments: ['deployment-1'],
      hostedByPath: ['platform-1'],
      hasFeatureOfInterest: 'weather',
      observedProperty: 'air-temp', 
      usedProcedures: ['point-sample'],
      resultTime: new Date('2019-12-04T17:26:23.205Z'),        
      hasResult: {
        value: 22.3,
        flags: ['persistence']
      } 
    };

    expect(buildObservation(result, timeseries)).toEqual(expected);

  });

});


describe('Testing of generateObservationId function', () => {

  test('Should correctly generate observation id (with resultTime as string)', () => {
    const timeseriesId = 543;
    const resultTime = '2019-12-06T14:57:18.838Z';
    const expected = '543-0-2019-12-06T14:57:18.838Z';
    expect(generateObservationId(timeseriesId, resultTime)).toBe(expected);
  });

  test('Should correctly generate observation id (with resultTime as date)', () => {
    const timeseriesId = 543;
    const resultTime = new Date('2019-12-06T14:57:18.838Z');
    const expected = '543-0-2019-12-06T14:57:18.838Z';
    expect(generateObservationId(timeseriesId, resultTime)).toBe(expected);
  });  

  test('Should correctly generate observation id when a location is included', () => {
    const timeseriesId = 543;
    const locationId = 23;
    const resultTime = '2019-12-06T14:57:18.838Z';
    const expected = '543-23-2019-12-06T14:57:18.838Z';
    expect(generateObservationId(timeseriesId, resultTime, locationId)).toBe(expected);
  });

});


describe('Testing of deconstructObservationId function', () => {

  test('Should correctly deconstruct when location was available', () => {
    const id = '25-54-2019-12-06T14:57:18.838Z';
    const expected = {
      timeseriesId: 25,
      resultTime: new Date('2019-12-06T14:57:18.838Z'),
      locationId: 54
    };
    expect(deconstructObservationId(id)).toEqual(expected);
  });

  test('Should correctly deconstruct when location was NOT available', () => {
    const id = '25-0-2019-12-06T14:57:18.838Z';
    const expected = {
      timeseriesId: 25,
      resultTime: new Date('2019-12-06T14:57:18.838Z')
    };
    expect(deconstructObservationId(id)).toEqual(expected);
  });

});