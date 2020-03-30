import {extractTimeseriesPropsFromObservation, generateObservationId, deconstructObservationId, observationAppToClient, observationClientToApp, observationDbToApp} from './observation.service';


describe('Testing of extractTimeseriesPropsFromObservation function', () => {

  test('Should correctly extract the timeseries props from an observation', () => {
    const observation = {
      madeBySensor: 'sensor-123',
      hasResult: {
        value: '12'
      },
      resultTime: new Date('2019-12-04T17:26:23.205Z'),
      hasFeatureOfInterest: 'EarthAtmosphere',
      observedProperty: 'AirTemperature',
      disciplines: ['Meteorology']
    };
    const expected = {
      madeBySensor: 'sensor-123',
      hasFeatureOfInterest: 'EarthAtmosphere',
      observedProperty: 'AirTemperature',
      disciplines: ['Meteorology']     
    };
    expect(extractTimeseriesPropsFromObservation(observation)).toEqual(expected);
  });


  test('Should correctly extract the timeseries props from an already full observation', () => {
    const observation = {
      madeBySensor: 'sensor-123',
      hasResult: {
        value: '12'
      },
      resultTime: new Date('2019-12-04T17:26:23.205Z'),
      inDeployments: ['deployment-1'],
      hostedByPath: ['platform-1'],
      hasFeatureOfInterest: 'EarthAtmosphere',
      observedProperty: 'AirTemperature',
      unit: 'DegreeCelsius', 
      disciplines: ['Meteorology'],
      usedProcedures: ['PointSample']
    };
    const expected = {
      madeBySensor: 'sensor-123',
      inDeployments: ['deployment-1'],
      hostedByPath: ['platform-1'],
      hasFeatureOfInterest: 'EarthAtmosphere',
      observedProperty: 'AirTemperature', 
      unit: 'DegreeCelsius',
      disciplines: ['Meteorology'],
      usedProcedures: ['PointSample']      
    };
    expect(extractTimeseriesPropsFromObservation(observation)).toEqual(expected);
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




describe('observationClientToApp function tests', () => {

  test('Can convert a fairly basic observation correctly.', () => {

    const observationClient = {
      madeBySensor: 'sensor-123',
      hasResult: {
        value: 12
      },
      resultTime: '2019-12-04T17:26:23.205Z',
      inDeployments: ['deployment-1'],
      hostedByPath: ['platform-1'],
      hasFeatureOfInterest: 'EarthAtmosphere',
      observedProperty: 'AirTemperature', 
      usedProcedures: ['PointSample']
    };

    const expected = {
      madeBySensor: 'sensor-123',
      hasResult: {
        value: 12
      },
      resultTime: new Date('2019-12-04T17:26:23.205Z'),
      inDeployments: ['deployment-1'],
      hostedByPath: ['platform-1'],
      hasFeatureOfInterest: 'EarthAtmosphere',
      observedProperty: 'AirTemperature', 
      usedProcedures: ['PointSample']
    };

    const observationApp = observationClientToApp(observationClient);
    expect(observationApp).toEqual(expected);

  });

  test('Can convert an observation that has a phenomenonTime object correctly.', () => {

    const observationClient = {
      madeBySensor: 'rain-gauge-123',
      hasResult: {
        value: 0.3
      },
      resultTime: '2019-12-04T17:26:23.205Z',
      phenomenonTime: {
        hasBeginning: '2019-12-04T17:16:23.205Z',
        hasEnd: '2019-12-04T17:26:23.205Z'
      },
      inDeployments: ['deployment-1'],
      hostedByPath: ['platform-1'],
      hasFeatureOfInterest: 'EarthAtmosphere',
      observedProperty: 'precipitation-depth'
    };

    const expected = {
      madeBySensor: 'rain-gauge-123',
      hasResult: {
        value: 0.3
      },
      resultTime: new Date('2019-12-04T17:26:23.205Z'),
      phenomenonTime: {
        hasBeginning: new Date('2019-12-04T17:16:23.205Z'),
        hasEnd: new Date('2019-12-04T17:26:23.205Z')
      },
      inDeployments: ['deployment-1'],
      hostedByPath: ['platform-1'],
      hasFeatureOfInterest: 'EarthAtmosphere',
      observedProperty: 'precipitation-depth'
    };

    const observationApp = observationClientToApp(observationClient);
    expect(observationApp).toEqual(expected);

  });

});



describe('observationAppToClient function tests', () => {

  test('Can convert a fairly basic observation correctly.', () => {

    const observationApp = {
      id: 12424,
      clientId: '151-12-2019-12-04T17:26:23.205Z',
      madeBySensor: 'sensor-123',
      hasResult: {
        value: 12
      },
      resultTime: new Date('2019-12-04T17:26:23.205Z'),
      inDeployments: ['deployment-1'],
      hostedByPath: ['platform-1'],
      hasFeatureOfInterest: 'EarthAtmosphere',
      observedProperty: 'AirTemperature', 
      usedProcedures: ['PointSample']
    };

    const expected = {
      id: '151-12-2019-12-04T17:26:23.205Z',
      madeBySensor: 'sensor-123',
      hasResult: {
        value: 12
      },
      resultTime: '2019-12-04T17:26:23.205Z',
      inDeployments: ['deployment-1'],
      hostedByPath: ['platform-1'],
      hasFeatureOfInterest: 'EarthAtmosphere',
      observedProperty: 'AirTemperature', 
      usedProcedures: ['PointSample']
    };

    const observationClient = observationAppToClient(observationApp);
    expect(observationClient).toEqual(expected);

  });


  test('Can convert an observation that has a phenomenonTime object correctly.', () => {

    const observationApp = {
      id: 12424,
      clientId: '111-9-2019-12-04T17:26:23.205Z',
      madeBySensor: 'rain-gauge-123',
      hasResult: {
        value: 0.3
      },
      resultTime: new Date('2019-12-04T17:26:23.205Z'),
      phenomenonTime: {
        hasBeginning: new Date('2019-12-04T17:16:23.205Z'),
        hasEnd: new Date('2019-12-04T17:26:23.205Z')
      },
      inDeployments: ['deployment-1'],
      hostedByPath: ['platform-1'],
      hasFeatureOfInterest: 'EarthAtmosphere',
      observedProperty: 'precipitation-depth'
    };

    const expected = {
      id: '111-9-2019-12-04T17:26:23.205Z',
      madeBySensor: 'rain-gauge-123',
      hasResult: {
        value: 0.3
      },
      resultTime: '2019-12-04T17:26:23.205Z',
      phenomenonTime: {
        hasBeginning: '2019-12-04T17:16:23.205Z',
        hasEnd: '2019-12-04T17:26:23.205Z'
      },
      inDeployments: ['deployment-1'],
      hostedByPath: ['platform-1'],
      hasFeatureOfInterest: 'EarthAtmosphere',
      observedProperty: 'precipitation-depth'
    };

    const observationClient = observationAppToClient(observationApp);
    expect(observationClient).toEqual(expected);

  });  

});


describe('observationDbToApp function tests', () => {

  test('Converts db response correctly', () => {
    
    const observationDb = {
      id: '223253',
      timeseries_id: 54,
      location_id: 33,
      result_time: '2019-12-04T17:26:23.205Z',
      has_beginning: '2019-12-04T17:16:23.205Z',
      has_end: '2019-12-04T17:26:23.205Z',
      value_number: '0.3',
      value_boolean: null,
      value_text: null,
      value_json: null,
      flags: null,
      made_by_sensor: 'rain-gauge-123',
      in_deployments: ['deployment-1'],
      hosted_by_path: 'bob-back-garden.platform-1',
      has_feature_of_interest: 'EarthAtmosphere',
      observed_property: 'precipitation-depth',
      used_procedures: ['tip-sum'],
      location_client_id: '146380a6-0614-48ce-a0ae-a1bf935f015c',
      location_geojson: {type: 'Point', coordinates: [-1.9, 52.9]},
      location_valid_at: '2019-07-05T12:43:24.621Z' 
    };

    const expected = {
      id: 223253,
      clientId: '54-33-2019-12-04T17:26:23.205Z',
      resultTime: new Date('2019-12-04T17:26:23.205Z'),
      phenomenonTime: {
        hasBeginning: new Date('2019-12-04T17:16:23.205Z'),
        hasEnd: new Date('2019-12-04T17:26:23.205Z')
      },
      hasResult: {
        value: 0.3
      },
      madeBySensor: 'rain-gauge-123',
      inDeployments: ['deployment-1'],
      hostedByPath: ['bob-back-garden', 'platform-1'],
      hasFeatureOfInterest: 'EarthAtmosphere',
      observedProperty: 'precipitation-depth',
      usedProcedures: ['tip-sum'],
      timeseriesId: 54,
      location: {
        id: 33,
        clientId: '146380a6-0614-48ce-a0ae-a1bf935f015c',
        geometry: {type: 'Point', coordinates: [-1.9, 52.9]},
        validAt: new Date('2019-07-05T12:43:24.621Z')
      }

    };

    const observationApp = observationDbToApp(observationDb);
    expect(observationApp).toEqual(expected);

  });

});