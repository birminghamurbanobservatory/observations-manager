import {convertPropsToExactWhere} from './timeseries.service';


describe('Testing of convertPropsToExactWhere function', () => {

  test('Converts a full set of props', () => {
    
    const props = {
      madeBySensor: 'sensor-123',
      inDeployments: ['deployment-1'],
      hostedByPath: ['platform-1'],
      observedProperty: 'air-temperature',
      hasFeatureOfInterest: 'weather',
      usedProcedures: ['point-sample']
    };

    const expected = {
      madeBySensor: 'sensor-123',
      inDeployments: ['deployment-1'],
      hostedByPath: ['platform-1'],
      observedProperty: 'air-temperature',
      hasFeatureOfInterest: 'weather',
      usedProcedures: ['point-sample']      
    };

    expect(convertPropsToExactWhere(props)).toEqual(expected);

  });

  test('Converts a missing props to $exists operators', () => {
    
    const props = {
      madeBySensor: 'sensor-123',
      inDeployments: ['deployment-1'],
      observedProperty: 'air-temperature',
      hasFeatureOfInterest: 'weather',
    };

    const expected = {
      madeBySensor: 'sensor-123',
      inDeployments: ['deployment-1'],
      hostedByPath: {$exists: false},
      observedProperty: 'air-temperature',
      hasFeatureOfInterest: 'weather',
      usedProcedures: {$exists: false}     
    };

    expect(convertPropsToExactWhere(props)).toEqual(expected);

  });



  test('Re-orderes the appropriate arrays', () => {
    
    const props = {
      madeBySensor: 'sensor-123',
      inDeployments: ['deployment-b', 'deployment-c', 'deployment-a'],
      hostedByPath: ['lamppost-12', 'beta-weather-station'],
      observedProperty: 'air-temperature',
      hasFeatureOfInterest: 'weather',
      usedProcedures: ['point-sample', 'averaged']     
    };

    const expected = {
      madeBySensor: 'sensor-123',
      inDeployments: ['deployment-a', 'deployment-b', 'deployment-c'], // do want this re-ordered
      hostedByPath: ['lamppost-12', 'beta-weather-station'], // don't want this re-ordered, as the order means something
      observedProperty: 'air-temperature',
      hasFeatureOfInterest: 'weather',
      usedProcedures: ['point-sample', 'averaged']  // don't want this re-ordered, as the order means something
    };

    expect(convertPropsToExactWhere(props)).toEqual(expected);

    // We also want to check it doesn't mutate the input array
    expect(props.inDeployments).toEqual(['deployment-b', 'deployment-c', 'deployment-a']);

  });


});