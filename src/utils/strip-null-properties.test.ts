import {stripNullProperties} from './strip-null-properties';

//-------------------------------------------------
// Tests
//-------------------------------------------------
describe('Test stripNullProperties function', () => {

  test('Strips null properties', () => {
    const input = {a: 1, b: null};
    const expected = {a: 1};
    const output = stripNullProperties(input);
    expect(output).toEqual(expected);
  });


  test('Does not modify input', () => {
    const input = {a: 1, b: null};
    stripNullProperties(input);
    expect(input).toEqual({a: 1, b: null});
  });


  test('Does not remove properties equal to undefined', () => {
    const input = {a: 1, b: null, c: undefined};
    const expected = {a: 1, c: undefined};
    const output = stripNullProperties(input);
    expect(output).toEqual(expected);
  });

  
  // Had some issues with date objects being removed by this function.
  test('Does not remove date objects', () => {
    const d = new Date('2019-08-30T14:31:32.047Z');
    const input = { 
      id: 10,
      client_id: 'my-static-platform',
      name: 'My static Platform',
      static: true,
      description: 'It stays still',
      created_at: d,
      deleted_at: null,
      in_deployment: 'my-deployment',
      is_hosted_by: null 
    };
    const expected = {
      id: 10,
      client_id: 'my-static-platform',
      name: 'My static Platform',
      static: true,
      description: 'It stays still',
      created_at: d,
      in_deployment: 'my-deployment',      
    };
    const output = stripNullProperties(input);
    expect(output).toEqual(expected);
  });  


  test('Does not mess up arrays', () => {
    const input = { 
      disciplines: ['Meteorology']
    };
    const expected = {
      disciplines: ['Meteorology'],      
    };
    const output = stripNullProperties(input);
    expect(output).toEqual(expected);
  });    


  test('Does not mess up objects', () => {
    const input = { 
      hasResult: {
        value: 22.2
      }
    };
    const expected = {
      hasResult: {
        value: 22.2
      }
    };
    const output = stripNullProperties(input);
    expect(output).toEqual(expected);
  });  


});

