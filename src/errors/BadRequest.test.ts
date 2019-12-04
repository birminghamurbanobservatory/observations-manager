import {BadRequest} from './BadRequest';
import {OperationalError} from './OperationalError';


//-------------------------------------------------
// Tests
//-------------------------------------------------
describe('Check BadRequest', () => {

  test('Is an instance of Error', () => {
    expect(new BadRequest('Whoops')).toBeInstanceOf(Error);
  });

  test('Is an instance of OperationalError', () => {
    expect(new BadRequest('Whoops')).toBeInstanceOf(OperationalError);
  }); 
 
  test('It has the correct name property', () => {
    const exampleError = new BadRequest('Whoops');
    expect(exampleError.name).toBe('BadRequest');
  });    

  test('Has the correct statusCode', () => {
    const exampleError = new BadRequest('Whoops');
    expect(exampleError.statusCode).toBe(400);
  });     

  test('Sets a default message when left undefined', () => {
    const exampleError = new BadRequest();
    expect(typeof exampleError.message).toBe('string');
    expect(exampleError.message.length).toBeGreaterThan(0);
  });  

  test('Applies a custom message', () => {
    const msg = 'Whoops';
    const exampleError = new BadRequest(msg);
    expect(exampleError.message).toBe(msg);
  });

});
