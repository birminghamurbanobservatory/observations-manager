import {DatabaseError} from './DatabaseError';
import {OperationalError} from './OperationalError';


//-------------------------------------------------
// Tests
//-------------------------------------------------
describe('Check DatabaseError', () => {

  test('DatabaseError is an instance of Error', () => {
    expect(new DatabaseError('Whoops')).toBeInstanceOf(Error);
  });

  test('DatabaseError is an instance of OperationalError', () => {
    expect(new DatabaseError('Whoops')).toBeInstanceOf(OperationalError);
  }); 
 
  test('It has the correct name property', () => {
    const exampleError = new DatabaseError('Whoops');
    expect(exampleError.name).toBe('DatabaseError');
  });    

  test('Has the correct statusCode', () => {
    const exampleError = new DatabaseError('Whoops');
    expect(exampleError.statusCode).toBe(500);
  });     

  test('Sets a default message when left undefined', () => {
    const exampleError = new DatabaseError();
    expect(typeof exampleError.message).toBe('string');
    expect(exampleError.message.length).toBeGreaterThan(0);
  });  

  test('Applies a custom message', () => {
    const msg = 'Whoops';
    const exampleError = new DatabaseError(msg);
    expect(exampleError.message).toBe(msg);
  });

  test('DatabaseError can be passed a private message', () => {
    const msg = 'Whoops';
    const privateMessage = 'This was a big whoops, do not tell the client this.';
    const exampleError = new DatabaseError(msg, privateMessage);
    expect(exampleError.privateMessage).toBe(privateMessage);
  });   

});
