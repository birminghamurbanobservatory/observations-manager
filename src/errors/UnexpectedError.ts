export class UnexpectedError extends Error {

  public statusCode: number;

  public constructor(message = 'An unexpected error occurred') {
    super(message); // 'Error' breaks prototype chain here
    Object.setPrototypeOf(this, new.target.prototype); // restore prototype chain
    // Ensure the name of this error is the same as the class name.
    this.name = this.constructor.name;
    // Add a statusCode, useful when converting an error object to a HTTP response
    this.statusCode = 500;
  }

}