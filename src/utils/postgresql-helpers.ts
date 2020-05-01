

// Comes in handy when you need to perform an exact match on an array column. Order is important when making such a database query, and therefore you're expect to have already sorted the array if required before passing it to this function.
export function arrayToPostgresArrayString(array: string[]): string {

  const joined = array.join(',');
  const fullString = `{${joined}}`;
  return fullString;

}