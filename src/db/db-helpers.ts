

export function arrayToLtreeString(arr: string[]): string {
  const str = arr.join('.');
  // Convert any hypens to underscores as ltree doesn't accept hypens
  const strNoHyphens = str.replace(/-/g, '_');
  return strNoHyphens;
}


export function ltreeStringToArray(str: string): string[] {
  // Convert any underscores to hypens
  const strWithHyphens = str.replace(/_/g, '-');
  const arr = strWithHyphens.split('.');
  return arr;
}


export function platformIdToAnywhereLquery(platformId: string): string {
  const withUnderscores = platformId.replace(/-/g, '_');
  const bookended = `*.${withUnderscores}.*`;
  return bookended;
}


