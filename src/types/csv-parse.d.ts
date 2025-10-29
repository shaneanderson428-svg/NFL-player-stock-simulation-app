declare module 'csv-parse/sync' {
  type Options = {
    columns?: boolean | Array<string> | ((header: string[]) => any);
    skip_empty_lines?: boolean;
    from?: number;
    to?: number;
    [key: string]: any;
  };
  export function parse(input: string, options?: Options): any;
  export { parse as default };
}
