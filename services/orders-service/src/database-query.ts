export interface DatabaseQuery {
  query<Row>(
    text: string,
    values?: readonly unknown[],
  ): Promise<{
    rows: Row[];
  }>;
}
