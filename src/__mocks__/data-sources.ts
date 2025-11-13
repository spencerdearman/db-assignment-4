import { DataSource } from 'typeorm';
import * as AnalyticsModels from '../models/analytics.models';

/* get the real mysqlDataSource */
const actualDataSources = jest.requireActual('../data-sources');
export const mysqlDataSource = actualDataSources.mysqlDataSource;

/* creating the fake sqlite datasource for clean database for each test */
export const sqliteDataSource = new DataSource({
  type: 'sqlite',
  database: ':memory:',
  entities: Object.values(AnalyticsModels),
  synchronize: true,
});

/* helper for the test repository */
export const getTestDbRepo = (entity: any) => {
  return sqliteDataSource.getRepository(entity);
};
