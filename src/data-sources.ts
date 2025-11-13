import 'reflect-metadata';
import { DataSource } from 'typeorm';
import * as SourceModels from './models/source.models';
import * as AnalyticsModels from './models/analytics.models';

/* sql datasource */
export const mysqlDataSource = new DataSource({
  type: 'mysql',
  host: 'localhost',
  port: 3306,
  username: 'root',
  password: 'sockmonkey',
  database: 'sakila',
  entities: Object.values(SourceModels),
  logging: false,
});

/* sqlite datasource */
export const sqliteDataSource = new DataSource({
  type: 'sqlite',
  database: 'analytics.db',
  entities: Object.values(AnalyticsModels),
  synchronize: false,
  logging: false,
});
