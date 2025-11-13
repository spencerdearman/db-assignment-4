import { program } from './index';
import {
  mysqlDataSource,
  sqliteDataSource,
  getTestDbRepo,
} from './data-sources';
import { Actor } from './models/source.models';
import { DimActor, FactRental, SyncState } from './models/analytics.models';
import { DataSource } from 'typeorm';

jest.mock('./data-sources');
const runCli = async (command: string) => {
  return program.parseAsync(['node', 'index.ts', command]);
};

/* setup */
let realMysql: DataSource;
const testSqlite = sqliteDataSource;

/* connect to mySql db */
beforeAll(async () => {
  realMysql = mysqlDataSource;
  await realMysql.initialize();
  await testSqlite.initialize();
});

/* disconnect */
afterAll(async () => {
  await realMysql.destroy();
  await testSqlite.destroy();
});

/* clean in-memory database */
beforeEach(async () => {
  await testSqlite.synchronize(true);
});

/* clean up the data */
afterEach(async () => {
  await realMysql
    .getRepository(Actor)
    .save({ actorId: 1, firstName: 'SPENCER', lastName: 'DEARMAN' });
  const jestActor = await realMysql
    .getRepository(Actor)
    .findOneBy({ firstName: 'JEST' });
  if (jestActor) {
    await realMysql.getRepository(Actor).delete(jestActor.actorId);
  }
});

/* test init */
test('1. Init command', async () => {
  /* check if the tables exist */
  const tables = await testSqlite.query(
    "SELECT name FROM sqlite_master WHERE type='table'"
  );

  /* find tables */
  const hasDimActor = tables.some((t: any) => t.name === 'dim_actor');
  const hasDimCategory = tables.some((t: any) => t.name === 'dim_category');
  const hasDimStore = tables.some((t: any) => t.name === 'dim_store');
  const hasDimCustomer = tables.some((t: any) => t.name === 'dim_customer');
  const hasDimFilm = tables.some((t: any) => t.name === 'dim_film');
  const hasDimDate = tables.some((t: any) => t.name === 'dim_date');
  const hasBridgeFilmActor = tables.some(
    (t: any) => t.name === 'bridge_film_actor'
  );
  const hasBridgeFilmCategory = tables.some(
    (t: any) => t.name === 'bridge_film_category'
  );
  const hasFactPayment = tables.some((t: any) => t.name === 'fact_payment');
  const hasFactRental = tables.some((t: any) => t.name === 'fact_rental');
  const hasSyncState = tables.some((t: any) => t.name === 'sync_state');

  /* verify */
  expect(hasDimActor).toBe(true);
  expect(hasDimCategory).toBe(true);
  expect(hasDimStore).toBe(true);
  expect(hasDimCustomer).toBe(true);
  expect(hasDimFilm).toBe(true);
  expect(hasDimDate).toBe(true);
  expect(hasBridgeFilmActor).toBe(true);
  expect(hasBridgeFilmCategory).toBe(true);
  expect(hasFactPayment).toBe(true);
  expect(hasFactRental).toBe(true);
  expect(hasSyncState).toBe(true);
});

/* test full-load */
test('2. Full-load command', async () => {
  /* find source count */
  const sourceCount = await realMysql.getRepository(Actor).count();
  expect(sourceCount).toBe(200);

  /* run full-load */
  await runCli('full-load');

  /* check target count */
  const targetRepo = getTestDbRepo(DimActor);
  const targetCount = await targetRepo.count();
  expect(targetCount).toEqual(sourceCount);
});

/* test incremental (new data) */
test('3. Incremental command (new data)', async () => {
  /* check full-load before adding */
  await runCli('full-load');

  /* check the sync state */
  const syncRepo = getTestDbRepo(SyncState);
  const initialSync = await syncRepo.findOneBy({ tableName: 'actor' });
  expect(initialSync).toBeDefined();
  await new Promise((res) => setTimeout(res, 1000));

  /* add a new actor to the MySQL database */
  const newActor = await realMysql.getRepository(Actor).save({
    firstName: 'JEST',
    lastName: 'TEST',
  });

  /* run the incremental command */
  await runCli('incremental');

  /* check if the new actor exists */
  const targetRepo = getTestDbRepo(DimActor);
  const syncedActor = await targetRepo.findOneBy({ actorId: newActor.actorId });
  expect(syncedActor).toBeDefined();
  expect(syncedActor?.firstName).toBe('JEST');
});

/* test incremental (updates) */
test('4. Incremental command (updates)', async () => {
  /* run full-load */
  await runCli('full-load');

  /* check that actor 1 is 'SPENCER' */
  const targetRepo = getTestDbRepo(DimActor);
  const originalActor = await targetRepo.findOneBy({ actorId: 1 });
  expect(originalActor?.firstName).toBe('SPENCER');

  // ACT: Update the actor in the REAL MySQL database
  // We must pause for 1 second to ensure the last_update timestamp is different
  await new Promise((res) => setTimeout(res, 1000));
  await realMysql.getRepository(Actor).save({
    actorId: 1,
    firstName: 'SPENCER',
    lastName: 'DEARMAN',
  });

  // Run the command
  await runCli('incremental');

  // ASSERT: Check if the actor was updated in the TEST SQLite DB
  const updatedActor = await targetRepo.findOneBy({ actorId: 1 });
  expect(updatedActor?.firstName).toBe('SPENCER');
  expect(updatedActor?.lastName).toBe('DEARMAN');
});

/* test validate */
test('5. Validate command', async () => {
  await runCli('full-load');

  /* capture the output */
  const consoleMock = jest.spyOn(console, 'log').mockImplementation(() => {});
  await runCli('validate');

  /* match the console output */
  const logCalls = consoleMock.mock.calls.map((call) => call[0]);
  const successMessage = logCalls.find((call) =>
    call.includes('Validation PASSED')
  );
  expect(successMessage).toBeDefined();
  consoleMock.mockRestore();
});
