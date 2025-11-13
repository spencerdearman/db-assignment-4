import chalk from 'chalk';
import { sqliteDataSource } from '../data-sources';

export const initCommand = async () => {
  try {
    console.log('Initializing analytics database...');
    if (!sqliteDataSource.isInitialized) await sqliteDataSource.initialize();
    await sqliteDataSource.synchronize();
    console.log(chalk.green.bold('Analytics database initialized.'));
  } catch (error) {
    console.error(chalk.red.bold('Initialization error:', error));
    process.exit(1);
  } finally {
    if (process.env.NODE_ENV !== 'test') {
      if (sqliteDataSource.isInitialized) await sqliteDataSource.destroy();
      console.log(chalk.green('\nDatabase connections closed.'));
    }
  }
};
