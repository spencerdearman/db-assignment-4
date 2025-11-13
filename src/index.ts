import 'reflect-metadata';
import { Command } from 'commander';
const program = new Command();

import { initCommand } from './commands/init.command';
import { fullLoadCommand } from './commands/full-load.command';
import { incrementalCommand } from './commands/incremental.command';
import { validateCommand } from './commands/validate.command';

/* prevents the commander from killing the test runner */
if (process.env.NODE_ENV === 'test') {
  program.exitOverride();
}

/* enable commands */
program.command('init').action(initCommand);
program.command('full-load').action(fullLoadCommand);
program.command('incremental').action(incrementalCommand);
program.command('validate').action(validateCommand);

/* run the app if it isn't a test env */
if (process.env.NODE_ENV !== 'test') {
  program.parse(process.argv);
}

export { program };
