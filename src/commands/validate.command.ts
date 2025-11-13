import chalk from 'chalk';
import { mysqlDataSource, sqliteDataSource } from '../data-sources';
import { Payment, Rental, Staff } from '../models/source.models';
import {
  FactPayment,
  DimDate,
  FactRental,
  DimStore,
} from '../models/analytics.models';

/* --- validate command --- */
export const validateCommand = async () => {
  console.log(chalk.cyan('Starting validation command...'));

  /* time window and format date strings */
  const thirtyDaysAgo = new Date();
  thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
  const thirtyDaysAgoString = thirtyDaysAgo.toISOString().split('T')[0]!;

  let errorsFound = 0;

  try {
    if (!mysqlDataSource.isInitialized) await mysqlDataSource.initialize();
    if (!sqliteDataSource.isInitialized) await sqliteDataSource.initialize();
    console.log(chalk.green('Databases connected.'));

    console.log(
      chalk.yellow(`\n--- Validating Data Since ${thirtyDaysAgoString} ---`)
    );

    /* validate payment count */
    const sourcePaymentCount = await mysqlDataSource
      .getRepository(Payment)
      .createQueryBuilder('payment')
      .where('payment.paymentDate > :date', { date: thirtyDaysAgo })
      .getCount();

    const targetPaymentCount = await sqliteDataSource
      .getRepository(FactPayment)
      .createQueryBuilder('fact_payment')
      .innerJoin(
        DimDate,
        'dim_date',
        'fact_payment.dateKeyPaid = dim_date.dateKey'
      )
      .where('dim_date.date > :date', { date: thirtyDaysAgoString })
      .getCount();

    if (sourcePaymentCount === targetPaymentCount) {
      console.log(
        chalk.green(`Payment Count:  PASSED (${sourcePaymentCount})`)
      );
    } else {
      console.log(
        chalk.red(
          `Payment Count:  FAILED (Source: ${sourcePaymentCount}, Target: ${targetPaymentCount})`
        )
      );
      errorsFound++;
    }

    /* validate payment sum */
    const sourcePaymentSum = (
      await mysqlDataSource
        .getRepository(Payment)
        .createQueryBuilder('payment')
        .select('SUM(payment.amount)', 'sum')
        .where('payment.paymentDate > :date', { date: thirtyDaysAgo })
        .getRawOne()
    ).sum;

    const targetPaymentSum = (
      await sqliteDataSource
        .getRepository(FactPayment)
        .createQueryBuilder('fact_payment')
        .innerJoin(
          DimDate,
          'dim_date',
          'fact_payment.dateKeyPaid = dim_date.dateKey'
        )
        .select('SUM(fact_payment.amount)', 'sum')
        .where('dim_date.date > :date', { date: thirtyDaysAgoString })
        .getRawOne()
    ).sum;

    /* standardize floats */
    const sourceSumFloat = parseFloat(sourcePaymentSum).toFixed(2);
    const targetSumFloat = parseFloat(targetPaymentSum).toFixed(2);

    if (sourceSumFloat === targetSumFloat) {
      console.log(chalk.green(`Payment Total:  PASSED ($${sourceSumFloat})`));
    } else {
      console.log(
        chalk.red(
          `Payment Total:  FAILED (Source: $${sourceSumFloat}, Target: $${targetSumFloat})`
        )
      );
      errorsFound++;
    }

    /* validate rental count */
    const sourceRentalCount = await mysqlDataSource
      .getRepository(Rental)
      .createQueryBuilder('rental')
      .where('rental.rentalDate > :date', { date: thirtyDaysAgo })
      .getCount();

    const targetRentalCount = await sqliteDataSource
      .getRepository(FactRental)
      .createQueryBuilder('fact_rental')
      .innerJoin(
        DimDate,
        'dim_date',
        'fact_rental.dateKeyRented = dim_date.dateKey'
      )
      .where('dim_date.date > :date', { date: thirtyDaysAgoString })
      .getCount();

    if (sourceRentalCount === targetRentalCount) {
      console.log(chalk.green(`Rental Count:   PASSED (${sourceRentalCount})`));
    } else {
      console.log(
        chalk.red(
          `Rental Count:   FAILED (Source: ${sourceRentalCount}, Target: ${targetRentalCount})`
        )
      );
      errorsFound++;
    }

    /* validate revenue by store */
    console.log(chalk.yellow('\n--- Validating Revenue by Store ---'));

    const sourceStoreRevenue = await mysqlDataSource
      .getRepository(Payment)
      .createQueryBuilder('payment')
      .innerJoin(Staff, 'staff', 'payment.staffId = staff.staffId')
      .select('staff.storeId', 'store_id')
      .addSelect('SUM(payment.amount)', 'sum')
      .where('payment.paymentDate > :date', { date: thirtyDaysAgo })
      .groupBy('staff.storeId')
      .orderBy('staff.storeId')
      .getRawMany();

    const targetStoreRevenue = await sqliteDataSource
      .getRepository(FactPayment)
      .createQueryBuilder('fact_payment')
      .innerJoin(
        DimStore,
        'dim_store',
        'fact_payment.storeKey = dim_store.storeKey'
      )
      .innerJoin(
        DimDate,
        'dim_date',
        'fact_payment.dateKeyPaid = dim_date.dateKey'
      )
      .select('dim_store.storeId', 'store_id')
      .addSelect('SUM(fact_payment.amount)', 'sum')
      .where('dim_date.date > :date', { date: thirtyDaysAgoString })
      .groupBy('dim_store.storeId')
      .orderBy('dim_store.storeId')
      .getRawMany();

    let storeMismatch = false;
    if (sourceStoreRevenue.length !== targetStoreRevenue.length) {
      storeMismatch = true;
    } else {
      for (const sourceStore of sourceStoreRevenue) {
        const targetStore = targetStoreRevenue.find(
          (t) => t.store_id === sourceStore.store_id
        );
        if (
          !targetStore ||
          parseFloat(sourceStore.sum).toFixed(2) !==
            parseFloat(targetStore.sum).toFixed(2)
        ) {
          storeMismatch = true;
          break;
        }
      }
    }

    if (storeMismatch) {
      console.log(chalk.red('Revenue by Store: FAILED'));
      errorsFound++;
    } else {
      console.log(chalk.green('Revenue by Store: PASSED'));
    }

    /* report */
    if (errorsFound === 0) {
      console.log(chalk.green.bold('\nValidation PASSED. Data is consistent.'));
    } else {
      console.log(
        chalk.red.bold(
          `\nValidation FAILED. Found ${errorsFound} inconsistencies.`
        )
      );
    }
  } catch (error) {
    console.error(chalk.red('Error during validation:'), error);
    process.exit(1);
  } finally {
    /* destroy connections if not in a test environment */
    if (process.env.NODE_ENV !== 'test') {
      if (mysqlDataSource.isInitialized) await mysqlDataSource.destroy();
      if (sqliteDataSource.isInitialized) await sqliteDataSource.destroy();
      console.log(chalk.green('\nDatabase connections closed.'));
    }
  }
};
