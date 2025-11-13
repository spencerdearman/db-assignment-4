import chalk from 'chalk';
import { mysqlDataSource, sqliteDataSource } from '../data-sources';
import { EntityManager } from 'typeorm';
import {
  Actor,
  Category,
  Store,
  Customer,
  Film,
  FilmActor,
  FilmCategory,
  Rental,
  Payment,
  Staff,
} from '../models/source.models';
import {
  DimActor,
  DimCategory,
  DimStore,
  DimCustomer,
  DimFilm,
  BridgeFilmActor,
  BridgeFilmCategory,
  FactRental,
  FactPayment,
  SyncState,
  DimDate,
} from '../models/analytics.models';
import { generateKey, createDimDate, duration } from '../utils/date.utils';

/* --- full-load command --- */
export const fullLoadCommand = async () => {
  try {
    console.log('Starting full load command...');
    if (!mysqlDataSource.isInitialized) await mysqlDataSource.initialize();
    if (!sqliteDataSource.isInitialized) await sqliteDataSource.initialize();
    console.log(chalk.green('Databases connected.'));

    await sqliteDataSource.transaction(
      async (transactionalEntityManager: EntityManager) => {
        /* ---------- loading dimensions ---------- */
        console.log(chalk.yellow('\n--- Loading Dimensions ---'));

        /* -- actors section -- */
        const sourceActorRepo = mysqlDataSource.getRepository(Actor);
        const targetActorRepo =
          transactionalEntityManager.getRepository(DimActor);
        const actorsFromSource = await sourceActorRepo.find();
        const newDimActors = actorsFromSource.map((actor: Actor) => {
          const newDimActor = new DimActor();
          newDimActor.actorId = actor.actorId;
          newDimActor.firstName = actor.firstName;
          newDimActor.lastName = actor.lastName;
          newDimActor.lastUpdate = actor.lastUpdate;
          return newDimActor;
        });
        await targetActorRepo.insert(newDimActors);
        console.log(`Actors: LOADED (${newDimActors.length} actors)`);

        /* -- categories section -- */
        const sourceCategoryRepo = mysqlDataSource.getRepository(Category);
        const targetCategoryRepo =
          transactionalEntityManager.getRepository(DimCategory);
        const categoriesFromSource = await sourceCategoryRepo.find();
        const newDimCategories = categoriesFromSource.map(
          (category: Category) => {
            const newDimCategory = new DimCategory();
            newDimCategory.categoryId = category.categoryId;
            newDimCategory.name = category.name;
            newDimCategory.lastUpdate = category.lastUpdate;
            return newDimCategory;
          }
        );
        await targetCategoryRepo.insert(newDimCategories);
        console.log(
          `Categories: LOADED (${newDimCategories.length} categories)`
        );

        /* -- stores section -- */
        const sourceStoreRepo = mysqlDataSource.getRepository(Store);
        const targetStoreRepo =
          transactionalEntityManager.getRepository(DimStore);
        const storesFromSource = await sourceStoreRepo.find({
          relations: ['address', 'address.city', 'address.city.country'],
        });
        const newDimStores = storesFromSource.map((store: Store) => {
          const newDimStore = new DimStore();
          newDimStore.storeId = store.storeId;
          newDimStore.city = store.address.city.city;
          newDimStore.country = store.address.city.country.country;
          newDimStore.lastUpdate = store.lastUpdate;
          return newDimStore;
        });
        await targetStoreRepo.insert(newDimStores);
        console.log(`Stores: LOADED (${newDimStores.length} stores)`);

        /* -- customer section -- */
        const sourceCustomerRepo = mysqlDataSource.getRepository(Customer);
        const targetCustomerRepo =
          transactionalEntityManager.getRepository(DimCustomer);
        const customersFromSource = await sourceCustomerRepo.find({
          relations: ['address', 'address.city', 'address.city.country'],
        });
        const newDimCustomers = customersFromSource.map(
          (customer: Customer) => {
            const newDimCustomer = new DimCustomer();
            newDimCustomer.customerId = customer.customerId;
            newDimCustomer.firstName = customer.firstName;
            newDimCustomer.lastName = customer.lastName;
            newDimCustomer.active = customer.active === 1;
            newDimCustomer.city = customer.address.city.city;
            newDimCustomer.country = customer.address.city.country.country;
            newDimCustomer.lastUpdate = customer.lastUpdate;
            return newDimCustomer;
          }
        );
        await targetCustomerRepo.insert(newDimCustomers);
        console.log(`Customers: LOADED (${newDimCustomers.length} customers)`);

        /* -- film section -- */
        const sourceFilmRepo = mysqlDataSource.getRepository(Film);
        const targetFilmRepo =
          transactionalEntityManager.getRepository(DimFilm);
        const filmsFromSource = await sourceFilmRepo.find({
          relations: ['language'],
        });
        const newDimFilms = filmsFromSource.map((film: Film) => {
          const newDimFilm = new DimFilm();
          newDimFilm.filmId = film.filmId;
          newDimFilm.title = film.title;
          newDimFilm.rating = film.rating;
          newDimFilm.length = film.length;
          newDimFilm.language = film.language ? film.language.name : 'Unknown';
          newDimFilm.releaseYear = film.releaseYear;
          newDimFilm.lastUpdate = film.lastUpdate;
          return newDimFilm;
        });
        await targetFilmRepo.insert(newDimFilms);
        console.log(`Films: LOADED (${newDimFilms.length} films)`);

        /* -- date section -- */
        const rentalDates = await mysqlDataSource.getRepository(Rental).find();
        const paymentDates = await mysqlDataSource
          .getRepository(Payment)
          .find();

        const dateSet = new Set<string>();
        rentalDates.forEach((r) => {
          dateSet.add(r.rentalDate.toISOString().split('T')[0]!);
          if (r.returnDate) {
            dateSet.add(r.returnDate.toISOString().split('T')[0]!);
          }
        });
        paymentDates.forEach((p) => {
          dateSet.add(p.paymentDate.toISOString().split('T')[0]!);
        });

        const dimDates = Array.from(dateSet).map((dateString) => {
          return createDimDate(new Date(dateString));
        });

        await transactionalEntityManager.getRepository(DimDate).save(dimDates);
        console.log(`Dates: LOADED (${dimDates.length} unique dates)`);

        console.log(chalk.green.bold(`Dimensions loaded.`));

        /* ---------- creating key maps ---------- */
        console.log(chalk.yellow('\n--- Creating Key Maps ---'));

        /* reading dimensions from sqlite to create the maps */
        const allDimActors = await targetActorRepo.find();
        const allDimCategories = await targetCategoryRepo.find();
        const allDimStores = await targetStoreRepo.find();
        const allDimCustomers = await targetCustomerRepo.find();
        const allDimFilms = await targetFilmRepo.find();

        /* mapping old_natural_key, new_surrogate_key */
        const actorKeyMap = new Map<number, number>();
        allDimActors.forEach((a) => actorKeyMap.set(a.actorId, a.actorKey));

        const categoryKeyMap = new Map<number, number>();
        allDimCategories.forEach((c) =>
          categoryKeyMap.set(c.categoryId, c.categoryKey)
        );

        const storeKeyMap = new Map<number, number>();
        allDimStores.forEach((s) => storeKeyMap.set(s.storeId, s.storeKey));

        const customerKeyMap = new Map<number, number>();
        allDimCustomers.forEach((c) =>
          customerKeyMap.set(c.customerId, c.customerKey)
        );

        const filmKeyMap = new Map<number, number>();
        allDimFilms.forEach((f) => filmKeyMap.set(f.filmId, f.filmKey));

        console.log(chalk.green.bold('Key maps created.'));

        /* ---------- loading bridge tables ---------- */
        console.log(chalk.yellow('\n--- Loading Bridge Tables ---'));

        /* -- bridge_film_actor -- */
        const sourceFilmActorRepo = mysqlDataSource.getRepository(FilmActor);
        const targetFilmActorRepo =
          transactionalEntityManager.getRepository(BridgeFilmActor);
        const filmActorsFromSource = await sourceFilmActorRepo.find();

        const newBridgeFilmActors = filmActorsFromSource
          .map((fa) => {
            const bridge = new BridgeFilmActor();
            bridge.filmKey = filmKeyMap.get(fa.filmId) as number;
            bridge.actorKey = actorKeyMap.get(fa.actorId) as number;
            return bridge;
          })
          .filter((b) => b.filmKey && b.actorKey);

        await targetFilmActorRepo.insert(newBridgeFilmActors);
        console.log(
          `Film-Actor Links: LOADED (${newBridgeFilmActors.length} links)`
        );

        /* -- bridge_film_category -- */
        const sourceFilmCategoryRepo =
          mysqlDataSource.getRepository(FilmCategory);
        const targetFilmCategoryRepo =
          transactionalEntityManager.getRepository(BridgeFilmCategory);
        const filmCategoriesFromSource = await sourceFilmCategoryRepo.find();

        const newBridgeFilmCategories = filmCategoriesFromSource
          .map((fc) => {
            const bridge = new BridgeFilmCategory();
            bridge.filmKey = filmKeyMap.get(fc.filmId) as number;
            bridge.categoryKey = categoryKeyMap.get(fc.categoryId) as number;
            return bridge;
          })
          .filter((b) => b.filmKey && b.categoryKey);

        await targetFilmCategoryRepo.insert(newBridgeFilmCategories);
        console.log(
          `Film-Category Links: LOADED (${newBridgeFilmCategories.length} links)`
        );
        console.log(chalk.green.bold(`Bridge tables loaded.`));

        /* ---------- loading fact tables ---------- */
        console.log(chalk.yellow('\n--- Loading Fact Tables ---'));

        /* -- fact_rental -- */
        const sourceRentalRepo = mysqlDataSource.getRepository(Rental);
        const targetRentalRepo =
          transactionalEntityManager.getRepository(FactRental);
        const rentalsFromSource = await sourceRentalRepo.find({
          relations: ['inventory', 'inventory.store'],
        });

        const newFactRentals = rentalsFromSource
          .map((rental) => {
            const fact = new FactRental();
            fact.rentalId = rental.rentalId;
            fact.dateKeyRented = generateKey(rental.rentalDate) as number;
            fact.dateKeyReturned = generateKey(rental.returnDate);

            /* using maps to get the surrogate keys */
            fact.customerKey = customerKeyMap.get(rental.customerId) as number;
            fact.filmKey = filmKeyMap.get(rental.inventory.filmId) as number;
            fact.storeKey = storeKeyMap.get(
              rental.inventory.store.storeId
            ) as number;

            fact.staffId = rental.staffId;
            fact.rentalDurationDays = duration(
              rental.rentalDate,
              rental.returnDate
            );

            return fact;
          })
          .filter((f) => f.customerKey && f.filmKey && f.storeKey);

        await targetRentalRepo.insert(newFactRentals);
        console.log(`Fact Rental: LOADED (${newFactRentals.length} rentals)`);

        /* -- fact_payment -- */
        const sourcePaymentRepo = mysqlDataSource.getRepository(Payment);
        const targetPaymentRepo =
          transactionalEntityManager.getRepository(FactPayment);
        const paymentsFromSource = await sourcePaymentRepo.find({
          relations: ['staff'],
        });

        const newFactPayments = paymentsFromSource
          .map((payment) => {
            const fact = new FactPayment();
            fact.paymentId = payment.paymentId;
            fact.dateKeyPaid = generateKey(payment.paymentDate) as number;
            fact.customerKey = customerKeyMap.get(payment.customerId) as number;
            fact.storeKey = storeKeyMap.get(payment.staff.storeId) as number;
            fact.staffId = payment.staffId;
            fact.amount = payment.amount;
            return fact;
          })
          .filter((f) => f.customerKey && f.storeKey);

        await targetPaymentRepo.insert(newFactPayments);
        console.log(
          `Fact Payment: LOADED (${newFactPayments.length} payments)`
        );
        console.log(chalk.green.bold(`Fact tables loaded.`));

        /* --- initializing sync state --- */
        console.log(chalk.yellow('\n--- Initializing Sync State ---'));
        const syncStateRepo =
          transactionalEntityManager.getRepository(SyncState);
        const now = new Date();

        const tableNames = [
          'actor',
          'category',
          'store',
          'customer',
          'film',
          'dim_date_sync',
          'bridge_film_actor',
          'bridge_film_category',
          'fact_rental',
          'fact_payment',
        ];

        const syncStates: SyncState[] = [];
        for (const tableName of tableNames) {
          const state = new SyncState();
          state.tableName = tableName;
          state.lastRun = now;
          syncStates.push(state);
        }

        await syncStateRepo.save(syncStates);
        console.log(chalk.green('Sync state initialized.'));
      }
    );

    console.log(chalk.green.bold('Data load complete.'));
  } catch (error) {
    console.error(chalk.red.bold('Error during full load:', error));
    process.exit(1);
  } finally {
    /* destroy if we are not in a test environment */
    if (process.env.NODE_ENV !== 'test') {
      if (mysqlDataSource.isInitialized) await mysqlDataSource.destroy();
      if (sqliteDataSource.isInitialized) await sqliteDataSource.destroy();
      console.log(chalk.green('\nDatabase connections closed.'));
    }
  }
};
