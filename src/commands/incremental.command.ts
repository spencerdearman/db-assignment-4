import chalk from "chalk";
import { mysqlDataSource, sqliteDataSource } from "../data-sources";
import { EntityManager, MoreThan } from "typeorm";
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
} from "../models/source.models";
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
} from "../models/analytics.models";
import { generateKey, createDimDate, duration } from "../utils/date.utils";

export const incrementalCommand = async () => {
  try {
    console.log("Starting incremental load command...");
    if (!mysqlDataSource.isInitialized) await mysqlDataSource.initialize();
    if (!sqliteDataSource.isInitialized) await sqliteDataSource.initialize();
    console.log(chalk.green("Databases connected."));

    await sqliteDataSource.transaction(
      async (transactionalEntityManager: EntityManager) => {
        const syncStateRepo =
          transactionalEntityManager.getRepository(SyncState);

        /* build key maps */
        console.log("Building key maps...");

        const actorKeyMap = new Map<number, number>();
        (
          await transactionalEntityManager.getRepository(DimActor).find()
        ).forEach((a) => actorKeyMap.set(a.actorId, a.actorKey));

        const categoryKeyMap = new Map<number, number>();
        (
          await transactionalEntityManager.getRepository(DimCategory).find()
        ).forEach((c) => categoryKeyMap.set(c.categoryId, c.categoryKey));

        const storeKeyMap = new Map<number, number>();
        (
          await transactionalEntityManager.getRepository(DimStore).find()
        ).forEach((s) => storeKeyMap.set(s.storeId, s.storeKey));

        const customerKeyMap = new Map<number, number>();
        (
          await transactionalEntityManager.getRepository(DimCustomer).find()
        ).forEach((c) => customerKeyMap.set(c.customerId, c.customerKey));

        const filmKeyMap = new Map<number, number>();
        (
          await transactionalEntityManager.getRepository(DimFilm).find()
        ).forEach((f) => filmKeyMap.set(f.filmId, f.filmKey));

        console.log("Key maps built.");

        /* sync dimensions */

        /* -- Sync Actors -- */
        console.log("Syncing actors...");
        const targetActorRepo =
          transactionalEntityManager.getRepository(DimActor);
        let lastSync = await syncStateRepo.findOneBy({ tableName: "actor" });
        let lastSyncTime = lastSync ? lastSync.lastRun : new Date(0);

        const newSourceActors = await mysqlDataSource
          .getRepository(Actor)
          .find({
            where: { lastUpdate: MoreThan(lastSyncTime) },
          });

        if (newSourceActors.length > 0) {
          const actorsToSave: DimActor[] = [];
          for (const sourceActor of newSourceActors) {
            const dimActor = new DimActor();
            dimActor.actorId = sourceActor.actorId;
            dimActor.firstName = sourceActor.firstName;
            dimActor.lastName = sourceActor.lastName;
            dimActor.lastUpdate = sourceActor.lastUpdate;

            const existingKey = actorKeyMap.get(sourceActor.actorId);
            if (existingKey) dimActor.actorKey = existingKey;
            actorsToSave.push(dimActor);
          }
          const savedActors = await targetActorRepo.save(actorsToSave);
          savedActors.forEach((a) => actorKeyMap.set(a.actorId, a.actorKey));
          console.log(`Synced ${savedActors.length} actors.`);
        } else {
          console.log("No new actors to sync.");
        }
        await syncStateRepo.save({ tableName: "actor", lastRun: new Date() });

        /* -- Sync Categories -- */
        console.log("Syncing categories...");
        const targetCategoryRepo =
          transactionalEntityManager.getRepository(DimCategory);
        lastSync = await syncStateRepo.findOneBy({ tableName: "category" });
        lastSyncTime = lastSync ? lastSync.lastRun : new Date(0);

        const newSourceCategories = await mysqlDataSource
          .getRepository(Category)
          .find({
            where: { lastUpdate: MoreThan(lastSyncTime) },
          });

        if (newSourceCategories.length > 0) {
          const categoriesToSave: DimCategory[] = [];
          for (const sourceCategory of newSourceCategories) {
            const newDimCategory = new DimCategory();
            newDimCategory.categoryId = sourceCategory.categoryId;
            newDimCategory.name = sourceCategory.name;
            newDimCategory.lastUpdate = sourceCategory.lastUpdate;

            const existingKey = categoryKeyMap.get(sourceCategory.categoryId);
            if (existingKey) newDimCategory.categoryKey = existingKey;
            categoriesToSave.push(newDimCategory);
          }
          const savedCategories = await targetCategoryRepo.save(
            categoriesToSave
          );
          savedCategories.forEach((c) =>
            categoryKeyMap.set(c.categoryId, c.categoryKey)
          );
          console.log(`Synced ${savedCategories.length} categories.`);
        } else {
          console.log("No new categories to sync.");
        }
        await syncStateRepo.save({
          tableName: "category",
          lastRun: new Date(),
        });

        /* -- Sync Stores -- */
        console.log("Syncing stores...");
        const targetStoreRepo =
          transactionalEntityManager.getRepository(DimStore);
        lastSync = await syncStateRepo.findOneBy({ tableName: "store" });
        lastSyncTime = lastSync ? lastSync.lastRun : new Date(0);

        const newSourceStores = await mysqlDataSource
          .getRepository(Store)
          .find({
            where: { lastUpdate: MoreThan(lastSyncTime) },
            relations: ["address", "address.city", "address.city.country"],
          });

        if (newSourceStores.length > 0) {
          const storesToSave: DimStore[] = [];
          for (const sourceStore of newSourceStores) {
            const newDimStore = new DimStore();
            newDimStore.storeId = sourceStore.storeId;
            newDimStore.city = sourceStore.address.city.city;
            newDimStore.country = sourceStore.address.city.country.country;
            newDimStore.lastUpdate = sourceStore.lastUpdate;

            const existingKey = storeKeyMap.get(sourceStore.storeId);
            if (existingKey) newDimStore.storeKey = existingKey;
            storesToSave.push(newDimStore);
          }
          const savedStores = await targetStoreRepo.save(storesToSave);
          savedStores.forEach((s) => storeKeyMap.set(s.storeId, s.storeKey));
          console.log(`Synced ${savedStores.length} stores.`);
        } else {
          console.log("No new stores to sync.");
        }
        await syncStateRepo.save({ tableName: "store", lastRun: new Date() });

        /* -- Sync Customers -- */
        console.log("Syncing customers...");
        const targetCustomerRepo =
          transactionalEntityManager.getRepository(DimCustomer);
        lastSync = await syncStateRepo.findOneBy({ tableName: "customer" });
        lastSyncTime = lastSync ? lastSync.lastRun : new Date(0);

        const newSourceCustomers = await mysqlDataSource
          .getRepository(Customer)
          .find({
            where: { lastUpdate: MoreThan(lastSyncTime) },
            relations: ["address", "address.city", "address.city.country"],
          });

        if (newSourceCustomers.length > 0) {
          const customersToSave: DimCustomer[] = [];
          for (const sourceCustomer of newSourceCustomers) {
            const newDimCustomer = new DimCustomer();
            newDimCustomer.customerId = sourceCustomer.customerId;
            newDimCustomer.firstName = sourceCustomer.firstName;
            newDimCustomer.lastName = sourceCustomer.lastName;
            newDimCustomer.active = sourceCustomer.active === 1;
            newDimCustomer.city = sourceCustomer.address.city.city;
            newDimCustomer.country =
              sourceCustomer.address.city.country.country;
            newDimCustomer.lastUpdate = sourceCustomer.lastUpdate;

            const existingKey = customerKeyMap.get(sourceCustomer.customerId);
            if (existingKey) newDimCustomer.customerKey = existingKey;
            customersToSave.push(newDimCustomer);
          }
          const savedCustomers = await targetCustomerRepo.save(customersToSave);
          savedCustomers.forEach((c) =>
            customerKeyMap.set(c.customerId, c.customerKey)
          );
          console.log(`Synced ${savedCustomers.length} customers.`);
        } else {
          console.log("No new customers to sync.");
        }
        await syncStateRepo.save({
          tableName: "customer",
          lastRun: new Date(),
        });

        /* -- Sync Films -- */
        console.log("Syncing films...");
        const targetFilmRepo =
          transactionalEntityManager.getRepository(DimFilm);
        lastSync = await syncStateRepo.findOneBy({ tableName: "film" });
        lastSyncTime = lastSync ? lastSync.lastRun : new Date(0);

        const newSourceFilms = await mysqlDataSource.getRepository(Film).find({
          where: { lastUpdate: MoreThan(lastSyncTime) },
          relations: ["language"],
        });

        if (newSourceFilms.length > 0) {
          const filmsToSave: DimFilm[] = [];
          for (const sourceFilm of newSourceFilms) {
            const newDimFilm = new DimFilm();
            newDimFilm.filmId = sourceFilm.filmId;
            newDimFilm.title = sourceFilm.title;
            newDimFilm.rating = sourceFilm.rating;
            newDimFilm.length = sourceFilm.length;
            newDimFilm.language = sourceFilm.language
              ? sourceFilm.language.name
              : "Unknown";
            newDimFilm.releaseYear = sourceFilm.releaseYear;
            newDimFilm.lastUpdate = sourceFilm.lastUpdate;

            const existingKey = filmKeyMap.get(sourceFilm.filmId);
            if (existingKey) newDimFilm.filmKey = existingKey;
            filmsToSave.push(newDimFilm);
          }
          const savedFilms = await targetFilmRepo.save(filmsToSave);
          savedFilms.forEach((f) => filmKeyMap.set(f.filmId, f.filmKey));
          console.log(`Synced ${savedFilms.length} films.`);
        } else {
          console.log("No new films to sync.");
        }
        await syncStateRepo.save({ tableName: "film", lastRun: new Date() });

        /* sync dimensions */
        console.log("Syncing date dimension...");
        let lastDateSync = await syncStateRepo.findOneBy({
          tableName: "dim_date_sync",
        });
        let lastDateSyncTime = lastDateSync
          ? lastDateSync.lastRun
          : new Date(0);

        const newRentalDates = await mysqlDataSource
          .getRepository(Rental)
          .find({ where: { lastUpdate: MoreThan(lastDateSyncTime) } });
        const newPaymentDates = await mysqlDataSource
          .getRepository(Payment)
          .find({ where: { paymentDate: MoreThan(lastDateSyncTime) } });

        const existingDateKeys = new Set(
          (await transactionalEntityManager.getRepository(DimDate).find()).map(
            (d) => d.dateKey
          )
        );
        const newDateSet = new Set<string>();

        newRentalDates.forEach((r) => {
          newDateSet.add(r.rentalDate.toISOString().split("T")[0]!);
          if (r.returnDate) {
            newDateSet.add(r.returnDate.toISOString().split("T")[0]!);
          }
        });
        newPaymentDates.forEach((p) => {
          newDateSet.add(p.paymentDate.toISOString().split("T")[0]!);
        });

        const newDimDates: DimDate[] = [];
        for (const dateString of newDateSet) {
          const date = new Date(dateString);
          const key = generateKey(date)!;
          if (!existingDateKeys.has(key)) {
            newDimDates.push(createDimDate(date));
            existingDateKeys.add(key);
          }
        }

        if (newDimDates.length > 0) {
          await transactionalEntityManager
            .getRepository(DimDate)
            .save(newDimDates);
          console.log(`Loaded ${newDimDates.length} new dates into dim_date.`);
        } else {
          console.log("No new dates to add.");
        }
        await syncStateRepo.save({
          tableName: "dim_date_sync",
          lastRun: new Date(),
        });

        /* sync bridges and facts */

        /* -- Sync BridgeFilmActor -- */
        console.log("Syncing bridge_film_actor...");
        const targetFilmActorRepo =
          transactionalEntityManager.getRepository(BridgeFilmActor);
        lastSync = await syncStateRepo.findOneBy({
          tableName: "bridge_film_actor",
        });
        lastSyncTime = lastSync ? lastSync.lastRun : new Date(0);

        const newSourceFilmActors = await mysqlDataSource
          .getRepository(FilmActor)
          .find({
            where: { lastUpdate: MoreThan(lastSyncTime) },
          });

        if (newSourceFilmActors.length > 0) {
          const bridgeToSave = newSourceFilmActors
            .map((source) => {
              const bridge = new BridgeFilmActor();
              bridge.filmKey = filmKeyMap.get(source.filmId)!;
              bridge.actorKey = actorKeyMap.get(source.actorId)!;
              return bridge;
            })
            .filter((b) => b.filmKey && b.actorKey);

          await targetFilmActorRepo.save(bridgeToSave);
          console.log(`Synced ${bridgeToSave.length} film-actor links.`);
        } else {
          console.log("No new film-actor links to sync.");
        }
        await syncStateRepo.save({
          tableName: "bridge_film_actor",
          lastRun: new Date(),
        });

        /* -- Sync BridgeFilmCategory -- */
        console.log("Syncing bridge_film_category...");
        const targetFilmCategoryRepo =
          transactionalEntityManager.getRepository(BridgeFilmCategory);
        lastSync = await syncStateRepo.findOneBy({
          tableName: "bridge_film_category",
        });
        lastSyncTime = lastSync ? lastSync.lastRun : new Date(0);

        const newSourceFilmCategories = await mysqlDataSource
          .getRepository(FilmCategory)
          .find({
            where: { lastUpdate: MoreThan(lastSyncTime) },
          });

        if (newSourceFilmCategories.length > 0) {
          const bridgeToSave = newSourceFilmCategories
            .map((source) => {
              const bridge = new BridgeFilmCategory();
              bridge.filmKey = filmKeyMap.get(source.filmId)!;
              bridge.categoryKey = categoryKeyMap.get(source.categoryId)!;
              return bridge;
            })
            .filter((b) => b.filmKey && b.categoryKey);

          await targetFilmCategoryRepo.save(bridgeToSave);
          console.log(`Synced ${bridgeToSave.length} film-category links.`);
        } else {
          console.log("No new film-category links to sync.");
        }
        await syncStateRepo.save({
          tableName: "bridge_film_category",
          lastRun: new Date(),
        });

        /* -- Sync FactRental -- */
        console.log("Syncing fact_rental...");
        const targetRentalRepo =
          transactionalEntityManager.getRepository(FactRental);
        lastSync = await syncStateRepo.findOneBy({ tableName: "fact_rental" });
        lastSyncTime = lastSync ? lastSync.lastRun : new Date(0);

        const newSourceRentals = await mysqlDataSource
          .getRepository(Rental)
          .find({
            where: [
              { rentalDate: MoreThan(lastSyncTime) },
              { lastUpdate: MoreThan(lastSyncTime) },
            ],
            relations: ["inventory", "inventory.store"],
          });

        if (newSourceRentals.length > 0) {
          const rentalsToSave: FactRental[] = [];
          for (const rental of newSourceRentals) {
            const fact = new FactRental();
            fact.rentalId = rental.rentalId;
            fact.dateKeyRented = generateKey(rental.rentalDate)!;
            fact.dateKeyReturned = generateKey(rental.returnDate);

            fact.customerKey = customerKeyMap.get(rental.customerId)!;
            fact.filmKey = filmKeyMap.get(rental.inventory.filmId)!;
            fact.storeKey = storeKeyMap.get(rental.inventory.store.storeId)!;

            fact.staffId = rental.staffId;
            fact.rentalDurationDays = duration(
              rental.rentalDate,
              rental.returnDate
            );

            const existingFact = await targetRentalRepo.findOneBy({
              rentalId: rental.rentalId,
            });
            if (existingFact) {
              fact.factRentalKey = existingFact.factRentalKey;
            }

            if (fact.customerKey && fact.filmKey && fact.storeKey) {
              rentalsToSave.push(fact);
            } else {
              console.warn(
                `Skipping rental ${rental.rentalId}: missing foreign key.`
              );
            }
          }
          await targetRentalRepo.save(rentalsToSave);
          console.log(`Synced ${rentalsToSave.length} rentals.`);
        } else {
          console.log("No new rentals to sync.");
        }
        await syncStateRepo.save({
          tableName: "fact_rental",
          lastRun: new Date(),
        });

        /* -- Sync FactPayment -- */
        console.log("Syncing fact_payment...");
        const targetPaymentRepo =
          transactionalEntityManager.getRepository(FactPayment);
        lastSync = await syncStateRepo.findOneBy({ tableName: "fact_payment" });
        lastSyncTime = lastSync ? lastSync.lastRun : new Date(0);

        /* find new payments based on payment_date */
        const newSourcePayments = await mysqlDataSource
          .getRepository(Payment)
          .find({
            where: { paymentDate: MoreThan(lastSyncTime) },
            relations: ["staff"],
          });

        if (newSourcePayments.length > 0) {
          const paymentsToSave: FactPayment[] = [];
          for (const payment of newSourcePayments) {
            const fact = new FactPayment();
            fact.paymentId = payment.paymentId;
            fact.dateKeyPaid = generateKey(payment.paymentDate)!;

            fact.customerKey = customerKeyMap.get(payment.customerId)!;
            fact.storeKey = storeKeyMap.get(payment.staff.storeId)!;

            fact.staffId = payment.staffId;
            fact.amount = payment.amount;

            const existingFact = await targetPaymentRepo.findOneBy({
              paymentId: payment.paymentId,
            });
            if (existingFact) {
              fact.factPaymentKey = existingFact.factPaymentKey;
            }

            if (fact.customerKey && fact.storeKey) {
              paymentsToSave.push(fact);
            } else {
              console.warn(
                `Skipping payment ${payment.paymentId}: missing foreign key.`
              );
            }
          }
          await targetPaymentRepo.save(paymentsToSave);
          console.log(`Synced ${paymentsToSave.length} payments.`);
        } else {
          console.log("No new payments to sync.");
        }
        await syncStateRepo.save({
          tableName: "fact_payment",
          lastRun: new Date(),
        });
      }
    );

    console.log("Incremental load complete.");
  } catch (error) {
    console.error("Error during incremental load:", error);
    process.exit(1);
  } finally {
    /* destroy if not in a test environment */
    if (process.env.NODE_ENV !== "test") {
      if (mysqlDataSource.isInitialized) await mysqlDataSource.destroy();
      if (sqliteDataSource.isInitialized) await sqliteDataSource.destroy();
      console.log(chalk.green("\nDatabase connections closed."));
    }
  }
};
