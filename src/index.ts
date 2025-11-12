import 'reflect-metadata';
import chalk from 'chalk';
import { program } from 'commander';
import { mysqlDataSource, sqliteDataSource } from './data-sources';
import { EntityManager, MoreThan } from 'typeorm';
import { 
    Actor, Category, Store, Customer, Film, Language, 
    FilmActor, FilmCategory, Rental, Payment, Inventory, Staff 
} from './models/source.models'; 
import { 
    DimActor, DimCategory, DimStore, DimCustomer, DimFilm, 
    BridgeFilmActor, BridgeFilmCategory, FactRental, FactPayment, 
    SyncState, DimDate 
} from './models/analytics.models';

/**
 * Converts a Date object into a YYYYMMDD integer key.
 */
function dateToKey(date: Date | null | undefined): number | null {
    if (!date) return null;
    const d = new Date(date);
    const year = d.getFullYear();
    const month = (d.getMonth() + 1).toString().padStart(2, '0');
    const day = d.getDate().toString().padStart(2, '0');
    return parseInt(`${year}${month}${day}`);
}

/**
 * Creates a new DimDate object from a Date.
 */
function createDimDate(date: Date): DimDate {
    const d = new DimDate();
    const jsDate = new Date(date);
    
    d.dateKey = dateToKey(jsDate) as number;
    d.date = jsDate.toISOString().split('T')[0]!;
    d.year = jsDate.getFullYear();
    d.month = jsDate.getMonth() + 1;
    d.dayOfMonth = jsDate.getDate();
    d.dayOfWeek = jsDate.getDay();
    d.isWeekend = (d.dayOfWeek === 0 || d.dayOfWeek === 6);
    
    const month = d.month;
    if (month <= 3) d.quarter = 1;
    else if (month <= 6) d.quarter = 2;
    else if (month <= 9) d.quarter = 3;
    else d.quarter = 4;
    
    return d;
}

/**
 * Calculates the difference in days between two dates.
 */
function getDaysBetween(startDate: Date | null, endDate: Date | null): number | null {
    if (!startDate || !endDate) return null;
    const start = new Date(startDate);
    const end = new Date(endDate);
    const diffTime = Math.abs(end.getTime() - start.getTime());
    return Math.ceil(diffTime / (1000 * 60 * 60 * 24));
}

/* --- init command --- */
program
    .command('init')
    .description('Initializes the SQLite database and creates all tables')
    .action(async () => {
        try {
            console.log('Initializing analytics database...');
            await sqliteDataSource.initialize();
            await sqliteDataSource.synchronize();
            await sqliteDataSource.destroy();
            console.log(chalk.green.bold('Analytics database initialized.'));
        } catch (error) {
            console.error(chalk.red.bold('Initialization error:', error));
            process.exit(1);
        }
    });

/* --- full-load command --- */
program
    .command('full-load')
    .description('Performs a full load from Sakila (MySQL) to Analytics (SQLite)')
    .action(async () => {
        try {
            console.log('Starting full load command...');
            await mysqlDataSource.initialize();
            await sqliteDataSource.initialize();
            console.log(chalk.green('Databases connected.'));

            await sqliteDataSource.transaction(async (transactionalEntityManager: EntityManager) => {
                /* ---------- loading dimensions ---------- */
                console.log(chalk.yellow('\n--- Loading Dimensions ---'));

                /* -- actors section -- */
                const sourceActorRepo = mysqlDataSource.getRepository(Actor);
                const targetActorRepo = transactionalEntityManager.getRepository(DimActor);
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
                const targetCategoryRepo = transactionalEntityManager.getRepository(DimCategory);
                const categoriesFromSource = await sourceCategoryRepo.find();
                const newDimCategories = categoriesFromSource.map((category: Category) => {
                    const newDimCategory = new DimCategory();
                    newDimCategory.categoryId = category.categoryId;
                    newDimCategory.name = category.name;
                    newDimCategory.lastUpdate = category.lastUpdate;
                    return newDimCategory;
                });
                await targetCategoryRepo.insert(newDimCategories);
                console.log(`Categories: LOADED (${newDimCategories.length} categories)`);

                /* -- stores section -- */
                const sourceStoreRepo = mysqlDataSource.getRepository(Store);
                const targetStoreRepo = transactionalEntityManager.getRepository(DimStore);
                const storesFromSource = await sourceStoreRepo.find({
                    relations: ['address', 'address.city', 'address.city.country']
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
                const targetCustomerRepo = transactionalEntityManager.getRepository(DimCustomer);
                const customersFromSource = await sourceCustomerRepo.find({
                    relations: ['address', 'address.city', 'address.city.country']
                });
                const newDimCustomers = customersFromSource.map((customer: Customer) => {
                    const newDimCustomer = new DimCustomer();
                    newDimCustomer.customerId = customer.customerId;
                    newDimCustomer.firstName = customer.firstName;
                    newDimCustomer.lastName = customer.lastName;
                    newDimCustomer.active = customer.active === 1;
                    newDimCustomer.city = customer.address.city.city;
                    newDimCustomer.country = customer.address.city.country.country;
                    newDimCustomer.lastUpdate = customer.lastUpdate;
                    return newDimCustomer;
                });
                await targetCustomerRepo.insert(newDimCustomers);
                console.log(`Customers: LOADED (${newDimCustomers.length} customers)`);

                /* -- film section -- */
                const sourceFilmRepo = mysqlDataSource.getRepository(Film);
                const targetFilmRepo = transactionalEntityManager.getRepository(DimFilm);
                const filmsFromSource = await sourceFilmRepo.find({
                    relations: ['language']
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
                const paymentDates = await mysqlDataSource.getRepository(Payment).find();
                
                const dateSet = new Set<string>();
                rentalDates.forEach(r => {
                    dateSet.add(r.rentalDate.toISOString().split('T')[0]!);
                    if (r.returnDate) {
                        dateSet.add(r.returnDate.toISOString().split('T')[0]!);
                    }
                });
                paymentDates.forEach(p => {
                    dateSet.add(p.paymentDate.toISOString().split('T')[0]!);
                });

                const dimDates = Array.from(dateSet).map(dateString => {
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
                allDimActors.forEach(a => actorKeyMap.set(a.actorId, a.actorKey));

                const categoryKeyMap = new Map<number, number>();
                allDimCategories.forEach(c => categoryKeyMap.set(c.categoryId, c.categoryKey));

                const storeKeyMap = new Map<number, number>();
                allDimStores.forEach(s => storeKeyMap.set(s.storeId, s.storeKey));

                const customerKeyMap = new Map<number, number>();
                allDimCustomers.forEach(c => customerKeyMap.set(c.customerId, c.customerKey));

                const filmKeyMap = new Map<number, number>();
                allDimFilms.forEach(f => filmKeyMap.set(f.filmId, f.filmKey));

                console.log(chalk.green.bold('Key maps created.'));

                /* ---------- loading bridge tables ---------- */
                console.log(chalk.yellow('\n--- Loading Bridge Tables ---'));
                
                /* -- bridge_film_actor -- */
                const sourceFilmActorRepo = mysqlDataSource.getRepository(FilmActor);
                const targetFilmActorRepo = transactionalEntityManager.getRepository(BridgeFilmActor);
                const filmActorsFromSource = await sourceFilmActorRepo.find();
                
                const newBridgeFilmActors = filmActorsFromSource.map(fa => {
                    const bridge = new BridgeFilmActor();
                    bridge.filmKey = filmKeyMap.get(fa.filmId) as number;
                    bridge.actorKey = actorKeyMap.get(fa.actorId) as number;
                    return bridge;
                }).filter(b => b.filmKey && b.actorKey);
                
                await targetFilmActorRepo.insert(newBridgeFilmActors);
                console.log(`Film-Actor Links: LOADED (${newBridgeFilmActors.length} links)`);
                
                /* -- bridge_film_category -- */
                const sourceFilmCategoryRepo = mysqlDataSource.getRepository(FilmCategory);
                const targetFilmCategoryRepo = transactionalEntityManager.getRepository(BridgeFilmCategory);
                const filmCategoriesFromSource = await sourceFilmCategoryRepo.find();

                const newBridgeFilmCategories = filmCategoriesFromSource.map(fc => {
                    const bridge = new BridgeFilmCategory();
                    bridge.filmKey = filmKeyMap.get(fc.filmId) as number;
                    bridge.categoryKey = categoryKeyMap.get(fc.categoryId) as number;
                    return bridge;
                }).filter(b => b.filmKey && b.categoryKey);
                
                await targetFilmCategoryRepo.insert(newBridgeFilmCategories);
                console.log(`Film-Category Links: LOADED (${newBridgeFilmCategories.length} links)`);
                console.log(chalk.green.bold(`Bridge tables loaded.`));

                /* ---------- loading fact tables ---------- */
                console.log(chalk.yellow('\n--- Loading Fact Tables ---'));

                /* -- fact_rental -- */
                const sourceRentalRepo = mysqlDataSource.getRepository(Rental);
                const targetRentalRepo = transactionalEntityManager.getRepository(FactRental);
                const rentalsFromSource = await sourceRentalRepo.find({
                    relations: ['inventory', 'inventory.store']
                });

                const newFactRentals = rentalsFromSource.map(rental => {
                    const fact = new FactRental();
                    fact.rentalId = rental.rentalId;
                    fact.dateKeyRented = dateToKey(rental.rentalDate) as number;
                    fact.dateKeyReturned = dateToKey(rental.returnDate);
                    
                    /* using maps to get the surrogate keys */
                    fact.customerKey = customerKeyMap.get(rental.customerId) as number;
                    fact.filmKey = filmKeyMap.get(rental.inventory.filmId) as number;
                    fact.storeKey = storeKeyMap.get(rental.inventory.store.storeId) as number;
                    
                    fact.staffId = rental.staffId;
                    fact.rentalDurationDays = getDaysBetween(rental.rentalDate, rental.returnDate);
                    
                    return fact;
                }).filter(f => f.customerKey && f.filmKey && f.storeKey);

                await targetRentalRepo.insert(newFactRentals);
                console.log(`Fact Rental: LOADED (${newFactRentals.length} rentals)`);

                /* -- fact_payment -- */
                const sourcePaymentRepo = mysqlDataSource.getRepository(Payment);
                const targetPaymentRepo = transactionalEntityManager.getRepository(FactPayment);
                const paymentsFromSource = await sourcePaymentRepo.find({
                    relations: ['staff']
                });

                const newFactPayments = paymentsFromSource.map(payment => {
                    const fact = new FactPayment();
                    fact.paymentId = payment.paymentId;
                    fact.dateKeyPaid = dateToKey(payment.paymentDate) as number;
                    fact.customerKey = customerKeyMap.get(payment.customerId) as number;
                    fact.storeKey = storeKeyMap.get(payment.staff.storeId) as number;
                    fact.staffId = payment.staffId;
                    fact.amount = payment.amount;
                    return fact;
                }).filter(f => f.customerKey && f.storeKey);

                await targetPaymentRepo.insert(newFactPayments);
                console.log(`Fact Payment: LOADED (${newFactPayments.length} payments)`);
                console.log(chalk.green.bold(`Fact tables loaded.`));

                /* --- initializing sync state --- */
                console.log(chalk.yellow('\n--- Initializing Sync State ---'));
                const syncStateRepo = transactionalEntityManager.getRepository(SyncState);
                const now = new Date();

                const tableNames = [
                    'actor', 'category', 'store', 'customer', 'film',
                    'dim_date_sync', 'bridge_film_actor', 'bridge_film_category',
                    'fact_rental', 'fact_payment'
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
            });

            console.log(chalk.green.bold('Data load complete.'));

        } catch (error) {
            console.error(chalk.red.bold('Error during full load:', error));
            process.exit(1);
        } finally {
            if (mysqlDataSource.isInitialized) await mysqlDataSource.destroy();
            if (sqliteDataSource.isInitialized) await sqliteDataSource.destroy();
            console.log(chalk.green('\nDatabase connections closed.'));
        }
    });


/* --- incremental command --- */
program
    .command('incremental')
    .description('Loads new or changed data.')
    .action(async () => {
        try {
            console.log('Starting incremental load command...');
            await mysqlDataSource.initialize();
            await sqliteDataSource.initialize();
            console.log(chalk.green('Databases connected.'));

            await sqliteDataSource.transaction(async (transactionalEntityManager: EntityManager) => {
                
                const syncStateRepo = transactionalEntityManager.getRepository(SyncState);

                /* build key maps */
                console.log('Building key maps...');
                
                const actorKeyMap = new Map<number, number>();
                (await transactionalEntityManager.getRepository(DimActor).find()).forEach(a => actorKeyMap.set(a.actorId, a.actorKey));

                const categoryKeyMap = new Map<number, number>();
                (await transactionalEntityManager.getRepository(DimCategory).find()).forEach(c => categoryKeyMap.set(c.categoryId, c.categoryKey));

                const storeKeyMap = new Map<number, number>();
                (await transactionalEntityManager.getRepository(DimStore).find()).forEach(s => storeKeyMap.set(s.storeId, s.storeKey));

                const customerKeyMap = new Map<number, number>();
                (await transactionalEntityManager.getRepository(DimCustomer).find()).forEach(c => customerKeyMap.set(c.customerId, c.customerKey));

                const filmKeyMap = new Map<number, number>();
                (await transactionalEntityManager.getRepository(DimFilm).find()).forEach(f => filmKeyMap.set(f.filmId, f.filmKey));
                
                console.log('Key maps built.');

                /* sync dimensions */
                
                /* -- Sync Actors -- */
                console.log('Syncing actors...');
                const targetActorRepo = transactionalEntityManager.getRepository(DimActor);
                let lastSync = await syncStateRepo.findOneBy({ tableName: 'actor' });
                let lastSyncTime = lastSync ? lastSync.lastRun : new Date(0); 

                const newSourceActors = await mysqlDataSource.getRepository(Actor).find({
                    where: { lastUpdate: MoreThan(lastSyncTime) }
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
                    savedActors.forEach(a => actorKeyMap.set(a.actorId, a.actorKey));
                    console.log(`Synced ${savedActors.length} actors.`);
                } else {
                    console.log('No new actors to sync.');
                }
                await syncStateRepo.save({ tableName: 'actor', lastRun: new Date() });

                /* -- Sync Categories -- */
                console.log('Syncing categories...');
                const targetCategoryRepo = transactionalEntityManager.getRepository(DimCategory);
                lastSync = await syncStateRepo.findOneBy({ tableName: 'category' });
                lastSyncTime = lastSync ? lastSync.lastRun : new Date(0);

                const newSourceCategories = await mysqlDataSource.getRepository(Category).find({
                    where: { lastUpdate: MoreThan(lastSyncTime) }
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
                    const savedCategories = await targetCategoryRepo.save(categoriesToSave);
                    savedCategories.forEach(c => categoryKeyMap.set(c.categoryId, c.categoryKey));
                    console.log(`Synced ${savedCategories.length} categories.`);
                } else {
                    console.log('No new categories to sync.');
                }
                await syncStateRepo.save({ tableName: 'category', lastRun: new Date() });

                /* -- Sync Stores -- */
                console.log('Syncing stores...');
                const targetStoreRepo = transactionalEntityManager.getRepository(DimStore);
                lastSync = await syncStateRepo.findOneBy({ tableName: 'store' });
                lastSyncTime = lastSync ? lastSync.lastRun : new Date(0);

                const newSourceStores = await mysqlDataSource.getRepository(Store).find({
                    where: { lastUpdate: MoreThan(lastSyncTime) },
                    relations: ['address', 'address.city', 'address.city.country']
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
                    savedStores.forEach(s => storeKeyMap.set(s.storeId, s.storeKey));
                    console.log(`Synced ${savedStores.length} stores.`);
                } else {
                    console.log('No new stores to sync.');
                }
                await syncStateRepo.save({ tableName: 'store', lastRun: new Date() });
                
                /* -- Sync Customers -- */
                console.log('Syncing customers...');
                const targetCustomerRepo = transactionalEntityManager.getRepository(DimCustomer);
                lastSync = await syncStateRepo.findOneBy({ tableName: 'customer' });
                lastSyncTime = lastSync ? lastSync.lastRun : new Date(0);

                const newSourceCustomers = await mysqlDataSource.getRepository(Customer).find({
                    where: { lastUpdate: MoreThan(lastSyncTime) },
                    relations: ['address', 'address.city', 'address.city.country']
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
                        newDimCustomer.country = sourceCustomer.address.city.country.country;
                        newDimCustomer.lastUpdate = sourceCustomer.lastUpdate;

                        const existingKey = customerKeyMap.get(sourceCustomer.customerId);
                        if (existingKey) newDimCustomer.customerKey = existingKey;
                        customersToSave.push(newDimCustomer);
                    }
                    const savedCustomers = await targetCustomerRepo.save(customersToSave);
                    savedCustomers.forEach(c => customerKeyMap.set(c.customerId, c.customerKey));
                    console.log(`Synced ${savedCustomers.length} customers.`);
                } else {
                    console.log('No new customers to sync.');
                }
                await syncStateRepo.save({ tableName: 'customer', lastRun: new Date() });

                /* -- Sync Films -- */
                console.log('Syncing films...');
                const targetFilmRepo = transactionalEntityManager.getRepository(DimFilm);
                lastSync = await syncStateRepo.findOneBy({ tableName: 'film' });
                lastSyncTime = lastSync ? lastSync.lastRun : new Date(0);

                const newSourceFilms = await mysqlDataSource.getRepository(Film).find({
                    where: { lastUpdate: MoreThan(lastSyncTime) },
                    relations: ['language']
                });

                if (newSourceFilms.length > 0) {
                    const filmsToSave: DimFilm[] = [];
                    for (const sourceFilm of newSourceFilms) {
                        const newDimFilm = new DimFilm();
                        newDimFilm.filmId = sourceFilm.filmId;
                        newDimFilm.title = sourceFilm.title;
                        newDimFilm.rating = sourceFilm.rating;
                        newDimFilm.length = sourceFilm.length;
                        newDimFilm.language = sourceFilm.language ? sourceFilm.language.name : 'Unknown';
                        newDimFilm.releaseYear = sourceFilm.releaseYear;
                        newDimFilm.lastUpdate = sourceFilm.lastUpdate;
                        
                        const existingKey = filmKeyMap.get(sourceFilm.filmId);
                        if (existingKey) newDimFilm.filmKey = existingKey;
                        filmsToSave.push(newDimFilm);
                    }
                    const savedFilms = await targetFilmRepo.save(filmsToSave);
                    savedFilms.forEach(f => filmKeyMap.set(f.filmId, f.filmKey));
                    console.log(`Synced ${savedFilms.length} films.`);
                } else {
                    console.log('No new films to sync.');
                }
                await syncStateRepo.save({ tableName: 'film', lastRun: new Date() });

                // ==============================================================
                //  STEP 3: SYNC DATE DIMENSION
                // ==============================================================
                console.log('Syncing date dimension...');
                let lastDateSync = await syncStateRepo.findOneBy({ tableName: 'dim_date_sync' });
                let lastDateSyncTime = lastDateSync ? lastDateSync.lastRun : new Date(0);

                const newRentalDates = await mysqlDataSource.getRepository(Rental).find({ where: { lastUpdate: MoreThan(lastDateSyncTime) } });
                const newPaymentDates = await mysqlDataSource.getRepository(Payment).find({ where: { paymentDate: MoreThan(lastDateSyncTime) } });
                
                const existingDateKeys = new Set( (await transactionalEntityManager.getRepository(DimDate).find()).map(d => d.dateKey) );
                const newDateSet = new Set<string>();

                newRentalDates.forEach(r => {
                    newDateSet.add(r.rentalDate.toISOString().split('T')[0]!);
                    if (r.returnDate) {
                        newDateSet.add(r.returnDate.toISOString().split('T')[0]!);
                    }
                });
                newPaymentDates.forEach(p => {
                    newDateSet.add(p.paymentDate.toISOString().split('T')[0]!);
                });
                
                const newDimDates: DimDate[] = [];
                for (const dateString of newDateSet) {
                    const date = new Date(dateString);
                    const key = dateToKey(date)!;
                    if (!existingDateKeys.has(key)) {
                        newDimDates.push(createDimDate(date));
                        existingDateKeys.add(key); // Add to set to prevent duplicates *in this run*
                    }
                }
                
                if(newDimDates.length > 0) {
                    await transactionalEntityManager.getRepository(DimDate).save(newDimDates);
                    console.log(`Loaded ${newDimDates.length} new dates into dim_date.`);
                } else {
                    console.log('No new dates to add.');
                }
                await syncStateRepo.save({ tableName: 'dim_date_sync', lastRun: new Date() });


                // ==============================================================
                //  STEP 4: SYNC BRIDGES & FACTS
                // ==============================================================

                /* -- Sync BridgeFilmActor -- */
                console.log('Syncing bridge_film_actor...');
                const targetFilmActorRepo = transactionalEntityManager.getRepository(BridgeFilmActor);
                lastSync = await syncStateRepo.findOneBy({ tableName: 'bridge_film_actor' });
                lastSyncTime = lastSync ? lastSync.lastRun : new Date(0);
                
                const newSourceFilmActors = await mysqlDataSource.getRepository(FilmActor).find({
                    where: { lastUpdate: MoreThan(lastSyncTime) }
                });

                if (newSourceFilmActors.length > 0) {
                    const bridgeToSave = newSourceFilmActors.map(source => {
                        const bridge = new BridgeFilmActor();
                        bridge.filmKey = filmKeyMap.get(source.filmId)!;
                        bridge.actorKey = actorKeyMap.get(source.actorId)!;
                        return bridge;
                    }).filter(b => b.filmKey && b.actorKey);

                    await targetFilmActorRepo.save(bridgeToSave);
                    console.log(`Synced ${bridgeToSave.length} film-actor links.`);
                } else {
                    console.log('No new film-actor links to sync.');
                }
                await syncStateRepo.save({ tableName: 'bridge_film_actor', lastRun: new Date() });
                
                /* -- Sync BridgeFilmCategory -- */
                console.log('Syncing bridge_film_category...');
                const targetFilmCategoryRepo = transactionalEntityManager.getRepository(BridgeFilmCategory);
                lastSync = await syncStateRepo.findOneBy({ tableName: 'bridge_film_category' });
                lastSyncTime = lastSync ? lastSync.lastRun : new Date(0);
                
                const newSourceFilmCategories = await mysqlDataSource.getRepository(FilmCategory).find({
                    where: { lastUpdate: MoreThan(lastSyncTime) }
                });

                if (newSourceFilmCategories.length > 0) {
                    const bridgeToSave = newSourceFilmCategories.map(source => {
                        const bridge = new BridgeFilmCategory();
                        bridge.filmKey = filmKeyMap.get(source.filmId)!;
                        bridge.categoryKey = categoryKeyMap.get(source.categoryId)!;
                        return bridge;
                    }).filter(b => b.filmKey && b.categoryKey);

                    await targetFilmCategoryRepo.save(bridgeToSave);
                    console.log(`Synced ${bridgeToSave.length} film-category links.`);
                } else {
                    console.log('No new film-category links to sync.');
                }
                await syncStateRepo.save({ tableName: 'bridge_film_category', lastRun: new Date() });


                /* -- Sync FactRental -- */
                console.log('Syncing fact_rental...');
                const targetRentalRepo = transactionalEntityManager.getRepository(FactRental);
                lastSync = await syncStateRepo.findOneBy({ tableName: 'fact_rental' });
                lastSyncTime = lastSync ? lastSync.lastRun : new Date(0);

                const newSourceRentals = await mysqlDataSource.getRepository(Rental).find({
                    where: [
                        { rentalDate: MoreThan(lastSyncTime) },
                        { lastUpdate: MoreThan(lastSyncTime) }
                    ],
                    relations: ['inventory', 'inventory.store']
                });

                if (newSourceRentals.length > 0) {
                    const rentalsToSave: FactRental[] = [];
                    for (const rental of newSourceRentals) {
                        const fact = new FactRental();
                        fact.rentalId = rental.rentalId;
                        fact.dateKeyRented = dateToKey(rental.rentalDate)!;
                        fact.dateKeyReturned = dateToKey(rental.returnDate);
                        
                        fact.customerKey = customerKeyMap.get(rental.customerId)!;
                        fact.filmKey = filmKeyMap.get(rental.inventory.filmId)!;
                        fact.storeKey = storeKeyMap.get(rental.inventory.store.storeId)!;
                        
                        fact.staffId = rental.staffId;
                        fact.rentalDurationDays = getDaysBetween(rental.rentalDate, rental.returnDate);
                        
                        const existingFact = await targetRentalRepo.findOneBy({ rentalId: rental.rentalId });
                        if (existingFact) {
                            fact.factRentalKey = existingFact.factRentalKey;
                        }
                        
                        if (fact.customerKey && fact.filmKey && fact.storeKey) {
                            rentalsToSave.push(fact);
                        } else {
                            console.warn(`Skipping rental ${rental.rentalId}: missing foreign key.`);
                        }
                    }
                    await targetRentalRepo.save(rentalsToSave);
                    console.log(`Synced ${rentalsToSave.length} rentals.`);
                } else {
                    console.log('No new rentals to sync.');
                }
                await syncStateRepo.save({ tableName: 'fact_rental', lastRun: new Date() });

                /* -- Sync FactPayment -- */
                console.log('Syncing fact_payment...');
                const targetPaymentRepo = transactionalEntityManager.getRepository(FactPayment);
                lastSync = await syncStateRepo.findOneBy({ tableName: 'fact_payment' });
                lastSyncTime = lastSync ? lastSync.lastRun : new Date(0);

                // Payments are immutable; we only look for new ones based on payment_date
                const newSourcePayments = await mysqlDataSource.getRepository(Payment).find({
                    where: { paymentDate: MoreThan(lastSyncTime) },
                    relations: ['staff']
                });

                if (newSourcePayments.length > 0) {
                    const paymentsToSave: FactPayment[] = [];
                    for (const payment of newSourcePayments) {
                        const fact = new FactPayment();
                        fact.paymentId = payment.paymentId;
                        fact.dateKeyPaid = dateToKey(payment.paymentDate)!;
                        
                        fact.customerKey = customerKeyMap.get(payment.customerId)!;
                        fact.storeKey = storeKeyMap.get(payment.staff.storeId)!;
                        
                        fact.staffId = payment.staffId;
                        fact.amount = payment.amount;

                        // This is an INSERT-only table, but we'll do the upsert check
                        // just in case the job is re-run without a state-file update
                        const existingFact = await targetPaymentRepo.findOneBy({ paymentId: payment.paymentId });
                        if (existingFact) {
                            fact.factPaymentKey = existingFact.factPaymentKey;
                        }
                        
                        if (fact.customerKey && fact.storeKey) {
                            paymentsToSave.push(fact);
                        } else {
                            console.warn(`Skipping payment ${payment.paymentId}: missing foreign key.`);
                        }
                    }
                    await targetPaymentRepo.save(paymentsToSave);
                    console.log(`Synced ${paymentsToSave.length} payments.`);
                } else {
                    console.log('No new payments to sync.');
                }
                await syncStateRepo.save({ tableName: 'fact_payment', lastRun: new Date() });
            });
            
            console.log('Incremental load complete.');

        } catch (error) {
            console.error('Error during incremental load:', error);
            process.exit(1);
        } finally {
            if (mysqlDataSource.isInitialized) await mysqlDataSource.destroy();
            if (sqliteDataSource.isInitialized) await sqliteDataSource.destroy();
            console.log('Database connections closed.');
        }
    });


/* --- validate command --- */
program
    .command('validate')
    .description('Verifies data consistency between source and target')
    .action(async () => {
        console.log(chalk.cyan('Starting validation command...'));

        // Define the time window
        const thirtyDaysAgo = new Date();
        thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
        // Format for SQLite's 'YYYY-MM-DD' date strings
        const thirtyDaysAgoString = thirtyDaysAgo.toISOString().split('T')[0]!;

        let errorsFound = 0;

        try {
            await mysqlDataSource.initialize();
            await sqliteDataSource.initialize();
            console.log(chalk.green('Databases connected.'));

            console.log(chalk.yellow(`\n--- Validating Data Since ${thirtyDaysAgoString} ---`));

            // ===================================
            //  1. Validate Payment Count
            // ===================================
            const sourcePaymentCount = await mysqlDataSource
                .getRepository(Payment)
                .createQueryBuilder('payment')
                .where('payment.paymentDate > :date', { date: thirtyDaysAgo })
                .getCount();
            
            const targetPaymentCount = await sqliteDataSource
                .getRepository(FactPayment)
                .createQueryBuilder('fact_payment')
                .innerJoin(DimDate, 'dim_date', 'fact_payment.dateKeyPaid = dim_date.dateKey')
                .where('dim_date.date > :date', { date: thirtyDaysAgoString })
                .getCount();

            if (sourcePaymentCount === targetPaymentCount) {
                console.log(chalk.green(`Payment Count:  PASSED (${sourcePaymentCount})`));
            } else {
                console.log(chalk.red(`Payment Count:  FAILED (Source: ${sourcePaymentCount}, Target: ${targetPaymentCount})`));
                errorsFound++;
            }

            // ===================================
            //  2. Validate Payment Sum
            // ===================================
            const sourcePaymentSum = (await mysqlDataSource
                .getRepository(Payment)
                .createQueryBuilder('payment')
                .select('SUM(payment.amount)', 'sum')
                .where('payment.paymentDate > :date', { date: thirtyDaysAgo })
                .getRawOne()).sum;
            
            const targetPaymentSum = (await sqliteDataSource
                .getRepository(FactPayment)
                .createQueryBuilder('fact_payment')
                .innerJoin(DimDate, 'dim_date', 'fact_payment.dateKeyPaid = dim_date.dateKey')
                .select('SUM(fact_payment.amount)', 'sum')
                .where('dim_date.date > :date', { date: thirtyDaysAgoString })
                .getRawOne()).sum;
            
            // MySQL returns string, SQLite returns number. Standardize to 2 decimal places.
            const sourceSumFloat = parseFloat(sourcePaymentSum).toFixed(2);
            const targetSumFloat = parseFloat(targetPaymentSum).toFixed(2);

            if (sourceSumFloat === targetSumFloat) {
                console.log(chalk.green(`Payment Total:  PASSED ($${sourceSumFloat})`));
            } else {
                console.log(chalk.red(`Payment Total:  FAILED (Source: $${sourceSumFloat}, Target: $${targetSumFloat})`));
                errorsFound++;
            }

            // ===================================
            //  3. Validate Rental Count
            // ===================================
            const sourceRentalCount = await mysqlDataSource
                .getRepository(Rental)
                .createQueryBuilder('rental')
                .where('rental.rentalDate > :date', { date: thirtyDaysAgo })
                .getCount();

            const targetRentalCount = await sqliteDataSource
                .getRepository(FactRental)
                .createQueryBuilder('fact_rental')
                .innerJoin(DimDate, 'dim_date', 'fact_rental.dateKeyRented = dim_date.dateKey')
                .where('dim_date.date > :date', { date: thirtyDaysAgoString })
                .getCount();
            
            if (sourceRentalCount === targetRentalCount) {
                console.log(chalk.green(`Rental Count:   PASSED (${sourceRentalCount})`));
            } else {
                console.log(chalk.red(`Rental Count:   FAILED (Source: ${sourceRentalCount}, Target: ${targetRentalCount})`));
                errorsFound++;
            }

            // ===================================
            //  4. Validate Revenue by Store
            // ===================================
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
                .innerJoin(DimStore, 'dim_store', 'fact_payment.storeKey = dim_store.storeKey')
                .innerJoin(DimDate, 'dim_date', 'fact_payment.dateKeyPaid = dim_date.dateKey')
                .select('dim_store.storeId', 'store_id')
                .addSelect('SUM(fact_payment.amount)', 'sum')
                .where('dim_date.date > :date', { date: thirtyDaysAgoString })
                .groupBy('dim_store.storeId')
                .orderBy('dim_store.storeId')
                .getRawMany();

            // Log tables for visual comparison
            console.log(chalk.gray('Source (MySQL):'));
            console.table(sourceStoreRevenue.map(r => ({ store_id: r.store_id, sum: parseFloat(r.sum).toFixed(2) })));
            console.log(chalk.gray('Target (SQLite):'));
            console.table(targetStoreRevenue.map(r => ({ store_id: r.store_id, sum: parseFloat(r.sum).toFixed(2) })));

            // Programmatic check
            let storeMismatch = false;
            if (sourceStoreRevenue.length !== targetStoreRevenue.length) {
                storeMismatch = true;
            } else {
                for (const sourceStore of sourceStoreRevenue) {
                    const targetStore = targetStoreRevenue.find(t => t.store_id === sourceStore.store_id);
                    if (!targetStore || parseFloat(sourceStore.sum).toFixed(2) !== parseFloat(targetStore.sum).toFixed(2)) {
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

            // ===================================
            //  Final Report
            // ===================================
            if (errorsFound === 0) {
                console.log(chalk.green.bold('\n\nValidation PASSED. Data is consistent.'));
            } else {
                console.log(chalk.red.bold(`\n\nValidation FAILED. Found ${errorsFound} inconsistencies.`));
            }


        } catch (error) {
            console.error(chalk.red('Error during validation:'), error);
            process.exit(1);
        } finally {
            if (mysqlDataSource.isInitialized) await mysqlDataSource.destroy();
            if (sqliteDataSource.isInitialized) await sqliteDataSource.destroy();
            console.log('\nDatabase connections closed.');
        }
    });

program.parse(process.argv);
