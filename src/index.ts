import 'reflect-metadata';
import { program } from 'commander';
import { mysqlDataSource, sqliteDataSource } from './data-sources';
import { EntityManager } from 'typeorm'; // Import EntityManager for typing

// Import the specific models we need to reference
import { Actor } from './models/source.models'; 
import { DimActor } from './models/analytics.models';

// =================================================================
//  COMMAND: init
// =================================================================
program
    .command('init')
    .description('Initializes the SQLite database and creates all tables')
    .action(async () => {
        try {
            console.log('Initializing analytics database...');
            await sqliteDataSource.initialize();
            
            // synchronize() builds all the tables
            await sqliteDataSource.synchronize(); 
            
            await sqliteDataSource.destroy();
            console.log('✅ Analytics database initialized successfully.');
        } catch (error) {
            console.error('❌ Error during init:', error);
            process.exit(1);
        }
    });

// =================================================================
//  COMMAND: full-load
// =================================================================
program
    .command('full-load')
    .description('Performs a full load from Sakila (MySQL) to Analytics (SQLite)')
    .action(async () => {
        try {
            console.log('Starting full data load...');
            
            await mysqlDataSource.initialize();
            await sqliteDataSource.initialize();
            console.log('Databases connected.');

            // Use the explicit type for the transaction manager
            await sqliteDataSource.transaction(async (transactionalEntityManager: EntityManager) => {
                
                // --- Load Actors ---
                console.log('Loading actors...');
                
                // [E] EXTRACT from MySQL
                const sourceActorRepo = mysqlDataSource.getRepository(Actor);
                const actorsFromSource = await sourceActorRepo.find();

                // [T] TRANSFORM to analytics shape
                // Add the explicit 'Actor' type to the 'actor' parameter
                const newDimActors = actorsFromSource.map((actor: Actor) => {
                    const newDimActor = new DimActor();
                    // FIX: Use camelCase properties (actorId), not snake_case (actor_id)
                    newDimActor.actorId = actor.actorId;
                    newDimActor.firstName = actor.firstName;
                    newDimActor.lastName = actor.lastName;
                    newDimActor.lastUpdate = actor.lastUpdate;
                    return newDimActor;
                });

                // [L] LOAD into SQLite
                const targetActorRepo = transactionalEntityManager.getRepository(DimActor);
                await targetActorRepo.save(newDimActors);
                console.log(`Loaded ${newDimActors.length} actors.`);

                // --- Load Categories (Your next task) ---
                // ...
            });

            console.log('✅ Full data load complete.');

        } catch (error) {
            console.error('❌ Error during full load:', error);
            process.exit(1);
        } finally {
            if (mysqlDataSource.isInitialized) await mysqlDataSource.destroy();
            if (sqliteDataSource.isInitialized) await sqliteDataSource.destroy();
            console.log('Database connections closed.');
        }
    });

// This line starts the commander program
program.parse(process.argv);

