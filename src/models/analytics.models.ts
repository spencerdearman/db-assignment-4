// src/models/analytics.models.ts
import { 
    Entity, 
    PrimaryGeneratedColumn, 
    PrimaryColumn, 
    Column, 
    Index 
} from "typeorm";

// =================================================================
//  Dimensions
// =================================================================

@Entity('dim_date')
export class DimDate {
    @PrimaryColumn({ name: 'date_key', type: 'int' })
    dateKey: number;

    @Column({ type: 'date' })
    date: string;

    @Column({ type: 'int' })
    year: number;

    @Column({ type: 'int' })
    quarter: number;

    @Column({ type: 'int' })
    month: number;

    @Column({ name: 'day_of_month', type: 'int' })
    dayOfMonth: number;

    @Column({ name: 'day_of_week', type: 'int' })
    dayOfWeek: number;

    @Column({ name: 'is_weekend', type: 'boolean' })
    isWeekend: boolean;
}

@Entity('dim_film')
export class DimFilm {
    @PrimaryGeneratedColumn({ name: 'film_key' })
    filmKey: number;

    @Index() // Index the natural key for fast updates
    @Column({ name: 'film_id' })
    filmId: number;

    @Column()
    title: string;

    @Column({ nullable: true })
    rating: string;

    @Column({ type: 'int', nullable: true })
    length: number;

    @Column({ nullable: true })
    language: string;

    @Column({ name: 'release_year', nullable: true })
    releaseYear: number;

    @Column({ name: 'last_update', type: 'datetime' })
    lastUpdate: Date;
}

@Entity('dim_actor')
export class DimActor {
    @PrimaryGeneratedColumn({ name: 'actor_key' })
    actorKey: number;

    @Index() // Index the natural key
    @Column({ name: 'actor_id' })
    actorId: number;

    @Column({ name: 'first_name' })
    firstName: string;

    @Column({ name: 'last_name' })
    lastName: string;

    @Column({ name: 'last_update', type: 'datetime' })
    lastUpdate: Date;
}

@Entity('dim_category')
export class DimCategory {
    @PrimaryGeneratedColumn({ name: 'category_key' })
    categoryKey: number;

    @Index() // Index the natural key
    @Column({ name: 'category_id' })
    categoryId: number;

    @Column()
    name: string;

    @Column({ name: 'last_update', type: 'datetime' })
    lastUpdate: Date;
}

@Entity('dim_store')
export class DimStore {
    @PrimaryGeneratedColumn({ name: 'store_key' })
    storeKey: number;

    @Index() // Index the natural key
    @Column({ name: 'store_id' })
    storeId: number;

    @Column()
    city: string;

    @Column()
    country: string;

    @Column({ name: 'last_update', type: 'datetime' })
    lastUpdate: Date;
}

@Entity('dim_customer')
export class DimCustomer {
    @PrimaryGeneratedColumn({ name: 'customer_key' })
    customerKey: number;

    @Index() // Index the natural key
    @Column({ name: 'customer_id' })
    customerId: number;

    @Column({ name: 'first_name' })
    firstName: string;

    @Column({ name: 'last_name' })
    lastName: string;

    @Column({ type: 'boolean' })
    active: boolean;

    @Column()
    city: string;

    @Column()
    country: string;

    @Column({ name: 'last_update', type: 'datetime' })
    lastUpdate: Date;
}


// =================================================================
//  Bridge Tables
// =================================================================

@Entity('bridge_film_actor')
export class BridgeFilmActor {
    // This table uses a composite primary key
    @PrimaryColumn({ name: 'film_key' })
    @Index() // Index for joining
    filmKey: number;

    @PrimaryColumn({ name: 'actor_key' })
    @Index() // Index for joining
    actorKey: number;
}

@Entity('bridge_film_category')
export class BridgeFilmCategory {
    // This table also uses a composite primary key
    @PrimaryColumn({ name: 'film_key' })
    @Index() // Index for joining
    filmKey: number;

    @PrimaryColumn({ name: 'category_key' })
    @Index() // Index for joining
    categoryKey: number;
}


// =================================================================
//  Fact Tables
// =================================================================

@Entity('fact_rental')
export class FactRental {
    @PrimaryGeneratedColumn({ name: 'fact_rental_key' })
    factRentalKey: number;

    @Index() // Index the natural key
    @Column({ name: 'rental_id' })
    rentalId: number;

    @Index() // Index for time-series analysis
    @Column({ name: 'date_key_rented', type: 'int' })
    dateKeyRented: number;

    @Index() // Index for time-series analysis
    @Column({ name: 'date_key_returned', type: 'int', nullable: true })
    dateKeyReturned: number;

    @Index() // Index for joins
    @Column({ name: 'film_key' })
    filmKey: number;

    @Index() // Index for joins
    @Column({ name: 'store_key' })
    storeKey: number;

    @Index() // Index for joins
    @Column({ name: 'customer_key' })
    customerKey: number;

    @Column({ name: 'staff_id' })
    staffId: number;

    @Column({ name: 'rental_duration_days', type: 'int', nullable: true })
    rentalDurationDays: number;
}

@Entity('fact_payment')
export class FactPayment {
    @PrimaryGeneratedColumn({ name: 'fact_payment_key' })
    factPaymentKey: number;

    @Index() // Index the natural key
    @Column({ name: 'payment_id' })
    paymentId: number;

    @Index() // Index for time-series analysis
    @Column({ name: 'date_key_paid', type: 'int' })
    dateKeyPaid: number;

    @Index() // Index for joins
    @Column({ name: 'customer_key' })
    customerKey: number;

    @Index() // Index for joins
    @Column({ name: 'store_key' })
    storeKey: number;

    @Column({ name: 'staff_id' })
    staffId: number;

    @Column({ type: 'decimal', precision: 5, scale: 2 })
    amount: number;
}

// =================================================================
//  Sync Control Table (For Incremental)
// =================================================================

@Entity('sync_state')
export class SyncState {
    // The name of the table we are tracking, e.g., 'actor'
    @PrimaryColumn({ name: 'table_name' })
    tableName: string;

    // The last 'last_update' timestamp we saved from that table
    @Column({ name: 'last_run', type: 'datetime' })
    lastRun: Date;
}