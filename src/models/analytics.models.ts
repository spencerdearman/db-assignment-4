import {
  Entity,
  PrimaryGeneratedColumn,
  PrimaryColumn,
  Column,
  Index,
} from 'typeorm';

/* dimensions */
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

  @Index()
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

  @Index()
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

  @Index()
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

  @Index()
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

  @Index()
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

/* bridge tables */
@Entity('bridge_film_actor')
export class BridgeFilmActor {
  @PrimaryColumn({ name: 'film_key' })
  @Index()
  filmKey: number;

  @PrimaryColumn({ name: 'actor_key' })
  @Index()
  actorKey: number;
}

@Entity('bridge_film_category')
export class BridgeFilmCategory {
  @PrimaryColumn({ name: 'film_key' })
  @Index()
  filmKey: number;

  @PrimaryColumn({ name: 'category_key' })
  @Index()
  categoryKey: number;
}

/* fact tables */
@Entity('fact_rental')
export class FactRental {
  @PrimaryGeneratedColumn({ name: 'fact_rental_key' })
  factRentalKey: number;

  @Index()
  @Column({ name: 'rental_id' })
  rentalId: number;

  @Index()
  @Column({ name: 'date_key_rented', type: 'int' })
  dateKeyRented: number;

  @Index()
  @Column({ name: 'date_key_returned', type: 'int', nullable: true })
  dateKeyReturned: number | null;

  @Index()
  @Column({ name: 'film_key' })
  filmKey: number;

  @Index()
  @Column({ name: 'store_key' })
  storeKey: number;

  @Index()
  @Column({ name: 'customer_key' })
  customerKey: number;

  @Column({ name: 'staff_id' })
  staffId: number;

  @Column({ name: 'rental_duration_days', type: 'int', nullable: true })
  rentalDurationDays: number | null;
}

@Entity('fact_payment')
export class FactPayment {
  @PrimaryGeneratedColumn({ name: 'fact_payment_key' })
  factPaymentKey: number;

  @Index()
  @Column({ name: 'payment_id' })
  paymentId: number;

  @Index()
  @Column({ name: 'date_key_paid', type: 'int' })
  dateKeyPaid: number;

  @Index()
  @Column({ name: 'customer_key' })
  customerKey: number;

  @Index()
  @Column({ name: 'store_key' })
  storeKey: number;

  @Column({ name: 'staff_id' })
  staffId: number;

  @Column({ type: 'decimal', precision: 5, scale: 2 })
  amount: number;
}

/* sync control table */
@Entity('sync_state')
export class SyncState {
  @PrimaryColumn({ name: 'table_name' })
  tableName: string;

  @Column({ name: 'last_run', type: 'datetime' })
  lastRun: Date;
}
