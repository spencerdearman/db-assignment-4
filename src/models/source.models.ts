import { 
    Entity, 
    PrimaryGeneratedColumn, 
    PrimaryColumn, 
    Column, 
    UpdateDateColumn, 
    ManyToOne, 
    JoinColumn 
} from "typeorm";

@Entity('actor')
export class Actor {
    @PrimaryGeneratedColumn({ name: 'actor_id' })
    actorId: number;

    @Column({ name: 'first_name' })
    firstName: string;

    @Column({ name: 'last_name' })
    lastName: string;

    @UpdateDateColumn({ name: 'last_update' })
    lastUpdate: Date;
}

@Entity('category')
export class Category {
    @PrimaryGeneratedColumn({ name: 'category_id' })
    categoryId: number;

    @Column()
    name: string;

    @UpdateDateColumn({ name: 'last_update' })
    lastUpdate: Date;
}

@Entity('country')
export class Country {
    @PrimaryGeneratedColumn({ name: 'country_id' })
    countryId: number;

    @Column()
    country: string;

    @UpdateDateColumn({ name: 'last_update' })
    lastUpdate: Date;
}

@Entity('language')
export class Language {
    @PrimaryGeneratedColumn({ name: 'language_id' })
    languageId: number;

    @Column()
    name: string;

    @UpdateDateColumn({ name: 'last_update' })
    lastUpdate: Date;
}

@Entity('staff')
export class Staff {
    @PrimaryGeneratedColumn({ name: 'staff_id' })
    staffId: number;

    @Column({ name: 'first_name' })
    firstName: string;

    @Column({ name: 'last_name' })
    lastName: string;

    @Column({ name: 'store_id' })
    storeId: number; // We'll link this properly later

    @UpdateDateColumn({ name: 'last_update' })
    lastUpdate: Date;
}

// ===================================
// Level 2 Dependencies
// ===================================

@Entity('city')
export class City {
    @PrimaryGeneratedColumn({ name: 'city_id' })
    cityId: number;

    @Column()
    city: string;

    @Column({ name: 'country_id' })
    countryId: number;

    @ManyToOne(() => Country)
    @JoinColumn({ name: 'country_id' })
    country: Country;

    @UpdateDateColumn({ name: 'last_update' })
    lastUpdate: Date;
}

@Entity('film')
export class Film {
    @PrimaryGeneratedColumn({ name: 'film_id' })
    filmId: number;

    @Column()
    title: string;

    @Column()
    description: string;

    @Column({ name: 'release_year' })
    releaseYear: number;

    @Column({ name: 'language_id' })
    languageId: number;

    @ManyToOne(() => Language)
    @JoinColumn({ name: 'language_id' })
    language: Language;

    @Column({ type: 'int', name: 'length', nullable: true })
    length: number;

    @Column()
    rating: string;

    @UpdateDateColumn({ name: 'last_update' })
    lastUpdate: Date;
}

// ===================================
// Level 3 Dependencies
// ===================================

@Entity('address')
export class Address {
    @PrimaryGeneratedColumn({ name: 'address_id' })
    addressId: number;

    @Column({ name: 'address' })
    address: string;

    @Column({ name: 'city_id' })
    cityId: number;

    @ManyToOne(() => City)
    @JoinColumn({ name: 'city_id' })
    city: City;

    @UpdateDateColumn({ name: 'last_update' })
    lastUpdate: Date;
}

@Entity('film_actor')
export class FilmActor {
    @PrimaryColumn({ name: 'actor_id' })
    actorId: number;

    @PrimaryColumn({ name: 'film_id' })
    filmId: number;

    @ManyToOne(() => Actor)
    @JoinColumn({ name: 'actor_id' })
    actor: Actor;

    @ManyToOne(() => Film)
    @JoinColumn({ name: 'film_id' })
    film: Film;

    @UpdateDateColumn({ name: 'last_update' })
    lastUpdate: Date;
}

@Entity('film_category')
export class FilmCategory {
    @PrimaryColumn({ name: 'film_id' })
    filmId: number;

    @PrimaryColumn({ name: 'category_id' })
    categoryId: number;

    @ManyToOne(() => Film)
    @JoinColumn({ name: 'film_id' })
    film: Film;

    @ManyToOne(() => Category)
    @JoinColumn({ name: 'category_id' })
    category: Category;

    @UpdateDateColumn({ name: 'last_update' })
    lastUpdate: Date;
}

// ===================================
// Level 4 Dependencies
// ===================================

@Entity('customer')
export class Customer {
    @PrimaryGeneratedColumn({ name: 'customer_id' })
    customerId: number;

    @Column({ name: 'first_name' })
    firstName: string;

    @Column({ name: 'last_name' })
    lastName: string;

    @Column()
    active: number; // 0 or 1

    @Column({ name: 'address_id' })
    addressId: number;

    @ManyToOne(() => Address)
    @JoinColumn({ name: 'address_id' })
    address: Address;

    @UpdateDateColumn({ name: 'last_update' })
    lastUpdate: Date;
}

@Entity('store')
export class Store {
    @PrimaryGeneratedColumn({ name: 'store_id' })
    storeId: number;

    @Column({ name: 'address_id' })
    addressId: number;

    @ManyToOne(() => Address)
    @JoinColumn({ name: 'address_id' })
    address: Address;

    @UpdateDateColumn({ name: 'last_update' })
    lastUpdate: Date;
}

// ===================================
// Level 5 Dependencies
// ===================================

@Entity('inventory')
export class Inventory {
    @PrimaryGeneratedColumn({ name: 'inventory_id' })
    inventoryId: number;

    @Column({ name: 'film_id' })
    filmId: number;

    @Column({ name: 'store_id' })
    storeId: number;

    @ManyToOne(() => Film)
    @JoinColumn({ name: 'film_id' })
    film: Film;

    @ManyToOne(() => Store)
    @JoinColumn({ name: 'store_id' })
    store: Store;

    @UpdateDateColumn({ name: 'last_update' })
    lastUpdate: Date;
}

// =f==================================
// Level 6 Dependencies
// ===================================

@Entity('payment')
export class Payment {
    @PrimaryGeneratedColumn({ name: 'payment_id' })
    paymentId: number;

    @Column({ name: 'customer_id' })
    customerId: number;

    @Column({ name: 'staff_id' })
    staffId: number;

    @Column({ name: 'rental_id', nullable: true })
    rentalId: number; // Can be null

    @Column({ type: 'decimal', precision: 5, scale: 2 })
    amount: number;

    @Column({ name: 'payment_date' })
    paymentDate: Date;

    @ManyToOne(() => Customer)
    @JoinColumn({ name: 'customer_id' })
    customer: Customer;

    @ManyToOne(() => Staff)
    @JoinColumn({ name: 'staff_id' })
    staff: Staff;

    @UpdateDateColumn({ name: 'last_update' })
    lastUpdate: Date;
}

@Entity('rental')
export class Rental {
    @PrimaryGeneratedColumn({ name: 'rental_id' })
    rentalId: number;

    @Column({ name: 'rental_date' })
    rentalDate: Date;

    @Column({ name: 'inventory_id' })
    inventoryId: number;

    @Column({ name: 'customer_id' })
    customerId: number;

    @Column({ name: 'return_date', nullable: true })
    returnDate: Date;

    @Column({ name: 'staff_id' })
    staffId: number;

    @ManyToOne(() => Inventory)
    @JoinColumn({ name: 'inventory_id' })
    inventory: Inventory;

    @ManyToOne(() => Customer)
    @JoinColumn({ name: 'customer_id' })
    customer: Customer;

    @ManyToOne(() => Staff)
    @JoinColumn({ name: 'staff_id' })
    staff: Staff;

    @UpdateDateColumn({ name: 'last_update' })
    lastUpdate: Date;
}