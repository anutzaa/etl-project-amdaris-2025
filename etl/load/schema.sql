use etlproject;


drop table if exists fact_btc;
drop table if exists fact_gold;
drop table if exists fact_exchange_rates;
drop table if exists dim_date;


create table fact_btc(
    Id int auto_increment primary key,
    date date not null,
    currency_id int,
    open decimal(16,2) not null,
    high decimal(16,2) not null,
    low decimal(16,2) not null,
    close decimal(16,2) not null,
    volume decimal (20,8) not null,
    created_at timestamp(4) not null,
    updated_at timestamp(4) not null,
    foreign key (currency_id) references currency(Id) on delete set null,
    unique index idx_currency_date (currency_id, date)
);


create table fact_gold(
    Id int auto_increment primary key,
    currency_id int,
    date date not null,
    open decimal(16,2) not null,
    high decimal(16,2) not null,
    low decimal(16,2) not null,
    price decimal(16,2) not null,
    price_24k decimal(16,8),
    price_18k decimal(16,8),
    price_14k decimal(16,8),
    created_at timestamp(4) not null,
    updated_at timestamp(4) not null,
    foreign key (currency_id) references currency(Id) on delete set null,
    unique index idx_currency_date (currency_id, date)
);


create table fact_exchange_rates(
    Id int auto_increment primary key,
    date date not null,
    base_currency_id int not null,
    target_currency_id int not null,
    rate decimal(6,5) not null,
    created_at timestamp(4) not null,
    updated_at timestamp(4) not null,
    foreign key (base_currency_id) references currency(Id),
    foreign key (target_currency_id) references currency(Id),
    unique index idx_currency_date (base_currency_id, target_currency_id, date)
);


create table dim_date(
    date date primary key,
    day int not null,
    month int not null,
    month_name varchar(15) not null,
    quarter int not null,
    year int not null,
    day_of_week int not null,
    week_of_year int not null,
    is_weekend boolean not null
);