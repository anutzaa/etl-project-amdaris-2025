use etlproject;


drop table if exists transform_log;
drop table if exists btc_data_import;
drop table if exists gold_data_import;


create table transform_log(
    Id int auto_increment primary key,
    batch_date date not null,
    currency_id int,
    processed_directory_name varchar(50) not null,
    processed_file_name varchar(50) not null,
    row_count int,
    status varchar(255),
    foreign key (currency_id) references currency(Id) on delete set null
);


create table btc_data_import(
    Id int auto_increment primary key,
    currency_id int,
    date date not null,
    open decimal(16,2) not null,
    high decimal(16,2) not null,
    low decimal(16,2) not null,
    close decimal(16,2) not null,
    volume decimal (20,8) not null,
    foreign key (currency_id) references currency(Id) on delete set null
);

alter table btc_data_import
add unique index idx_currency_date (currency_id, date);


create table gold_data_import(
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
    foreign key (currency_id) references currency(Id) on delete set null
);