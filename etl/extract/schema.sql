create database etlproject;


create table currency (
    Id int auto_increment primary key,
    code varchar(3) unique not null,
    name varchar(50) unique not null
);


insert into currency(code, name)
values('EUR', 'Euro'),
      ('USD', 'United States Dollar'),
      ('GBP', 'British Pound Sterling');


create table api(
    Id varchar(3) primary key,
    name varchar(10) unique not null
);


insert into api
values ('BTC','BitcoinAPI'),
       ('XAU', 'GoldAPI');


create table import_log(
    Id int auto_increment primary key,
    batch_date date not null,
    currency_id int,
    import_directory_name varchar(50) not null,
    import_file_name varchar(20) not null,
    file_created_date timestamp(4) not null,
    file_last_modified_date timestamp(4),
    row_count int,
    foreign key (currency_id) references currency(Id) on delete set null
);


create table api_import_log(
    Id int auto_increment primary key,
    currency_id int,
    api_id varchar(3),
    start_time timestamp(4) not null,
    end_time timestamp(4),
    code_response smallint,
    error_messages varchar(255),
    foreign key (api_id) references api(Id) on delete set null,
    foreign key (currency_id) references currency(Id) on delete set null
);