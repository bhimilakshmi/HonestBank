DROP DATABASE IF EXISTS honest_bank;
CREATE DATABASE  honest_bank;
USE  honest_bank;
CREATE  TABLE  tickers(
`index` INTEGER ,
date TIMESTAMP NOT NULL,
ticker VARCHAR(100),
ticker_name VARCHAR(100),
open DOUBLE NOT NULL,
high DOUBLE NOT NULL,
low DOUBLE NOT NULL,
close DOUBLE NOT NULL,
`adj close` DOUBLE NOT NULL,
volume INTEGER NOT NULL
);
