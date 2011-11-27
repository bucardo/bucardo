#!/bin/bash

## Quick setup for semi-standardized command-line testing
echo This will remove any existing bucardo schema! Proceed?
read yn

if [ $yn != 'y' ]
then
  exit
fi

echo Good to go!

## Clear out old databases and users
psql -qc 'drop database if exists bucardo'
psql -qc 'drop database if exists test1'
psql -qc 'drop database if exists test2'
psql -qc 'drop database if exists test3'
psql -qc 'drop user bucardo'

## Install bucardo
./bucardo install --batch

## Create test databases, users, and tables
psql -qc 'create database test1'
psql -d test1 -qc 'create table t1 (id serial primary key, email text)'
psql -d test1 -qc 'create table t2 (id serial primary key, email text)'
psql -d test1 -qc 'create table t3 (id serial primary key, email text)'
psql -d test1 -qc 'create table t4 (id serial primary key, email text)'
psql -qc 'create database test2 template test1'
psql -qc 'create database test3 template test1'

## Tell Bucardo about the databases
./bucardo add db db1 dbname=test1
./bucardo add db db2 dbname=test2
./bucardo add db db3 dbname=test3

## Add in all the tables from test1, put into a herd
./bucardo add all tables herd=myherd

## Add a new table that is not in any herd (or sync)
psql -d test1 -qc 'create table t5 (id serial primary key, email text)'
./bucardo add table t5

## Simple source -> target sync
./bucardo add sync alpha herd=myherd dbs=db1,db2

## Source to source sync
./bucardo add sync beta herd=myherd dbs=db1,db2:source

## Source to source to target sync
./bucardo add sync charlie herd=myherd dbs=db1,db2:source,db3:target

