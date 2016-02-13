# A statsd PostgreSQL Backend
This backend supports storing statsd data into PostgreSQL.

## Setup
To use this backend you must do the following:

1. Run ```npm install``` in the directory in which you cloned this repo into
2. Update your statsd configuration to add the following properties:
```javascript
{
    pghost: "localhost",
    pguser: "user",
    pgpass: undefined,
    pgdb: "postgres",
    pgport: 5432,
    pginit: true,
    port: 8125,
    backends: [ "/path/to/statsdPostgresBackend" ]
}
```
3. After ensuring PostgreSQL is running, start-up statsd
4. ?????
5. Profit!

### PostgreSQL Initialization
If the ```pginit``` configuration value is set then it will attempt to initialize PostgreSQL. If the user does not have access to create tables and functions then you must run it separately and set ```pginit``` to false.

## Usage
This backend supports the typical data sent over statsd but it also has a nice little surprise for data requiring more granularity.

### Topics, categories and subcategories
When data is sent over the new backend will interpret two underscores (```__```) as a delimiter for bucketing data. So if the following is sent over:

```something__yup:3|g``` this will be interpreted as:
```
topic: "something"
metric: "yup"
value: 3
type: gague
```
This means when the data is stored into postgres the ```__``` will separate the columns in which they are stored. You can do this up to 3 levels.

```topic__category__subcategory__metric:4|c``` which translates into:
```
topic: "topic"
category: "category"
subcategory: "subcategory"
metric: "metric"
value: 4
type: count
```
#### You can ignore these buckets
If you only want to use this as intended then no big deal you'll just have some null columns in postgres you could get rid of if you want. It'll work with any statsd data out of the box :)

### Docker container
This repo also comes with a handy docker container that will pull in the latest statsd, the latest version of this PostgreSQL backend and, using environment variables, connect to whatever PostgreSQL instance you need and "just work".

Just update the environment variables in the ```./container/Dockerfile``` and you should be good to go!
