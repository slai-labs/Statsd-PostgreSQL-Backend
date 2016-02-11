# A statsd PostgreSQL Backend
This backend supports storing statsd data into PostgreSQL.

## Usage
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
