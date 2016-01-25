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
    port: 8125,
    backends: [ "/path/to/statsdPostgresBackend" ]
}
```
3. After ensuring PostgreSQL is running, start-up statsd
4. ?????
5. Profit!

### Caveats
There are a couple of caveats with this module at the moment.

1. The database user needs to be able to create tables and functions as this is how it is currently initialized. Ideally the initialization script can be run separately to avoid giving the database user such permissions but at the moment it'll try running it anyway on start-up.

2. At the moment this **only stores counts**! There are a lot of considerations when storing statsd data that simply haven't been factored into this backend yet.
