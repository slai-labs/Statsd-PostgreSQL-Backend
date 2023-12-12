{
  pghost: `postgresql-primary.${process.env.OKTETO_NAMESPACE}.svc.cluster.local`,
  pguser: "postgres",
  pgpass: "password",
  pgdb: "postgres",
  pgport: 5432,
  pginit: false,
  port: 8125,
  backends: [ "../statsd-postgres-backend" ],
  deleteGauges: true
}
