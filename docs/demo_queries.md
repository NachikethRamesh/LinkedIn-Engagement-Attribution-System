# Demo Queries

Use the SQL pack:
- `scripts/demo_queries.sql`

It includes compact queries for:
- latest pipeline runs and status
- top accounts by intent + reasons + enrichment status
- influenced opportunities by band/score
- writeback run performance by target/status
- recent writeback payload samples
- enrichment result counts and recent normalized samples

Run from `psql`:

```sql
\i scripts/demo_queries.sql
```
