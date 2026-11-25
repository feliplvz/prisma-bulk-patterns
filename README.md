# prisma-bulk-patterns

Patterns for high-volume writes with Prisma + Postgres on serverless runtimes.

## Problem

Prisma's default write pattern is one query per operation. On serverless
talking to Postgres across regions, each round-trip costs around
100-150ms. Loops like
`for (const item of items) { await prisma.model.upsert(...) }` become
unusable past a few hundred rows:

| Rows  | Sequential upsert | Observable behavior      |
|-------|-------------------|--------------------------|
| 100   | ~15-25s           | Noticeably slow          |
| 1,000 | ~2-5 min          | Request still running    |
| 3,500 | 15-25 min         | Hits Vercel maxDuration  |
| 10k   | n/a               | Times out                |

This repo documents the pattern that keeps round-trips at a small fixed
number regardless of row count, using a single interactive transaction
with `createMany`, `UPDATE ... CASE WHEN`, and raw SQL `ON CONFLICT`.

## The pattern

Three phases:

1. Pre-load existing rows with one `findMany`.
2. Partition in memory into `toCreate` and `toUpdate`. No DB calls.
3. Write inside a single transaction with `createMany` for new rows,
   `UPDATE ... CASE WHEN` for heterogeneous updates, and
   `INSERT ... ON CONFLICT DO UPDATE` via `$executeRaw` for anything
   that doesn't fit `updateMany`'s uniform-value constraint.

```typescript
import { Prisma, PrismaClient } from '@prisma/client'

const prisma = new PrismaClient()

export async function bulkUpsertProducts(
  rows: Array<{ sku: string; name: string; price: number }>,
) {
  const existing = await prisma.product.findMany({
    where: { sku: { in: rows.map(r => r.sku) } },
    select: { id: true, sku: true, name: true, price: true },
  })
  const bySku = new Map(existing.map(p => [p.sku, p]))

  const toCreate: typeof rows = []
  const toUpdate: Array<{ id: string; name: string; price: number }> = []

  for (const r of rows) {
    const hit = bySku.get(r.sku)
    if (!hit) {
      toCreate.push(r)
    } else if (hit.name !== r.name || Number(hit.price) !== r.price) {
      toUpdate.push({ id: hit.id, name: r.name, price: r.price })
    }
  }

  await prisma.$transaction(
    async tx => {
      if (toCreate.length > 0) {
        await tx.product.createMany({
          data: toCreate,
          skipDuplicates: true,
        })
      }
      if (toUpdate.length > 0) {
        const nameCases = Prisma.join(
          toUpdate.map(u => Prisma.sql`WHEN ${u.id} THEN ${u.name}`),
          ' ',
        )
        const priceCases = Prisma.join(
          toUpdate.map(u => Prisma.sql`WHEN ${u.id} THEN ${u.price}::decimal`),
          ' ',
        )
        await tx.$executeRaw(Prisma.sql`
          UPDATE "Product" SET
            "name"      = CASE "id" ${nameCases} END,
            "price"     = CASE "id" ${priceCases} END,
            "updatedAt" = NOW()
          WHERE "id" = ANY(${toUpdate.map(u => u.id)}::text[])
        `)
      }
    },
    { timeout: 30000, maxWait: 5000 },
  )
}
```

## Results

Measured on a cross-region serverless deployment (US-based compute,
South-America database):

| Approach                      | 100 rows | 1,000 rows | 3,500 rows |
|-------------------------------|----------|------------|------------|
| Sequential upsert + loop      | 15-25s   | 2-5 min    | timeout    |
| Promise.all in chunks of 10   | 8-12s    | 90s        | 7-10 min   |
| Bulk pattern (this repo)      | 1-2s     | 3-5s       | 10-15s     |

The gap grows with network latency between compute and database.

## When not to use this

- Fewer than ~20 rows: use `prisma.$transaction([...])` with a short
  array.
- Rows that need nested writes: `createMany` on Postgres doesn't
  support nested `create` for relations.
- Updates with conditional logic per row that can't be expressed in SQL:
  fall back to a loop inside the transaction.

## Gotchas

- `updateMany` accepts a single `data` object applied uniformly to all
  matched rows. Heterogeneous row updates need `UPDATE ... CASE WHEN`
  via raw SQL.
- Postgres has a 65,535 bind parameter limit per statement. Keep bulk
  inserts under ~7,000 rows for 8-column tables.
- `createMany` doesn't return IDs. Use `createManyAndReturn` when you
  need them for dependent inserts.
- `prisma.$transaction([...array])` sends operations sequentially
  despite the array API. For parallelism use the interactive form.
- Default `$transaction` timeout is 5s. Set it explicitly
  (e.g. `timeout: 30000`) for larger bulks.

## Runnable example

```
git clone https://github.com/feliplvz/prisma-bulk-patterns
cd prisma-bulk-patterns
pnpm install
cp .env.example .env
pnpm prisma migrate dev
pnpm tsx examples/bulk-upsert.ts
```

## License

MIT
