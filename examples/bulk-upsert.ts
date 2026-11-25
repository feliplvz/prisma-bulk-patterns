import { Prisma, PrismaClient } from '@prisma/client'
import { performance } from 'node:perf_hooks'

const prisma = new PrismaClient()

type ProductRow = { sku: string; name: string; price: number }

async function bulkUpsertProducts(rows: ProductRow[]) {
  const existing = await prisma.product.findMany({
    where: { sku: { in: rows.map(r => r.sku) } },
    select: { id: true, sku: true, name: true, price: true },
  })
  const bySku = new Map(existing.map(p => [p.sku, p]))

  const toCreate: ProductRow[] = []
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

  return { created: toCreate.length, updated: toUpdate.length }
}

function generateRows(count: number): ProductRow[] {
  return Array.from({ length: count }, (_, i) => ({
    sku: `SKU-${String(i + 1).padStart(5, '0')}`,
    name: `Item ${i + 1}`,
    price: Math.floor(Math.random() * 100_000) + 1_000,
  }))
}

async function main() {
  const countArg = Number(process.argv[2] ?? 100)
  if (Number.isNaN(countArg) || countArg <= 0) {
    console.error('Usage: pnpm tsx examples/bulk-upsert.ts [row_count]')
    process.exit(1)
  }

  const rows = generateRows(countArg)

  const t0 = performance.now()
  const result = await bulkUpsertProducts(rows)
  const elapsed = performance.now() - t0

  console.log(
    `${countArg} rows · ${result.created} created · ${result.updated} updated · ${elapsed.toFixed(0)}ms`,
  )

  await prisma.$disconnect()
}

main().catch(async err => {
  console.error(err)
  await prisma.$disconnect()
  process.exit(1)
})
