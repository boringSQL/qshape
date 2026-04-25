SELECT array_agg(
  CASE WHEN tn.nspname = 'pg_catalog' THEN t.typname
       ELSE tn.nspname || '.' || t.typname END
  ORDER BY a.ord)
FROM pg_proc p
JOIN pg_namespace pn ON pn.oid = p.pronamespace
JOIN LATERAL unnest(p.proargtypes) WITH ORDINALITY AS a(oid, ord) ON true
JOIN pg_type t ON t.oid = a.oid
JOIN pg_namespace tn ON tn.oid = t.typnamespace
WHERE p.proname = $2
  AND ($1 = '' OR pn.nspname = $1)
  AND p.pronargs = $3
GROUP BY p.oid
