# Worked business scenarios

Three end-to-end problems. Each shows the business goal, a **validated** doc you
can adapt, and the commands to validate → save → run → verify. These use the
built-in `http-json-extractor` and `sql-transform`; swap in your real API URLs,
buckets, and SQL. Always re-run `components get <name> --schema` for the config
shape on the cluster you're operating, and `pipeline validate` after editing.

All three assume auth is set (see SKILL.md → Prerequisites). Save each doc as
`pipeline.yaml` and run:

```bash
datuplet pipeline validate -f pipeline.yaml --json    # fix findings until exit 0
datuplet pipeline put -f pipeline.yaml
datuplet trigger --wait --json --timeout 15m <name>   # flags BEFORE the name; expect phase Succeeded
```

(Flags must precede the positional pipeline name — the parser stops at the first
non-flag arg, so `trigger <name> --wait` would ignore `--wait`.)

---

## Scenario 1 — Ingest an external JSON API into a table

**Goal:** "Pull our posts API into a table we can query."

One extract stage: `http-json-extractor` fetches the endpoint and writes it as a
table in a `raw` bucket.

```yaml
name: ingest-posts
stages:
  - name: extract
    components:
      - name: posts-extractor
        component: http-json-extractor
        config:
          url: "https://api.example.com/posts"
          table_name: posts
        outputs:
          tables:
            - name: posts
              bucket: raw
              writeMode: FULL_LOAD    # rebuild the table each run; use APPEND to accumulate
```

**Verify:**

```bash
datuplet storage sample raw.posts
datuplet query --sql 'SELECT count(*) AS n FROM "raw"."posts"'
```

Expect a non-zero row count matching what the API returned.

---

## Scenario 2 — Aggregate raw rows into a summary table

**Goal:** "From those posts, produce a per-user post-count summary."

Two stages: extract (as above), then a `sql-transform` that reads `raw.posts`
and writes a `reporting.user_summary` table. The transform's `inputs.tables`
name what it reads; the `CREATE TABLE` name matches its `outputs.tables[].name`.

```yaml
name: user-summary
stages:
  - name: extract
    components:
      - name: posts-extractor
        component: http-json-extractor
        config:
          url: "https://api.example.com/posts"
          table_name: posts
        outputs:
          defaultBucket: raw
          defaultWriteMode: FULL_LOAD
  - name: transform
    components:
      - name: summarize
        component: sql-transform
        inputs:
          tables:
            - bucket: raw
              table: posts
        outputs:
          tables:
            - name: user_summary
              bucket: reporting
              writeMode: FULL_LOAD
        config:
          sql: |
            CREATE TABLE user_summary AS
            SELECT
              "userId",
              COUNT(*) AS post_count
            FROM posts
            GROUP BY "userId"
            ORDER BY "userId";
```

**Verify:**

```bash
datuplet query --sql 'SELECT * FROM "reporting"."user_summary" ORDER BY "userId" LIMIT 20'
```

The classic "daily summary" is the same shape — extract the source, then a
`sql-transform` with a `GROUP BY` on a date column.

---

## Scenario 3 — Join two sources into an enriched table

**Goal:** "Combine posts with their authors so each post row carries the
author's name and email."

Extract stage runs **two** extractors in parallel (posts + users) into the same
`raw` bucket; a transform stage joins them. Note both input tables listed under
the transform's `inputs.tables`.

```yaml
name: enrich-posts
stages:
  - name: extract
    components:
      - name: posts-extractor
        component: http-json-extractor
        config:
          url: "https://api.example.com/posts"
          table_name: posts
        outputs:
          tables:
            - name: posts
              bucket: raw
              writeMode: FULL_LOAD
      - name: users-extractor
        component: http-json-extractor
        config:
          url: "https://api.example.com/users"
          table_name: users
        outputs:
          tables:
            - name: users
              bucket: raw
              writeMode: FULL_LOAD
  - name: transform
    components:
      - name: join
        component: sql-transform
        inputs:
          tables:
            - bucket: raw
              table: posts
            - bucket: raw
              table: users
        outputs:
          tables:
            - name: post_details
              bucket: joined
              writeMode: FULL_LOAD
        config:
          sql: |
            CREATE TABLE post_details AS
            SELECT
              p.id AS post_id,
              p.title,
              u.name AS author_name,
              u.email AS author_email
            FROM posts p
            JOIN users u ON p."userId" = u.id
            ORDER BY p.id;
```

**Verify:**

```bash
datuplet query --sql 'SELECT count(*) FROM "joined"."post_details"'
datuplet storage sample joined.post_details
```

Row count should match the number of posts that had a matching user; spot-check
that `author_name`/`author_email` are populated.

---

## Adapting these

- **Different source:** change the extractor `config` (its schema via
  `components get`); for market data use `finnhub-extractor` (its api token is a
  secret → `$[finnhub_token]`).
- **Incremental vs full:** `writeMode: APPEND` accumulates across runs;
  `FULL_LOAD` rebuilds. Match the business need.
- **More transforms:** add stages; each new stage's `inputs` can read any table
  an earlier stage wrote.
- **Secrets:** if any config field is `x-datuplet-secret`, write `$[key]` and
  ensure the project secret is set before triggering.
