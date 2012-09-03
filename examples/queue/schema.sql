---
--- Create a table to host events
---

CREATE TABLE libphp(id bigserial, data text);

CREATE OR REPLACE FUNCTION generate_events(nb integer)
  RETURNS void
  LANGUAGE PLpgSQL
AS $$
DECLARE
  max bigint := coalesce((SELECT max(id) FROM libphp), 0);
BEGIN
  INSERT INTO libphp
       SELECT x, md5(x::text)
         FROM generate_series(max, max+nb) as t(x);
END;
$$;

