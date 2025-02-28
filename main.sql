CREATE OR REPLACE FUNCTION pg_temp.bit_avg(in_schema TEXT, in_table TEXT, in_col TEXT) RETURNS TEXT AS
$$
DECLARE
    val             TEXT;
    tmp_binary      VARBIT;
    tmp_len         INT;
    max_len         INT  = 0;
    bit_count_array INT[];
    result          TEXT = '';
    count           INT  = 0;
BEGIN
    bit_count_array := ARRAY []::INT[];

    FOR val IN EXECUTE format('SELECT %I FROM %I.%I WHERE %s IS NOT NULL LIMIT 10', in_col, in_schema, in_table, in_col)
        LOOP
            count := count + 1;
            tmp_binary := right((val::bytea)::text, -1)::varbit;
            tmp_len := length(tmp_binary);
            max_len := greatest(tmp_len, max_len);
            FOR i IN 0..(tmp_len - 1)
                LOOP
                    IF bit_count_array[i] IS NULL THEN
                        bit_count_array[i] := 0;
                    END IF;
                    bit_count_array[i] := bit_count_array[i] + get_bit(tmp_binary, tmp_len - i - 1);
                END LOOP;
        END LOOP;

    FOR i IN 0..(max_len - 1)
        LOOP
            result := round(bit_count_array[i]::float / count) || result;
        END LOOP;

    RETURN result;
END;
$$ LANGUAGE plpgsql;


DO
$$
    DECLARE
        target_table_schema TEXT = 'public';
        target_table_name   TEXT = 'Н_УЧЕНИКИ'; -- 'warehouse_products';
        target_table_oid    OID;
        header_fmt          TEXT = E'%-3s %-20s| %-30s| %-60s| %-20s| %s\n';
        data_fmt            TEXT;
        output              TEXT = '';
        col                 RECORD;
        tmp_distinct        INT;
        table_pk_col_ids    SMALLINT[];
    BEGIN
        data_fmt := header_fmt || E'Среднее: %s\n \n';

        -- fetch table oid
        SELECT cls.oid
        INTO target_table_oid
        FROM pg_class cls
                 LEFT JOIN pg_namespace ns ON ns.oid = cls.relnamespace
        WHERE ns.nspname = target_table_schema
          AND cls.relname = target_table_name;

        IF NOT FOUND THEN
            RAISE NOTICE 'Not found table "%" at schema "%"', target_table_name, target_table_schema;
            RETURN;
        END IF;

        -- fetch primary key constrained cols id in table
        SELECT conkey
        FROM pg_constraint
        WHERE conrelid = target_table_oid
          AND contype = 'p'
        INTO table_pk_col_ids;

        -- print all
        FOR col IN
            SELECT att.attname                                                              AS name,
                   att.attnum                                                               AS ord,
                   CASE WHEN att.attnotnull THEN 'NOT NULL' ELSE 'NULLABLE' END             AS is_null,
                   coalesce(d.description, '<отсутствует>')                                 AS comment,
                   format_type(att.atttypid, att.atttypmod)                                 AS type,
                   pg_temp.bit_avg(target_table_schema, target_table_name, att.attname)     as bit_avg,
                   CASE WHEN att.attnum = ANY (table_pk_col_ids) THEN 'PRIMARY' ELSE '' END AS is_primary,
                   CASE
                       WHEN fk_dst_attr.attname IS NOT NULL
                           THEN format('FOREIGN(%s.%s)', fk_dst_table.relname, fk_dst_attr.attname)
                       ELSE '' END                                                          AS is_foreign
            FROM pg_attribute att
                     LEFT JOIN pg_description d -- for comments
                               ON d.objsubid = att.attnum AND d.objoid = target_table_oid
                     LEFT JOIN pg_constraint fk_constr -- for foreign constraints
                               ON fk_constr.contype = 'f'
                                   AND fk_constr.conrelid = target_table_oid
                                   AND fk_constr.conkey[1] = att.attnum
                     LEFT JOIN pg_attribute fk_dst_attr -- for fk target attribute
                               ON fk_dst_attr.attrelid = fk_constr.confrelid
                                   AND fk_dst_attr.attnum = fk_constr.confkey[1]
                     LEFT JOIN pg_class fk_dst_table
                               ON fk_dst_table.oid = fk_constr.confrelid
            WHERE att.attnum > 0
              AND att.attrelid = target_table_oid
              AND NOT att.attisdropped
            ORDER BY ord
            LOOP
                -- fetch distinct count
                EXECUTE format('SELECT COUNT(*) FROM (SELECT DISTINCT %I FROM %I.%I) AS subquery',
                               col.name,
                               target_table_schema,
                               target_table_name) INTO tmp_distinct;

                output := output ||
                          format(data_fmt,
                                 col.ord,
                                 col.name,
                                 col.type,
                                 array_to_string(array [
                                                     col.is_null,
                                                     nullif(col.is_primary, ''),
                                                     nullif(col.is_foreign, '')
                                                     ], ', '),
                                 tmp_distinct,
                                 col.comment,
                                 col.bit_avg
                          );
            END LOOP;

        RAISE NOTICE
            E'Таблица: % (%)\n\n%\n%',
            target_table_name, target_table_oid,
            format(header_fmt, 'No.', 'Имя столбца', 'Тип', 'Атрибуты', 'Уникальных к-во', 'Комментарий'),
            output;
    END
$$;

