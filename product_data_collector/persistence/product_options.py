"""Option latest state, inside the parent product lock and transaction."""
from psycopg.types.json import Jsonb

from product_data_collector.domain.current_state import option_states


def save_options(connection, product_id, row):
    from .postgres_storage import _json_safe
    options = option_states(row["platform"], row.get("options_payload", []), row.get("prices_payload"))
    columns = (
        "option_identity", "source_option_id", "identity_status", "option_name", "attributes",
        "price", "price_meaning", "price_confidence", "stock_quantity", "stock_is_infinite",
        "status", "status_basis", "raw_option",
    )
    query = f"""INSERT INTO product_options(product_id, {','.join(columns)}, last_observed_at, membership_observed_at)
        VALUES ({','.join(['%s'] * (len(columns) + 3))})
        ON CONFLICT (product_id, option_identity) DO UPDATE SET
        {','.join(f'{c}=EXCLUDED.{c}' for c in columns[1:])},
        last_observed_at=EXCLUDED.last_observed_at, membership_status='present',
        membership_observed_at=EXCLUDED.membership_observed_at
        WHERE EXCLUDED.last_observed_at > product_options.last_observed_at
          AND EXCLUDED.last_observed_at > product_options.membership_observed_at"""
    if options:
        with connection.cursor() as cursor:
            cursor.executemany(query, [
                (product_id, *(Jsonb(_json_safe(o[c])) if c in {"attributes", "raw_option"} else o[c] for c in columns),
                 row["collected_at"], row["collected_at"]) for o in options
            ])
    if row.get("options_complete"):
        connection.execute(
            "UPDATE product_options SET membership_status='missing', membership_observed_at=%s "
            "WHERE product_id=%s AND NOT (option_identity=ANY(%s)) AND membership_observed_at < %s "
            "AND membership_status <> 'deleted'",
            (row["collected_at"], product_id, [o["option_identity"] for o in options], row["collected_at"]),
        )


def link_event_option(connection, event_id, option_id, *, evidence):
    """Explicitly verified mapping only. Never infer a mapping from a title/hash."""
    if not isinstance(evidence, dict) or not evidence:
        raise ValueError("Verified option mapping evidence is required")
    result = connection.execute(
        "UPDATE product_events e SET option_id=o.id, option_link_status='verified', "
        "option_link_evidence=%s "
        "FROM product_options o JOIN products p ON p.id=o.product_id "
        "WHERE e.id=%s AND o.id=%s AND e.event_scope='option' "
        "AND e.platform=p.platform AND e.external_product_id=p.external_product_id RETURNING e.id",
        (Jsonb({"option_mapping": evidence}), event_id, option_id),
    ).fetchone()
    if not result:
        raise ValueError("Option does not belong to event's natural product key")
    return result["id"]
