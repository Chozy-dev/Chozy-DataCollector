"""Source event adapters. No network access and no modification of latest state."""
from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import copy

from .current_state import digest, number


def numeric_identity(value):
    if isinstance(value, dict):
        return {k: numeric_identity(v) for k, v in value.items()}
    parsed = number(value)
    return {"number": str(parsed.normalize())} if parsed is not None else value

OWNER_KINDS = {
    "priceIncreased": "base_price", "priceDecreased": "base_price",
    "soldout": "soldout", "restocked": "restocked", "prohibited": "prohibited",
    "returnShippingFeeIncreased": "return_shipping_fee", "returnShippingFeeDecreased": "return_shipping_fee",
    "optionSoldout": "soldout", "optionRestocked": "restocked",
    "optionPriceIncreased": "option_final_price", "optionPriceDecreased": "option_final_price",
    "optionChanged": "option_configuration",
    "categoryChanged": "category", "nameChanged": "product_name",
    "originChanged": "origin", "productionChanged": "production",
    "shippingTypeChanged": "shipping_type", "returnableChanged": "returnable",
    "returnCriteriaChanged": "return_criteria", "etc": "other_change",
    "contentChanged": "content", "imageChanged": "image",
    "productNotificationInformationChanged": "product_notification_information",
    "shippingFeeIncreased": "shipping_fee", "shippingFeeDecreased": "shipping_fee",
    "discontinued": "discontinued", "optionDiscontinued": "discontinued",
    "boxQuantityChanged": "box_quantity", "modelChanged": "model",
    "searchKeywordsChanged": "search_keywords",
    "fixedPriceIncreased": "fixed_price", "fixedPriceDecreased": "fixed_price",
    "sellerOnlyContentChanged": "seller_only_content", "adultOnlyChanged": "adult_only",
    "noReturnReasonChanged": "no_return_reason",
    "certificateInformationChanged": "certificate_information",
    "pricePolicyChanged": "price_policy", "taxFreeChanged": "tax_free",
    "additionalDocumentsChanged": "additional_documents",
}
DOME_KINDS = {
    "AMT": "price_signal", "OPTAMT": "option_price_signal", "SOLDOUT": "soldout",
    "RESTART": "restocked", "OPTSOLDOUT": "soldout", "OPTRESTART": "restocked",
    "OPEN": "listing_opened", "CLOSE": "listing_closed", "DEL": "listing_deleted",
}
EXCLUDED_KINDS = {"stockChanged", "optionStockChanged"}


def utc(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("Timezone-aware observation time is required")
    return value.astimezone(timezone.utc)


def epoch(value, divisor):
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        raise ValueError("Invalid source event timestamp")
    seconds = Decimal(str(value)) / divisor
    if not seconds.is_finite():
        raise ValueError("Invalid source event timestamp")
    return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=int(seconds * 1000000))


@dataclass(frozen=True)
class ProductEvent:
    platform: str
    external_product_id: str
    source_api: str
    event_kind: str
    occurred_at: datetime | None
    observed_at: datetime
    semantic: str
    raw_event: dict
    event_origin: str = "source"
    event_scope: str = "product"
    source_event_id: str | None = None
    option_reference: dict | None = None
    raw_before: object = None
    raw_after: object = None
    verified_before: object = None
    verified_after: object = None
    value_status: str = "unresolved"
    verification_evidence: dict | None = None
    identity_discriminator: object = None

    def record(self):
        if not all(isinstance(v, str) and v.strip() for v in
                   (self.platform, self.external_product_id, self.source_api, self.event_kind)):
            raise ValueError("Event natural key, API and kind are required")
        if self.event_kind in EXCLUDED_KINDS:
            raise ValueError("Quantity-only source events are excluded")
        if self.event_origin == "observation" and self.occurred_at is not None:
            raise ValueError("Observation events cannot invent an occurrence time")
        observed = utc(self.observed_at)
        occurred = utc(self.occurred_at) if self.occurred_at is not None else None
        stable = self.source_event_id not in (None, "")
        identity = ["v1", self.platform, self.external_product_id, self.source_api, self.event_origin]
        if stable:
            identity += ["source_id", str(self.source_event_id)]
        else:
            # Do not use cursor, collection time, page, or a random ID here.
            numeric = self.semantic in {"base_price", "option_final_price", "return_shipping_fee"}
            identity += [self.event_kind, occurred.isoformat() if occurred else None,
                         self.event_scope, self.option_reference,
                         numeric_identity(self.raw_before) if numeric else self.raw_before,
                         numeric_identity(self.raw_after) if numeric else self.raw_after,
                         numeric_identity(self.identity_discriminator) if self.source_api == "getAllSupplyChk" else self.identity_discriminator]
        row = asdict(self)
        row.pop("identity_discriminator")
        row.update(observed_at=observed, occurred_at=occurred, dedupe_key=digest(identity),
                   source_event_id=str(self.source_event_id) if stable else None,
                   identity_status="source_id" if stable else "fingerprint" if occurred else "ambiguous",
                   identity_note=None if stable else "Identical source records cannot prove distinct occurrences; no stable event ID verified.",
                   option_link_status="unresolved" if self.event_scope == "option" else "not_applicable")
        return row


def ownerclan_event(raw, observed_at, *, verified_source_event_id=None):
    kind = raw.get("kind")
    if kind in EXCLUDED_KINDS:
        return None
    if kind not in OWNER_KINDS:
        raise ValueError(f"Unsupported ownerclan event kind: {kind}")
    option = kind.startswith("option")
    before, after = raw.get("valueBefore"), raw.get("valueAfter")
    parsed_before, parsed_after, status = None, None, "unresolved"
    if OWNER_KINDS[kind] in {"base_price", "option_final_price", "return_shipping_fee"}:
        a, b = number(before), number(after)
        if a is not None and b is not None and (b > a if kind.endswith("Increased") else b < a):
            parsed_before, parsed_after, status = a, b, "source_parsed"
    return ProductEvent(
        "ownerclan", str(raw.get("itemKey") or ""), "itemHistories", kind,
        epoch(raw.get("createdAt"), 1000), utc(observed_at), OWNER_KINDS[kind], copy.deepcopy(raw),
        event_scope="option" if option else "product", source_event_id=verified_source_event_id,
        # Even an optionKey here is only an unverified reference, never auto-linked.
        option_reference={k: raw[k] for k in ("optionKey", "title") if k in raw} if option else None,
        raw_before=before, raw_after=after, verified_before=parsed_before, verified_after=parsed_after,
        value_status=status, identity_discriminator=raw.get("title"),
    )


def domeggook_event(raw, observed_at, *, verified_source_event_id=None):
    kind = raw.get("status")
    if kind not in DOME_KINDS:
        raise ValueError(f"Unsupported domeggook event status: {kind}")
    option = kind.startswith("OPT")
    return ProductEvent(
        "domeggook", str(raw.get("no") or ""), "getAllSupplyChk", kind,
        epoch(raw.get("date"), 1), utc(observed_at), DOME_KINDS[kind], copy.deepcopy(raw),
        event_scope="option" if option else "product", source_event_id=verified_source_event_id,
        option_reference={k: raw[k] for k in ("optionKey", "hid", "hash") if k in raw} if option else None,
        # price/qty are accompanying values, not a verified before/after delta.
        value_status="signal_only",
        identity_discriminator={k: raw[k] for k in ("price", "qty", "supplyQty", "supplyAmtNotDiscount") if k in raw},
    )


def observation_event(*, platform, external_product_id, kind, observed_at, before, after,
                      comparison_key, semantic, option_reference=None):
    if not comparison_key:
        raise ValueError("Durable comparison identity (e.g. history row IDs) is required")
    return ProductEvent(platform, external_product_id, "snapshot_comparison", kind, None,
        utc(observed_at), semantic, {}, event_origin="observation",
        event_scope="option" if option_reference is not None else "product", option_reference=option_reference,
        raw_before=before, raw_after=after, identity_discriminator=comparison_key)
