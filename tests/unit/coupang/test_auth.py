from datetime import datetime, timezone

from coupang_API.api.auth import generate_authorization


def test_hmac_signature_generation_known_value():
    now = datetime(2026, 8, 21, 12, 34, 56, tzinfo=timezone.utc)
    auth = generate_authorization(
        "GET",
        "/v2/providers/affiliate_open_api/apis/openapi/products/search?keyword=test&limit=10&srpLinkOnly=false",
        "access",
        "secret",
        now,
    )

    assert auth == (
        "CEA algorithm=HmacSHA256,"
        "access-key=access,"
        "signed-date=260821T123456Z,"
        "signature=17968a00747aea2cddcd1313722d7dd9e6bb48adf9fd57fd3eba16999c8ac4ca"
    )
