from coupang_API.config import load_credentials


def test_load_credentials_accepts_utf8_bom_env_file(tmp_path, monkeypatch):
    monkeypatch.delenv("COUPANG_ACCESS_KEY", raising=False)
    monkeypatch.delenv("COUPANG_SECRET_KEY", raising=False)
    (tmp_path / ".env").write_text(
        "COUPANG_ACCESS_KEY=access\n"
        "COUPANG_SECRET_KEY=secret\n",
        encoding="utf-8-sig",
    )

    assert load_credentials(tmp_path) == ("access", "secret")
