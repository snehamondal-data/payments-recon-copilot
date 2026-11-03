"""Fill the two public keys into snowflake/setup.sql. Writes secrets/setup_rendered.sql (git-ignored).

Generate the key pairs first (see README, Slice 5). Only public keys go into the SQL.
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def public_key_body(path: Path) -> str:
    """The base64 body of a PEM public key, on one line, as Snowflake's RSA_PUBLIC_KEY expects."""
    return "".join(line for line in path.read_text().splitlines() if "-----" not in line)


def main() -> None:
    sql = (ROOT / "snowflake" / "setup.sql").read_text()
    for user in ("loader", "agent"):
        key = public_key_body(ROOT / "secrets" / f"recon_{user}_rsa_key.pub")
        sql = sql.replace(f"{{{{RECON_{user.upper()}_PUBLIC_KEY}}}}", key)
    assert "{{" not in sql, "a placeholder was not filled"
    out = ROOT / "secrets" / "setup_rendered.sql"
    out.write_text(sql)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
