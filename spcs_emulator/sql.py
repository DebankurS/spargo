"""Tiny SQL front-end for the SPCS statement subset the emulator understands."""
from __future__ import annotations

import re


class SqlError(Exception):
    def __init__(self, message: str, code: str = "001003", state: str = "42000"):
        super().__init__(message)
        self.code, self.state = code, state


def not_found(kind: str, name: str) -> SqlError:
    return SqlError(f"SQL compilation error:\n{kind} '{name}' does not exist or not authorized.", "002003", "42S02")


def exists(kind: str, name: str) -> SqlError:
    return SqlError(f"SQL compilation error:\nObject '{name}' already exists.", "002002", "42710")


def extract_dollar_blocks(sql: str) -> tuple[str, list[str]]:
    blocks: list[str] = []

    def rep(m):
        blocks.append(m.group(1))
        return f"__SPEC{len(blocks) - 1}__"
    return re.sub(r"\$\$(.*?)\$\$", rep, sql, flags=re.S), blocks


PROP_RE = re.compile(
    r"(\w+)\s*=\s*('(?:[^'\\]|\\.|'')*'|\([^)]*\)|[^\s]+)", re.S)


def parse_value(raw: str):
    if raw.startswith("'"):
        body = raw[1:-1]
        return re.sub(r"\\(.)|''", lambda m: m.group(1) if m.group(1) else "'", body)
    if raw.startswith("("):
        return [x.strip().strip('"').upper() for x in raw[1:-1].split(",") if x.strip()]
    u = raw.upper()
    if u in ("TRUE", "FALSE"):
        return u == "TRUE"
    if re.fullmatch(r"-?\d+", raw):
        return int(raw)
    return raw.strip('"').upper() if re.fullmatch(r'"?[\w$]+"?', raw) else raw


def parse_props(text: str) -> dict:
    props = {}
    pos = 0
    text = text.strip()
    for m in PROP_RE.finditer(text):
        gap = text[pos:m.start()].strip()
        if gap:
            raise SqlError(f"syntax error: unexpected '{gap.split()[0]}'")
        props[m.group(1).upper()] = parse_value(m.group(2))
        pos = m.end()
    tail = text[pos:].strip()
    if tail:
        raise SqlError(f"syntax error: unexpected '{tail.split()[0]}'")
    return props


NAME = r'((?:"[^"]+"|[\w$]+)(?:\.(?:"[^"]+"|[\w$]+)){0,2})'


def split_name(name: str, db: str | None, schema: str | None) -> tuple[str, str, str]:
    parts = re.findall(r'"([^"]+)"|([\w$]+)', name)
    parts = [a if a else b.upper() for a, b in parts]
    if len(parts) == 3:
        return tuple(parts)
    if len(parts) == 2:
        if not db:
            raise SqlError("Cannot perform operation. This session does not have a current database.", "090105", "22000")
        return (db, *parts)
    if not db or not schema:
        raise SqlError("Cannot perform operation. This session does not have a current schema.", "090106", "22000")
    return (db, schema, parts[0])


def ident(name: str) -> str:
    return name.strip('"').upper() if not name.startswith('"') else name.strip('"')
