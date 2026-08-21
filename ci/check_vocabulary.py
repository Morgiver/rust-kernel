#!/usr/bin/env python3
"""Guard: no domain, transport or technology name inside the kernel crates.

Design section 16, altitude rule: "Aucun type du Kernel ne nomme une entite de
domaine, ni un transport, ni une technologie. Un tel nom dans `kernel-core` ou
`kernel` est un defaut, pas un raccourci." A boundary asserted but not verified
does not exist, so the rule is a build failure, not a review habit.

The word list lives in `ci/forbidden-words.txt` so it can be extended without
touching this script. It has two sections:

* `[anywhere]` — names matched in the whole file, comments and string literals
  included. Reserved for words no legitimate sentence of a generic kernel needs.
* `[identifiers]` — names matched in code only, with comments, string literals
  and character literals blanked out first. Reserved for words ordinary prose
  uses freely ("it opens no socket") but which must never name a type, a field,
  a function, a module or a feature.

Matching is case insensitive and token bounded: a word matches when it is not
glued to another alphanumeric run on either side, where a case change counts as
a boundary. So `http` matches `HttpThing`, `http_thing` and `myHttpThing`, and
does not match `https` or `restart`. Plural forms need their own entry.

`examples/` is excluded, and it is the only exclusion. The altitude rule holds
over the kernel's own crates; section 17 of the design is an application-layer
illustration, and an illustration forbidden from naming an entity of a domain
illustrates nothing. That exclusion is where the rule stops applying, stated
here rather than left to the accident of what `SCANNED` happens to reach.
"""

from __future__ import annotations

import bisect
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WORD_LIST = ROOT / "ci" / "forbidden-words.txt"
SCANNED = ("crates",)
SUFFIXES = (".rs", ".toml")

# Paths under any of these, relative to the repository root, are never scanned.
# The application layer is where domain vocabulary belongs; see the module
# docstring. Kept as an explicit filter so that widening `SCANNED` later cannot
# silently start failing the example.
EXCLUDED = ("examples",)


def excluded(path: Path) -> bool:
    """Whether `path` lies under an excluded directory."""
    parts = path.relative_to(ROOT).parts
    return any(part in EXCLUDED for part in parts)


def load_words(path: Path) -> dict[str, list[str]]:
    sections: dict[str, list[str]] = {"anywhere": [], "identifiers": []}
    current: str | None = None
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        if line.startswith("[") and line.endswith("]"):
            current = line[1:-1].strip()
            if current not in sections:
                sys.exit(f"{path}:{number}: unknown section [{current}]")
            continue
        if current is None:
            sys.exit(f"{path}:{number}: word outside any section")
        if not re.fullmatch(r"[A-Za-z0-9]+", line):
            sys.exit(f"{path}:{number}: '{line}' is not a single alphanumeric token")
        sections[current].append(line.lower())
    return sections


def blank_comments_and_literals(source: str) -> str:
    """Return `source` with comments and literals replaced by spaces.

    Line and column positions are preserved so a report still points at the
    offending line. Newlines survive; every other blanked character becomes a
    space.
    """
    out = list(source)
    i = 0
    length = len(source)

    def blank(start: int, end: int) -> None:
        for index in range(start, min(end, length)):
            if out[index] != "\n":
                out[index] = " "

    while i < length:
        char = source[i]
        if char == "/" and i + 1 < length and source[i + 1] == "/":
            end = source.find("\n", i)
            end = length if end == -1 else end
            blank(i, end)
            i = end
        elif char == "/" and i + 1 < length and source[i + 1] == "*":
            depth = 1
            j = i + 2
            while j < length and depth:
                if source.startswith("/*", j):
                    depth += 1
                    j += 2
                elif source.startswith("*/", j):
                    depth -= 1
                    j += 2
                else:
                    j += 1
            blank(i, j)
            i = j
        elif char in "rb" and (match := re.match(r'(?:b?r|rb)(#*)"', source[i:])):
            hashes = match.group(1)
            closing = '"' + hashes
            end = source.find(closing, i + match.end())
            end = length if end == -1 else end + len(closing)
            blank(i, end)
            i = end
        elif char == '"' or (char == "b" and source.startswith('b"', i)):
            j = i + (2 if char == "b" else 1)
            while j < length:
                if source[j] == "\\":
                    j += 2
                    continue
                if source[j] == '"':
                    j += 1
                    break
                j += 1
            blank(i, j)
            i = j
        elif char == "'":
            match = re.match(r"'(?:\\.|[^\\'])'", source[i:])
            if match:
                blank(i, i + match.end())
                i += match.end()
            else:
                i += 1  # a lifetime, not a literal
        else:
            i += 1
    return "".join(out)


def bounded(text: str, start: int, end: int) -> bool:
    before = text[start - 1] if start else ""
    after = text[end] if end < len(text) else ""
    head_ok = not before.isalnum() or (text[start].isupper() and not before.isupper())
    tail_ok = not after.isalnum() or after.isupper()
    return head_ok and tail_ok


def hits(text: str, words: list[str]) -> list[tuple[int, str, str]]:
    found: list[tuple[int, str, str]] = []
    lines = text.splitlines()
    starts: list[int] = []
    offset = 0
    for line in lines:
        starts.append(offset)
        offset += len(line) + 1
    for word in words:
        for match in re.finditer(re.escape(word), text, re.IGNORECASE):
            if not bounded(text, match.start(), match.end()):
                continue
            number = bisect.bisect_right(starts, match.start()) - 1
            found.append((number + 1, word, lines[number].strip()))
    return found


def main() -> int:
    if not WORD_LIST.exists():
        print(f"vocabulary guard: missing word list {WORD_LIST}", file=sys.stderr)
        return 1
    sections = load_words(WORD_LIST)

    files = sorted(
        path
        for directory in SCANNED
        for path in (ROOT / directory).rglob("*")
        if path.is_file() and path.suffix in SUFFIXES and not excluded(path)
    )

    violations = 0
    for path in files:
        source = path.read_text(encoding="utf-8")
        relative = path.relative_to(ROOT)
        reports: list[tuple[int, str, str, str]] = []
        for number, word, line in hits(source, sections["anywhere"]):
            reports.append((number, word, "anywhere", line))
        if path.suffix == ".rs":
            code = blank_comments_and_literals(source)
        else:
            code = source
        for number, word, line in hits(code, sections["identifiers"]):
            reports.append((number, word, "identifiers", line))
        for number, word, tier, line in sorted(reports):
            print(f"{relative}:{number}: forbidden [{tier}] word '{word}': {line}", file=sys.stderr)
            violations += 1

    if violations:
        print(
            f"vocabulary guard: {violations} violation(s); see ci/forbidden-words.txt",
            file=sys.stderr,
        )
        return 1

    print(f"vocabulary guard: ok ({len(files)} files scanned)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
