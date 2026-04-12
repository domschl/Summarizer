# Wiki Naming & Synchronisation Migration — Final Plan

## Decisions (from user review)

| Decision | Resolution |
|----------|-----------|
| Filename format | `Title - Author.md` (author always included) |
| Max length | **80 characters** for the basename (without `.md`) |
| Summary prefix | `"Summary of "` + basename — prefix is NOT subject to 80-char limit |
| Trailing numbering | Protected: truncation only shortens the title body, never the trailing number |
| Collision handling | **Always abort**. Never auto-disambiguate. User fixes Calibre metadata. |
| Git operations | Tools use OS `mv`/`os.rename`. User handles git commits. |
| Orphan handling | Auto-remove. Calibre→markdown, markdown→summaries source-of-truth chain. |
| Dry-run | Default mode for migration. `--execute` flag to apply. |
| Recovery | Git repos on new branch provide safety net. |

---

## Component 1: Naming Strategy Module

### [NEW] [naming.py](file:///home/dsc/Codeberg/Summarizer/naming.py)

**`title_to_filename(title: str, author: str, max_length: int = 80) -> str`:**

1. Form the author suffix: ` - <first_author>`
2. Form the full candidate: `title + author_suffix`
3. **Detect trailing numbering** in title: regex for patterns like `Vol 3`, `Volume III`, `Part 12`, `Book IV`, `Bd. 7`, `Nr. 14`, `, 2`, `(3)`, etc. Store as `number_suffix`.
4. **Replace filesystem-unsafe characters** in the full string:
   - `:` → ` —`
   - `/` → `-`
   - `\` → `-`  
   - `*`, `?`, `"`, `<`, `>`, `|` → removed
   - Leading/trailing spaces and dots stripped
5. **Truncate** if total length > `max_length`:
   - Only the title body is shortened (author suffix and number suffix are protected)
   - Truncate at last word boundary
   - Remove trailing punctuation (`, `, `_ `, etc.)
6. Return the result (mixed-case). Uniqueness checks use `.lower()`.

**`generate_filename(title: str, author: str, max_length: int = 80) -> str`:**
- Returns `title_to_filename(title, author, max_length) + ".md"`

**`generate_summary_filename(title: str, author: str, max_length: int = 80) -> str`:**
- Returns `"Summary of " + generate_filename(title, author, max_length)`
- No length restriction on the result.

**`check_collisions(filename_list: list[str]) -> list[tuple[str, list]]`:**
- Case-insensitive collision detection across a set of generated filenames.
- Returns list of collisions. Any collision is a fatal error.

---

## Component 2: Synchronisation Engine

### [MODIFY] [calibre_sync.py](file:///home/dsc/Codeberg/Summarizer/calibre_sync.py)

Two-phase sync:

**Phase 1 — `plan_sync()` → list of actions:**
- `ADD` — new book → convert
- `REMOVE` — book no longer in Calibre → delete markdown
- `RECONVERT` — source_hash changed → reconvert
- `RENAME` — title/author changed → `os.rename`
- `MOVE` — series changed → `os.rename` to new directory
- `UPDATE_METADATA` — other metadata changed → update YAML in-place
- `SKIP` — unchanged
- **Collision in planned state → ABORT entire sync**

**Phase 2 — `execute_sync(actions)` with progress `(2/200)`**

### [MODIFY] [summarizer_sync.py](file:///home/dsc/Codeberg/Summarizer/summarizer_sync.py)

Analogous two-phase sync. Source of truth = markdown files.

---

## Component 3: Metadata Enhancements

**Markdown files** get:
- `source_hash`: SHA-256 of Calibre source file
- `source_format`: `"pdf"`, `"epub"`, or `"markdown"`
- `converter_version`: `"pandoc-<ver> calibre_sync <ver>"` / `"docling-<ver> calibre_sync <ver>"` / `"calibre_sync <ver>"`

**Summary files** get:
- `source_md_hash`: SHA-256 of source markdown file

---

## Component 4: One-Time Migration

### [NEW] [migrate_filenames.py](file:///home/dsc/Codeberg/Summarizer/migrate_filenames.py)

- `--dry-run` (default): print rename plan
- `--execute`: perform renames with `os.rename`
- Scans markdown files, reads YAML frontmatter (uuid, title, authors)
- Computes new filename via `naming.generate_filename()`
- Checks for collisions → **abort if any**
- Renames markdown files and corresponding summary files
- Updates YAML frontmatter with new metadata fields
- **Removed after migration is complete**

---

## Implementation Order

| Step | Description |
|------|-------------|
| 1 | Create `naming.py` with algorithm + unit tests |
| 2 | Dry-run naming on all 3,101 Calibre books — verify uniqueness |
| 3 | Create `migrate_filenames.py` migration script |
| 4 | Execute migration on both repos (on new git branch) |
| 5 | Refactor `calibre_sync.py` — two-phase sync + metadata |
| 6 | Refactor `summarizer_sync.py` — two-phase sync |
| 7 | Update converter scripts for version strings |
| 8 | Update summarizer scripts for `source_md_hash` |
| 9 | End-to-end verification |
| 10 | Remove `migrate_filenames.py` |
