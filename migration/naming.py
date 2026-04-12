"""
Naming strategy for wiki-compatible filenames.

Generates deterministic filenames from Calibre metadata (title + author).
Filenames are capped at 80 characters (before .md extension), with trailing
numbering (arabic or roman) preserved during truncation.

Collision detection is case-insensitive; any collision is a fatal error
requiring user intervention in Calibre metadata.
"""

import re
import hashlib
import logging

logger = logging.getLogger(__name__)

# Regex for trailing numbering patterns in titles.
# Matches patterns like:
#   Vol 3, Vol. 3, Volume III, Part 12, Book IV, Bd. 7, Nr. 14,
#   Band 3, Bk. 4, Pt. 2, (3), , 2
# Must appear at the end of the title string.
TRAILING_NUMBER_RE = re.compile(
    r'('
    r'(?:,\s*)?'                    # optional leading comma+space
    r'(?:'
    r'(?:Vol(?:ume)?|Part|Book|Bd|Nr|Band|Bk|Pt|Tome|Heft|Chapter|Ch|No)'
    r'\.?\s+'                       # keyword + required space
    r'(?:[IVXLCDM]+|\d+)'          # roman or arabic number
    r'|'
    r'\(\d+\)'                     # parenthesised number like (3)
    r'|'
    r',\s*\d+'                     # trailing comma + number like , 2
    r')'
    r')\s*$',
    re.IGNORECASE
)

# Characters that are unsafe on Windows, macOS, or Linux filesystems
UNSAFE_CHARS_RE = re.compile(r'[*?"<>|]')


def _sanitize_chars(text: str) -> str:
    """Replace or remove filesystem-unsafe characters in a visually pleasing way."""
    # Replace colon with em-dash (looks good in titles)
    text = text.replace(':', ' —')
    # Replace slashes with hyphens
    text = text.replace('/', '-')
    text = text.replace('\\', '-')
    # Remove remaining unsafe characters
    text = UNSAFE_CHARS_RE.sub('', text)
    # Collapse multiple spaces
    text = re.sub(r'\s{2,}', ' ', text)
    # Strip leading/trailing whitespace and dots
    text = text.strip(' .')
    return text


def _extract_trailing_number(title: str) -> tuple[str, str]:
    """
    Split a title into body and trailing number suffix.

    Returns:
        (title_body, number_suffix) where number_suffix may be empty.
    """
    match = TRAILING_NUMBER_RE.search(title)
    if match:
        suffix = match.group(0)
        body = title[:match.start()]
        return body, suffix
    return title, ''


def _truncate_at_word_boundary(text: str, max_length: int) -> str:
    """
    Truncate text to at most max_length characters, cutting at the last
    word boundary. Strips trailing punctuation and whitespace.
    """
    if len(text) <= max_length:
        return text

    truncated = text[:max_length]
    # Try to cut at the last space to avoid mid-word truncation
    last_space = truncated.rfind(' ')
    if last_space > max_length // 2:  # only use word boundary if reasonable
        truncated = truncated[:last_space]

    # Strip trailing punctuation that looks bad at end of a truncated title
    truncated = truncated.rstrip(' ,;:_-—')
    return truncated


def title_to_filename(title: str, author: str, max_length: int = 80) -> str:
    """
    Generate a filesystem-safe, wiki-linkable filename from title and author.

    The result is at most max_length characters (before .md extension).
    Trailing numbering in the title is preserved even if the title body
    must be truncated.

    Args:
        title: The book title from Calibre metadata.
        author: The first author name from Calibre metadata.
        max_length: Maximum length of the returned string (default 80).

    Returns:
        A sanitized filename string without extension.
    """
    if not title:
        title = "Untitled"
    if not author:
        author = "Unknown"

    # Step 1: extract trailing numbering from the title before sanitization
    title_body, number_suffix = _extract_trailing_number(title)

    # Step 2: sanitize all parts
    title_body = _sanitize_chars(title_body)
    number_suffix = _sanitize_chars(number_suffix)
    author = _sanitize_chars(author)

    # Step 3: form the author suffix
    author_suffix = f" - {author}"

    # Step 4: compute space budget
    # Protected parts: number_suffix + author_suffix
    protected_length = len(number_suffix) + len(author_suffix)
    available_for_title = max_length - protected_length

    if available_for_title < 10:
        # If the author + number suffix is so long that there's almost no room
        # for the title, truncate the author instead
        author_suffix = f" - {_truncate_at_word_boundary(author, 20)}"
        protected_length = len(number_suffix) + len(author_suffix)
        available_for_title = max_length - protected_length

    # Step 5: truncate title body if necessary
    if len(title_body) > available_for_title:
        title_body = _truncate_at_word_boundary(title_body, available_for_title)

    # Step 6: reassemble
    result = f"{title_body}{number_suffix}{author_suffix}"

    # Final safety: ensure we don't exceed max_length
    if len(result) > max_length:
        # This shouldn't happen with proper logic above, but as a safeguard
        result = result[:max_length].rstrip(' ,;:_-—')

    return result


def generate_filename(title: str, author: str, max_length: int = 80) -> str:
    """
    Generate the full markdown filename including .md extension.

    Args:
        title: The book title from Calibre metadata.
        author: The first author name from Calibre metadata.
        max_length: Maximum length of the basename (without .md).

    Returns:
        Filename with .md extension.
    """
    return title_to_filename(title, author, max_length) + ".md"


def generate_summary_filename(title: str, author: str, max_length: int = 80) -> str:
    """
    Generate the summary filename with "Summary of " prefix.

    The "Summary of " prefix is NOT subject to the max_length limit.
    The limit only applies to the base filename generated by generate_filename().

    Args:
        title: The book title from Calibre metadata.
        author: The first author name from Calibre metadata.
        max_length: Maximum length of the base filename (without .md and prefix).

    Returns:
        Summary filename like "Summary of Title - Author.md"
    """
    return "Summary of " + generate_filename(title, author, max_length)


def check_collisions(entries: list[dict]) -> list[dict]:
    """
    Check for case-insensitive filename collisions.

    Args:
        entries: List of dicts with at least 'filename' and 'uuid' keys.

    Returns:
        List of collision groups. Each group is a dict with:
        - 'filename': the normalised (lowercased) filename
        - 'entries': list of the colliding entries
        Empty list means no collisions.
    """
    index: dict[str, list[dict]] = {}
    for entry in entries:
        key = entry['filename'].lower()
        if key not in index:
            index[key] = []
        index[key].append(entry)

    collisions = []
    for key, group in index.items():
        if len(group) > 1:
            collisions.append({
                'filename': key,
                'entries': group
            })

    return collisions


def compute_file_hash(filepath: str) -> str:
    """
    Compute SHA-256 hash of a file's contents.

    Args:
        filepath: Path to the file.

    Returns:
        Hex-encoded SHA-256 hash string.
    """
    sha256 = hashlib.sha256()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            sha256.update(chunk)
    return sha256.hexdigest()

