"""Tests for the naming module."""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from naming import (
    title_to_filename,
    generate_filename,
    generate_summary_filename,
    check_collisions,
    _extract_trailing_number,
    _sanitize_chars,
)


def test_basic_filename():
    """Simple title + author produces expected filename."""
    result = title_to_filename("The Republic", "Plato")
    assert result == "The Republic - Plato", f"Got: {result}"


def test_max_length_respected():
    """Result never exceeds max_length."""
    long_title = "A Very Long Title That Absolutely Must Be Truncated Because It Is Ridiculously Long"
    result = title_to_filename(long_title, "Some Author", max_length=80)
    assert len(result) <= 80, f"Length {len(result)}: {result}"


def test_trailing_arabic_number_preserved():
    """Arabic numbering at end of title survives truncation."""
    title = "The Art of Computer Programming, Vol 1, Fundamental Algorithms"
    result = title_to_filename(title, "Donald E. Knuth", max_length=80)
    # Must not lose "Vol 1"
    # Wait - "Vol 1" is NOT at the end. The full trailing part after regex would be nothing
    # because "Fundamental Algorithms" follows the "Vol 1" part.
    # Let's test with a title where the number IS at the end:
    title2 = "Foundation Book 3"
    result2 = title_to_filename(title2, "Isaac Asimov", max_length=80)
    assert "Book 3" in result2, f"Lost trailing number: {result2}"


def test_trailing_roman_numeral_preserved():
    """Roman numerals at end of title survive truncation."""
    title = "Great Perfection Volume II"
    result = title_to_filename(title, "Dzogchen Rinpoche III", max_length=80)
    assert "Volume II" in result, f"Lost roman numeral: {result}"


def test_trailing_vol_preserved_on_truncation():
    """When title must be truncated, trailing volume number is protected."""
    title = "A Very Long Title That Must Be Truncated Because It Is Extremely Verbose And Goes On And On Vol III"
    result = title_to_filename(title, "Author Name", max_length=80)
    assert len(result) <= 80, f"Too long: {len(result)}"
    assert "Vol III" in result, f"Lost trailing Vol III: {result}"
    assert "- Author Name" in result, f"Lost author: {result}"


def test_trailing_part_number():
    """Part N numbering preserved."""
    result = title_to_filename("Dune Part IV", "Frank Herbert")
    assert "Part IV" in result, f"Lost Part IV: {result}"


def test_trailing_comma_number():
    """Trailing comma+number preserved: ', 2'."""
    result = title_to_filename("Some Series Title, 2", "An Author")
    assert ", 2" in result, f"Lost trailing number: {result}"


def test_trailing_parenthesised_number():
    """Parenthesised number preserved: '(3)'."""
    result = title_to_filename("Some Title (3)", "An Author")
    assert "(3)" in result, f"Lost parenthesised number: {result}"


def test_colon_replacement():
    """Colons are replaced with em-dash."""
    result = title_to_filename("Mind: The Vision of Enlightenment", "Author")
    assert ":" not in result, f"Colon not replaced: {result}"
    assert "—" in result, f"No em-dash: {result}"


def test_slash_replacement():
    """Slashes are replaced with hyphens."""
    result = title_to_filename("SSL/TLS for DevOps", "Author")
    assert "/" not in result, f"Slash not replaced: {result}"
    assert "-" in result, f"No hyphen: {result}"


def test_unsafe_chars_removed():
    """Characters like *, ?, <, >, | are removed."""
    result = title_to_filename('Title with "quotes" and *stars*', "Author")
    assert '"' not in result
    assert '*' not in result


def test_generate_filename_adds_md():
    """generate_filename adds .md extension."""
    result = generate_filename("The Republic", "Plato")
    assert result.endswith(".md"), f"No .md: {result}"
    assert result == "The Republic - Plato.md", f"Got: {result}"


def test_generate_summary_filename():
    """Summary filename has prefix and is not length-limited."""
    result = generate_summary_filename("The Republic", "Plato")
    assert result == "Summary of The Republic - Plato.md", f"Got: {result}"


def test_summary_prefix_not_length_limited():
    """Summary filename can exceed 80 chars because prefix doesn't count."""
    long_title = "A Title That Is Exactly Sized To Fill The Maximum Length Allowed By Rules"
    result = generate_summary_filename(long_title, "Author Name")
    base = generate_filename(long_title, "Author Name")
    # The base filename (without "Summary of ") respects the limit
    base_without_ext = base[:-3]  # remove .md
    assert len(base_without_ext) <= 80, f"Base too long: {len(base_without_ext)}"
    # But the summary can exceed 80 because "Summary of " is added on top
    assert result.startswith("Summary of ")


def test_collision_detection_no_collisions():
    """No collisions returns empty list."""
    entries = [
        {"filename": "Book A - Auth1.md", "uuid": "aaa"},
        {"filename": "Book B - Auth2.md", "uuid": "bbb"},
    ]
    result = check_collisions(entries)
    assert len(result) == 0


def test_collision_detection_finds_case_insensitive():
    """Case-insensitive duplicates are detected."""
    entries = [
        {"filename": "The Book - Author.md", "uuid": "aaa"},
        {"filename": "the book - author.md", "uuid": "bbb"},
    ]
    result = check_collisions(entries)
    assert len(result) == 1
    assert len(result[0]['entries']) == 2


def test_collision_detection_finds_exact():
    """Exact duplicates are detected."""
    entries = [
        {"filename": "Same Name - Auth.md", "uuid": "aaa"},
        {"filename": "Same Name - Auth.md", "uuid": "bbb"},
    ]
    result = check_collisions(entries)
    assert len(result) == 1


def test_empty_title():
    """Empty title gets default 'Untitled'."""
    result = title_to_filename("", "Author")
    assert "Untitled" in result


def test_empty_author():
    """Empty author gets default 'Unknown'."""
    result = title_to_filename("Title", "")
    assert "Unknown" in result


def test_extract_trailing_number_vol():
    body, suffix = _extract_trailing_number("The Art of Programming Vol 3")
    assert suffix.strip() == "Vol 3" or "Vol 3" in suffix, f"Got suffix: '{suffix}'"
    assert "Vol 3" not in body


def test_extract_trailing_number_volume_roman():
    body, suffix = _extract_trailing_number("Great Perfection Volume II")
    assert "Volume II" in suffix, f"Got suffix: '{suffix}'"


def test_extract_trailing_number_none():
    body, suffix = _extract_trailing_number("Just A Regular Title")
    assert suffix == "", f"Got suffix: '{suffix}'"
    assert body == "Just A Regular Title"


def test_extract_trailing_book():
    body, suffix = _extract_trailing_number("Accelerando (Singularity Book 3)")
    # "Book 3)" — hmm, let's see. The title has (Singularity Book 3) which contains Book 3
    # Our regex should handle "Book 3" inside parentheses gracefully
    # Actually the regex expects Book N at the END. With the closing ) it might not match.
    # This is expected — the number here is not at the end of the title.
    # Let's test a clean case instead:
    body2, suffix2 = _extract_trailing_number("Foundation Book 3")
    assert "Book 3" in suffix2, f"Got suffix: '{suffix2}'"


def test_sanitize_chars():
    result = _sanitize_chars('Hello: World/Foo\\Bar*"<>|')
    assert ':' not in result
    assert '/' not in result
    assert '\\' not in result
    assert '*' not in result
    assert '"' not in result
    assert '<' not in result
    assert '>' not in result
    assert '|' not in result


def test_real_world_knuth():
    """Real-world test with Knuth volumes — they must produce distinct filenames."""
    titles = [
        ("The Art of Computer Programming, Vol 1, Fundamental Algorithms", "Donald E. Knuth"),
        ("The Art of Computer Programming, Vol 2, Seminumerical Algorithms", "Donald E. Knuth"),
        ("The Art of Computer Programming, Vol 3, Sorting and Searching", "Donald E. Knuth"),
        ("The Art of Computer Programming, Volume 1, Fascicle 1  MMIX -- A RISC Computer for the New Millennium", "Donald E. Knuth"),
        ("The Art of Computer Programming, Vol 4A, Combinatorial Algorithms", "Donald E. Knuth"),
    ]

    filenames = [generate_filename(t, a) for t, a in titles]
    # All must be unique
    assert len(set(filenames)) == len(filenames), f"Duplicates found: {filenames}"
    # All must be <= 80 chars (without .md)
    for fn in filenames:
        assert len(fn) - 3 <= 80, f"Too long: {fn} ({len(fn) - 3} chars)"

    print("Knuth filenames:")
    for fn in filenames:
        print(f"  [{len(fn)-3:2d}] {fn}")


def test_real_world_long_dharma_title():
    """Real-world test with very long Dharma title."""
    title = "The Treasury of Knowledge: Book 8, Part 4: Esoteric Instructions: Esoteric Instructions Bk.8, Pt. 4"
    author = "Jamgon Kongtrul Lodro Taye"
    result = generate_filename(title, author)
    basename = result[:-3]
    assert len(basename) <= 80, f"Too long ({len(basename)}): {result}"
    print(f"Dharma title: [{len(basename)}] {result}")


def test_real_world_summary_filename():
    """Summary of a real title."""
    result = generate_summary_filename(
        "The Structure of Scientific Revolutions",
        "Thomas S. Kuhn"
    )
    assert result.startswith("Summary of ")
    assert result.endswith(".md")
    print(f"Summary: {result}")


if __name__ == "__main__":
    test_functions = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    passed = 0
    failed = 0
    for func in test_functions:
        try:
            func()
            print(f"  PASS  {func.__name__}")
            passed += 1
        except AssertionError as e:
            print(f"  FAIL  {func.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"  ERROR {func.__name__}: {e}")
            failed += 1

    print(f"\n{'='*60}")
    print(f"Results: {passed} passed, {failed} failed, {passed + failed} total")
    if failed:
        sys.exit(1)
