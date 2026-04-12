"""
Dry-run: Generate filenames for all books in the Calibre library
and check for collisions.
"""

import os
import sys
import xml.etree.ElementTree as ET

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from naming import generate_filename, generate_summary_filename, check_collisions


def parse_title_author_uuid(opf_path: str):
    """Extract title, first author, and UUID from a Calibre metadata.opf file."""
    try:
        root = ET.parse(opf_path).getroot()
    except Exception as e:
        print(f"  ERROR parsing {opf_path}: {e}", file=sys.stderr)
        return None

    ns = {
        "opf": "http://www.idpf.org/2007/opf",
        "dc": "http://purl.org/dc/elements/1.1/",
    }

    metadata = root.find("opf:metadata", ns)
    if metadata is None:
        return None

    title_el = metadata.find("dc:title", ns)
    title = str(title_el.text) if title_el is not None and title_el.text else ""

    # First author with role="aut"
    author = ""
    for creator in metadata.findall("dc:creator", ns):
        if "{http://www.idpf.org/2007/opf}role" in creator.attrib:
            if creator.attrib["{http://www.idpf.org/2007/opf}role"] == "aut":
                author = str(creator.text) if creator.text else ""
                break

    uuid = ""
    for identifier in metadata.findall("dc:identifier", ns):
        if identifier.attrib.get("id") == "uuid_id":
            uuid = str(identifier.text) if identifier.text else ""
            break

    return {"title": title, "author": author, "uuid": uuid, "opf_path": opf_path}


def main():
    calibre_path = os.path.expanduser("~/ReferenceLibrary/Calibre Library")
    if not os.path.isdir(calibre_path):
        print(f"Calibre library not found at: {calibre_path}")
        sys.exit(1)

    print(f"Scanning Calibre library: {calibre_path}")
    print()

    # Find all metadata.opf files
    entries = []
    for root_dir, dirs, files in os.walk(calibre_path):
        if '.caltrash' in root_dir:
            continue
        if "metadata.opf" in files:
            opf_path = os.path.join(root_dir, "metadata.opf")
            info = parse_title_author_uuid(opf_path)
            if info and info["title"]:
                filename = generate_filename(info["title"], info["author"])
                summary_fn = generate_summary_filename(info["title"], info["author"])
                entries.append({
                    "filename": filename,
                    "summary_filename": summary_fn,
                    "uuid": info["uuid"],
                    "title": info["title"],
                    "author": info["author"],
                    "opf_path": info["opf_path"],
                })

    print(f"Total books parsed: {len(entries)}")
    print()

    # Check filename collisions
    collisions = check_collisions(entries)
    if collisions:
        print(f"*** COLLISIONS FOUND: {len(collisions)} ***")
        print()
        for group in collisions:
            print(f"  Collision: {group['filename']}")
            for e in group['entries']:
                print(f"    UUID: {e['uuid']}")
                print(f"    Title: {e.get('title', 'N/A')}")
                print(f"    Author: {e.get('author', 'N/A')}")
                print(f"    OPF: {e.get('opf_path', 'N/A')}")
                print()
    else:
        print("No filename collisions detected.")

    # Also check summary filename collisions
    summary_entries = [{"filename": e["summary_filename"], "uuid": e["uuid"],
                        "title": e["title"], "author": e["author"],
                        "opf_path": e["opf_path"]} for e in entries]
    summary_collisions = check_collisions(summary_entries)
    if summary_collisions:
        print(f"\n*** SUMMARY FILENAME COLLISIONS: {len(summary_collisions)} ***")
        for group in summary_collisions:
            print(f"  Collision: {group['filename']}")
            for e in group['entries']:
                print(f"    Title: {e.get('title', 'N/A')} by {e.get('author', 'N/A')}")
    else:
        print("No summary filename collisions detected.")

    # Length statistics
    lengths = [len(e["filename"]) - 3 for e in entries]  # minus .md
    print(f"\nFilename length stats (without .md):")
    print(f"  Min: {min(lengths)}")
    print(f"  Max: {max(lengths)}")
    print(f"  Avg: {sum(lengths)/len(lengths):.1f}")
    print(f"  Over 80: {sum(1 for l in lengths if l > 80)}")

    # Show longest filenames
    entries_sorted = sorted(entries, key=lambda e: len(e["filename"]), reverse=True)
    print(f"\nTop 10 longest filenames:")
    for e in entries_sorted[:10]:
        fn = e["filename"]
        print(f"  [{len(fn)-3:3d}] {fn}")

    # Show shortest filenames
    print(f"\nTop 10 shortest filenames:")
    for e in entries_sorted[-10:]:
        fn = e["filename"]
        print(f"  [{len(fn)-3:3d}] {fn}")


if __name__ == "__main__":
    main()
