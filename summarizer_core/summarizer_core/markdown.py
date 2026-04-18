import yaml

def split_header_content(text: str) -> tuple[str, str]:
    if not text.startswith("---\n"):
        return ("", text)
    parts = text.split("---\n", 2)
    if len(parts) < 3:
        return ("", text)
    return (parts[1], parts[2])

def parse_markdown(md_text: str):
    frontmatter, content = split_header_content(md_text)
    try:
        yaml_metadata = yaml.safe_load(frontmatter) if frontmatter else {}
    except Exception:
        yaml_metadata = {}
    return yaml_metadata, content

def assemble_markdown(metadata, md_text: str) -> str:
    if metadata is None:
        return md_text
        
    filtered_metadata = {}
    for k, v in metadata.items():
        if isinstance(v, list) and len(v) == 0:
            continue
        if isinstance(v, str) and v == "":
            continue
        filtered_metadata[k] = v
        
    header = yaml.dump(filtered_metadata, default_flow_style=False, indent=2)
    if not header.endswith("\n"):
        header += "\n"
    return f"---\n{header}---\n{md_text}"

def get_answer_from_output(text: str) -> str:
    if "<channel|>" in text:
        return text.split("<channel|>")[-1].strip()
    return text
