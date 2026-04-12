Please plan the following refactoring:

1. Naming conventions

- Make file naming suitable for wiki-linking. Currently the filenames for both calibre to markdown and markdown to summaries are identical and simply a mix of start of title and author. This has the following disadvantages:
  - both cannot be discriminated, they have same wiki-link [[filename]]
  - the shorting strategy is not unique (e.g. long-title-vol 1 and long-title-vol 2  might be shortened to the same filename)

Please create a naming strategy that is suitable for wiki-linking and ensures that:
- The filename is based on the metadata 'title'.
- Maximum lenght is shortened to 64 characters.
- If the title ends with numbering (either arabic or roman numbering), the numbering is preserved at the end of the filename.
- Special characters that cannot be used in filenames on all operating systems have to be replaced in a way that looks good in title.

Once the naming strategy is defined, test it on the calibre library to verify that all books have a unique filename. Also take into account that some filesystems cannot discriminate between upper and lower case.

If at a later point a book is added to calibre that has the same title as an existing book, the converter should abort with an error and request renaming of the calibre source.

Filenames for the summaries get the prefix "Summary of ". So a filename for a markdown-conversion of "The Structure of Scientific Revolutions" is "The Structure of Scientific Revolutions.md" and the summary is "Summary of The Structure of Scientific Revolutions.md".

We need an upgrade-procedure that looks for existing markdown-files and summaries and renames them according to the new naming strategy.

Note: both the markdown-files and the summaries are stored in GIT repositories.

2. Synchronisation

Currently changes to the source of truth (the Calibre librar) are not recognized.

Changes to the calibre library include:
- adding or removing books
- adding or removing representations of books (e.g. pdf, epub, md), which would cause a different format being the source of conversion
- metadata changes (e.g. title, author, series, tags, etc.)

The uuid in the calibre metadata is the unchanging identifier of a book. It is stored in the metadata of the markdown file and in the metadata of the summary.

A sha256 hash is used on the calibre file that is used as source for the conversion. This hash is stored in the metadata of the markdown file. If the hash changes, the markdown file is regenerated.
If the metadata changes, the markdown file is not regenerated, but it might require renaming (title) or moving (series).

The summaries metadata should contain a hash of the markdown file that was used to generate the summary. If the hash changes, the summary is regenerated.

Filename-changes and moves should be verified via the uuid of the book. The uuid is stored in the metadata of the markdown file and in the metadata of the summary. If a markdown file is renamed or moved, the summary should be renamed or moved accordingly.
Markdown files are the source of truth for summaries.

2.1 Two-path synchronisation

2.1.1 Markdown

First, collect the current state of the markdown files.

Then read the calibre library and use the UUID to match the markdown files to the books in the calibre library.

- If a markdown file is not found in the calibre library, it should be removed.
- If a markdown file is found in the calibre library, but the hash does not match, the markdown file should be regenerated.
- If the metadata of a markdown file has changed, the markdown file should be renamed or moved accordingly. The metadata is updated to the version of the calibre library, which is the source of truth.

First path just generates a list of actions to be performed. This way, a progress information (e.g. 2/200) can be displayed.

2.1.2 Summaries

Analog to 2.1.1, but for summaries. The source of truth is the markdown file.

3. Upgrade procedure

The code both for calibre-to-markdown and markdown-to-summary conversion should be upgraded to use the new naming strategy and the synchronisation procedure.

The upgrade-code is one-time use and should be removed after the upgrade.

4. Metadata changes

4.1 The markdown files should aget the following additional information:

- sha256 hash of the calibre file that was used to generate the markdown file
- The format that is used to produce the markdown file (e.g. pdf, epub, md)
- The converter_version entry should be changed: if pandoc is used, the version should be `pandoc-<pandoc_version> <calibre_to_markdown_converter_version>`, if docling is used, the version should be `docling-<docling_version> <calibre_to_markdown_converter_version>`. This information might be used at a later point to regenerate the markdown file with a different converter.

4.2 The summaries should get the following additional information:

- sha256 hash of the markdown file that was used to generate the summary
