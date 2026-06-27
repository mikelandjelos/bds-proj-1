# Presentation

This Markdown/Marp deck is deprecated. The canonical presentation source is now the LaTeX Beamer file:

- `docs/presentation.tex`

Build the PDF from the repository root:

```bash
build_dir="$(mktemp -d)"
lualatex -interaction=nonstopmode -halt-on-error \
  -output-directory "$build_dir" docs/presentation.tex
lualatex -interaction=nonstopmode -halt-on-error \
  -output-directory "$build_dir" docs/presentation.tex
cp "$build_dir/presentation.pdf" presentation.pdf
rm -rf "$build_dir"
```

This keeps LaTeX auxiliary files out of the repository.

The deck uses standard Beamer `Madrid` with the `wolverine` color theme.
The reusable personal package `mihajlo-elfak-logos`, installed under
`~/texmf/tex/latex/mihajlo-elfak-logos/`, adds the two ELFak corner logos,
the non-title-slide footer, and the faculty website palette. The logos are
shown only on the title slide.
A new presentation only needs:

```tex
\documentclass[aspectratio=169,11pt]{beamer}
\usetheme{Madrid}
\usecolortheme{wolverine}
\usepackage{mihajlo-elfak-logos}

\title{Projekat I}
\subtitle{Project name}
\author[Name and index]{Name and index}
\institute{Big Data Systems}
\date{2026}
```
