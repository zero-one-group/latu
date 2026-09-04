# Latu identity assets

Latu is Javanese for *spark* or *ember*. The identity is the letter ꦭ (la, U+A9AD) as the mark
and the word ꦭꦠꦸ (la‑ta‑suku, "latu") beside the Latin name as the lockup — two scripts, one
name, the way Yogyakarta's street signs pair them. All text is outlined; nothing here needs a
font installed.

## Files

| File | Use |
|---|---|
| `latu-lockup.svg` / `latu-lockup-dark.svg` | **Primary lockup**, `Latu \| ꦭꦠꦸ`, for light / dark grounds |
| `latu-lockup@2x.png` / `latu-lockup-dark@2x.png` | Same, 1200 px wide, for places that will not take SVG |
| `latu-lockup-with-mark*.svg` | Avatar tile + lockup, for a docs header or social card |
| `latu-lockup-stacked*.svg` | Latin over Javanese, for banners |
| `latu-mark.svg`, `-ondark`, `-mono`, `-white` | ꦭ alone; gradient for light / dark, flat #4B275F, white |
| `latu-avatar.svg`, `latu-avatar-{512,256,128}.png` | ꦭ on a tile — GitHub org/repo avatar, hex.pm |
| `favicon.svg`, `favicon.ico`, `favicon-{16,32,48}.png` | Favicon set (the tile) |

## README header

GitHub renders `<picture>` and honours the viewer's theme. The URLs are absolute because the
same README renders on hex.pm, where a relative `assets/...` path resolves to nothing:

```html
<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/zero-one-group/latu/main/assets/latu-lockup-dark.svg">
    <img alt="Latu" src="https://raw.githubusercontent.com/zero-one-group/latu/main/assets/latu-lockup.svg" width="360">
  </picture>
</p>
```

For ExDoc, point `logo:` in `docs/0` at `assets/latu-avatar.svg` (ExDoc shows a square).

## Rules of the system

- The Javanese and Latin share a **baseline**; the suku hangs below the line, as it does when
  Hanacaraka is set beside Latin. Do not centre the Javanese word on the Latin's x‑height.
- **Nothing goes above or below the letter ꦭ.** Those positions are vowel and consonant signs
  (cecak, cecak telu, wulu above; suku below). Decoration, if any, goes to the side.
- The mark uses Noto Sans Javanese **Bold** so it holds at 16 px; running Javanese text in the
  lockups uses **Regular** to match Outfit Medium.
- The avatar is the letter, never the word: ꦭꦠꦸ on a tile is unreadable at 32 px.
- The lockup image is **centred on the middle of the x‑height** — the band both scripts share —
  and the suku hangs below into the lower half. The descender never pushes the text up; the
  image carries matching blank space above the caps instead. An icon beside the lockup is
  centred on the middle of the Latin cap height, so it reads as level with “Latu”.

## Palette

| | Hex | Role |
|---|---|---|
| Ink | `#2E1A3D` | tile base, darkest text |
| Deep | `#4B275F` | Elixir's purple; flat mark, Latin wordmark on light |
| Mid | `#6E4A7E` | Elixir's second purple; Javanese text on light |
| Bright | `#8E5BB5` | gradient stop |
| Glow | `#B07DDB` | Javanese text on dark; gradient stop |
| Ember | `#D8A3F5` | glyph on the tile |

The mark's gradient runs Bright → Mid → Deep from the base up on light grounds ("lit from
below"), and Ember → Glow → Bright on dark grounds.

## Type

- Latin: [Outfit](https://fonts.google.com/specimen/Outfit) Medium (500), tracking −2/1000.
- Javanese: [Noto Sans Javanese](https://fonts.google.com/noto/specimen/Noto+Sans+Javanese)
  Bold (mark) and Regular (lockups). Both SIL Open Font License; outlines embedded in the SVGs.

## Regenerating

Every file here is drawn by a small Python generator (fontTools + uharfbuzz + cairosvg) from
the two fonts; sizes and colours are constants at the top of `build_final.py`. It is not in the
repo (the maintainer holds the `latu-identity.zip` pack); `dev/logo/` is the place if it is ever
wanted here.

## Why the script only appears as images

GitHub cannot load web fonts and macOS ships no Javanese font, so ꦭꦠꦸ typed into Markdown
renders as boxes for many readers. Every occurrence of the script in the README and the docs
is therefore an SVG with outlined text, never inline characters.
