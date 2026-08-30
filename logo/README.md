# ORM++ Logo

Ein **Datenbank-Zylinder aus gestapelten Scheiben** — die Scheiben sind das
append-only Event-Log, der cyanfarbene Deckel die aktuelle Projektion, auf die
die Anwendung schaut. Das hochgestellte **`++`** steht für das, was ORM++ über
klassisches ORM-Mapping hinaus mitbringt: Event Sourcing, Projektionen,
Snapshots und Expand/Contract-Migrationen.

| Farbe   | Wert      | Verwendung                                  |
|---------|-----------|---------------------------------------------|
| Tinte   | `#0F2333` | Zylinderkörper (helle Hintergründe)         |
| Cyan    | `#19B6C7` | Deckel, `++`, Nähte                         |
| Cyan-Hell | `#3ED0DF` | Deckel und `++` auf dunklen Hintergründen |
| Nebel   | `#E3EDF3` | Zylinderkörper (dunkle Hintergründe)        |

## Dateien

- `ormpp-icon-light.svg/.png` — für **helle** Hintergründe (dunkler Körper)
- `ormpp-icon-dark.svg/.png` — für **dunkle** Hintergründe (heller Körper)
- `ormpp-icon-neutral.svg/.png` — **einfarbig**; das SVG nutzt `currentColor`
  und übernimmt damit die Textfarbe des Umfelds (das PNG ist in `#64748B`
  gerastert). Für Druck, Stempel, Favicons und überall dort, wo nur eine Farbe
  zur Verfügung steht.
- `ormpp-wordmark-{light,dark,neutral}.svg/.png` — Zylinder **plus Schriftzug**
  „ORM++", für Kopfzeilen, READMEs und Präsentationen
- `ormpp-social-preview.svg/.png/.webp` — Vorschaubild für GitLab/GitHub und
  Social Media (1280×640, deckender Hintergrund)

Icons sind quadratisch (512×512 im SVG, 1024×1024 als PNG), Wortmarken 424×128
bzw. 1696×512 — alle maßstabsfrei und mit transparentem Hintergrund.

Der Schriftzug der Wortmarke ist echter Text in **Avenir Next Bold** (Fallback
Helvetica Neue/Helvetica/Arial). Wo diese Schriften fehlen, rendert das SVG mit
der nächstbesten Groteske; die PNGs sind davon nicht betroffen.

## Verwendung

Immer die Variante wählen, die zum Hintergrund passt — der zweifarbige Zylinder
verliert sonst seine Silhouette. Bei unbekanntem oder wechselndem Hintergrund
(Dark-Mode-Umschaltung) ist `neutral` die sichere Wahl.

Das SVG nicht nachfärben oder verzerren; für kleine Größen (< 32 px) das PNG in
der passenden Auflösung rastern statt das SVG zu skalieren.

## Neu rendern

Die PNGs entstehen aus den SVGs mit [sharp](https://sharp.pixelplumbing.com):

```js
sharp(svg, { density: 384 }).resize(1024, 1024).png().toFile(out)
```

Die Social Preview zusätzlich auf exakt 1280×640 begrenzen — ohne `resize`
skaliert die Renderdichte das Bild über die Zielgröße hinaus.
