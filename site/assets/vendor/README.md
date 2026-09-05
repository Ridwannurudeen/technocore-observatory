# Motion mini vendor bundle

`motion-13.1.1.min.js` is a classic IIFE exposing `Motion.animate`. It is built
from the `motion/mini` entry point and has no runtime imports or production
dependency on Node, React, npm, or a CDN.

Pinned npm inputs and published registry integrities:

- `motion@13.1.1`: `sha512-WNZoK6xiF+kkTqkZ5K7FDDh6A8BG4i5Hc7KXtW8gtTxkpJFds+hIOrDaQGKjQj/AE/i4hJqAaUHEqp/Qo02y6Q==`
- `framer-motion@13.1.1`: `sha512-B/xn2TPS4f61cEBLFjiYlQFnBZUW1YVj/LM+C+N4OP8Rs95VLEI2ot/RlfBg111la/EiyECFaJJi/A3FWA8MUA==`
- `motion-dom@13.1.1`: `sha512-XSf8VYWSB6G/0IY3rWVbyLcxWXtAVHkN1PQE2agTaCv3u8RGvbwu56TyyR/MNzBqqNavEBTZzErcxI1TxBrjcA==`
- `motion-utils@13.0.0`: `sha512-7DnN7TmbLcYXcG4RVadXIihWlyuM9afoUww8Y5Agg431kGKiuL2/OMyP4mJ5wLz+pvN3t5ySClLOaVXJ+wekRQ==`
- `esbuild@0.28.1` (build only): `sha512-HrJrvZv5ayxBzPfwphOoNzkzOIIlifzk0KJrGK2c8R4+LKpMtpYLQeUdjnwjWv/LZlkH2laZk+4w78pi99D4Vw==`

Bundle SHA-256:
`2b9b37fd2b8ebfac996f4d9a94f14360dc7bf9140d7a87cf79ff112af2307932`

Bundle size: 8,310 bytes raw; 3,352 bytes with `gzip -9`.

Rebuild in a disposable directory, never the repository root:

```sh
npm init -y
npm install --ignore-scripts --no-audit --no-fund --save-exact \
  motion@13.1.1 framer-motion@13.1.1 motion-dom@13.1.1 \
  motion-utils@13.0.0 tslib@2.8.1 esbuild@0.28.1
printf '%s\n' 'export { animate } from "motion/mini";' > entry.js
./node_modules/.bin/esbuild entry.js --bundle --format=iife \
  --global-name=Motion --minify --target=es2020 --legal-comments=external \
  --outfile=motion-13.1.1.min.js
sha256sum motion-13.1.1.min.js
```

The adjacent `MOTION-LICENSE.txt` must remain with redistributed copies.
