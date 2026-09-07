# Vendored SQLite pin

The signer database uses SQLite **3.53.4** from the official amalgamation:

- Download page: <https://sqlite.org/download.html>
- Artifact URL: <https://sqlite.org/2026/sqlite-amalgamation-3530400.zip>
- SHA3-256: `628a44cfe82c66aed1ccbbe85a562d2e33ebe64b3288981ed76285612227934e`

The hash above is the archive hash published by sqlite.org on the download page. Do not update the
URL, version, or hash independently.

## WAL-reset safety floor

The SQLite [release history](https://sqlite.org/changes.html) records that **3.51.3**, released
2026-03-13, fixes the WAL-reset database corruption bug. That release is the first version with the
fix. This deployment deliberately enforces the newer pinned version, 3.53.4, as the minimum runtime
for every process that opens the signer database. The 3.53.0 release notes also explicitly list the
same fix after the withdrawn 3.52.0 release.
