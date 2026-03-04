# Installing the Lattice Firefox Extension

## Load the extension

1. Open Firefox and go to about:debugging
2. Click "This Firefox"
3. Click "Load Temporary Add-on"
4. Select lattice-ext/manifest.json

The setup page will open automatically.

## One-time Firefox config

Firefox needs to know .loom is a URL, not a search query.
The setup page walks you through it - takes about 30 seconds.

Or do it manually:
1. Go to about:config
2. Search: browser.fixup.domainsuffixwhitelist.loom
3. Click + to create it (defaults to true)

## Requirements

- lattice-daemon must be running on the default port (7779/7780/7781)
- Firefox 91.1.0 or later

## Usage

Type any .loom address in the Firefox address bar:
  lattice.loom
  http://lattice.loom
