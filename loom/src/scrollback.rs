#![allow(dead_code)]

// This module will maintain a 24h rolling cache of signed message blocks;
// when a peer joins a channel it requests scrollback from connected peers and
// deduplicates by message signature.
