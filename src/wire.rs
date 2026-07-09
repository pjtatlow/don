//! Binary framing for the `GET /logstream` endpoint.
//!
//! The daemon streams formatted log lines to attached `don tui` frontends at
//! high volume. JSON-per-line would pay serialization + base64/array cost on
//! the hottest path in the system, so instead each [`FormattedLogLine`] is
//! written as a compact length-prefixed binary frame. The bytes are already
//! fully formatted (color prefix + optional timestamp), so the client renders
//! them verbatim — framing is the only overhead.
//!
//! Frames travel inside the HTTP chunked body, but chunk boundaries are
//! independent of frame boundaries: [`LogFrameDecoder`] buffers raw bytes and
//! yields whole frames as they complete.
//!
//! Frame layout (all integers big-endian):
//!
//! ```text
//! ┌───────────┬────────────┬──────────────┬──────────┬───────────────┐
//! │ name_len  │ bytes_len  │ is_lifecycle │  name    │  formatted     │
//! │  u32      │   u32      │     u8       │  …       │  bytes …       │
//! └───────────┴────────────┴──────────────┴──────────┴───────────────┘
//! ```

use crate::output::FormattedLogLine;

/// Fixed frame header size: `name_len` (4) + `bytes_len` (4) + `is_lifecycle` (1).
const HEADER_LEN: usize = 9;

/// Encode one formatted log line as a self-describing binary frame.
pub(crate) fn encode_log_frame(line: &FormattedLogLine) -> Vec<u8> {
    let name = line.name.as_bytes();
    let mut out = Vec::with_capacity(HEADER_LEN + name.len() + line.bytes.len());
    out.extend_from_slice(&(name.len() as u32).to_be_bytes());
    out.extend_from_slice(&(line.bytes.len() as u32).to_be_bytes());
    out.push(u8::from(line.is_lifecycle));
    out.extend_from_slice(name);
    out.extend_from_slice(&line.bytes);
    out
}

/// Incremental decoder for the log frame stream. Push raw body bytes as they
/// arrive (in any chunking), then drain whole frames via [`Self::next_frame`].
#[derive(Default)]
pub(crate) struct LogFrameDecoder {
    buf: Vec<u8>,
}

impl LogFrameDecoder {
    /// Append freshly received bytes.
    pub(crate) fn push(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// Pop the next complete frame, or `None` if more bytes are needed.
    pub(crate) fn next_frame(&mut self) -> Option<FormattedLogLine> {
        if self.buf.len() < HEADER_LEN {
            return None;
        }
        // Header reads are infallible: the length check above guarantees 9 bytes.
        let name_len = u32::from_be_bytes([self.buf[0], self.buf[1], self.buf[2], self.buf[3]])
            as usize;
        let bytes_len = u32::from_be_bytes([self.buf[4], self.buf[5], self.buf[6], self.buf[7]])
            as usize;
        let is_lifecycle = self.buf[8] != 0;
        let total = HEADER_LEN + name_len + bytes_len;
        if self.buf.len() < total {
            return None;
        }

        let frame: Vec<u8> = self.buf.drain(..total).collect();
        let name_end = HEADER_LEN + name_len;
        let name = String::from_utf8_lossy(&frame[HEADER_LEN..name_end]).into_owned();
        let bytes = frame[name_end..total].to_vec();
        Some(FormattedLogLine {
            name,
            is_lifecycle,
            bytes,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn line(name: &str, is_lifecycle: bool, bytes: &[u8]) -> FormattedLogLine {
        FormattedLogLine {
            name: name.to_string(),
            is_lifecycle,
            bytes: bytes.to_vec(),
        }
    }

    #[test]
    fn round_trips_a_single_frame() {
        let original = line("api", false, b"\x1b[36mapi\x1b[0m | listening on :3000");
        let mut decoder = LogFrameDecoder::default();
        decoder.push(&encode_log_frame(&original));
        let decoded = decoder.next_frame().unwrap();
        assert_eq!(decoded.name, original.name);
        assert_eq!(decoded.is_lifecycle, original.is_lifecycle);
        assert_eq!(decoded.bytes, original.bytes);
        assert!(decoder.next_frame().is_none());
    }

    #[test]
    fn decodes_frames_split_across_pushes() {
        let frames: Vec<FormattedLogLine> = vec![
            line("db", true, b"[don] db: stopping"),
            line("", false, b"raw lifecycle line"),
            line("worker", false, b"job done"),
        ];
        let mut wire = Vec::new();
        for f in &frames {
            wire.extend_from_slice(&encode_log_frame(f));
        }

        // Feed one byte at a time — the worst-case chunk boundary.
        let mut decoder = LogFrameDecoder::default();
        let mut decoded = Vec::new();
        for byte in wire {
            decoder.push(&[byte]);
            while let Some(frame) = decoder.next_frame() {
                decoded.push(frame);
            }
        }
        assert_eq!(decoded.len(), frames.len());
        for (got, want) in decoded.iter().zip(&frames) {
            assert_eq!(got.name, want.name);
            assert_eq!(got.is_lifecycle, want.is_lifecycle);
            assert_eq!(got.bytes, want.bytes);
        }
    }

    #[test]
    fn empty_name_and_body_are_valid() {
        let original = line("", false, b"");
        let mut decoder = LogFrameDecoder::default();
        decoder.push(&encode_log_frame(&original));
        let decoded = decoder.next_frame().unwrap();
        assert_eq!(decoded.name, "");
        assert!(decoded.bytes.is_empty());
    }
}
