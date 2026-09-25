#!/usr/bin/env -S cargo +nightly -Zscript
---
[package]
name = "add-page-index"
version = "0.1.0"
edition = "2021"

[dependencies]
parquet = { version = "59", default-features = false }
snap = "1.1"
zstd = "0.13"
indicatif = "0.17"
humansize = "2"

[profile.release]
opt-level = 3

# `cargo -Zscript` builds the dev profile, which is what the shebang runs, so
# optimize it too: this tool decompresses pages and counts levels.
[profile.dev]
opt-level = 3
---
//! Add a Parquet page index (ColumnIndex + OffsetIndex) to an existing file
//! *in place*, without reading or rewriting a single byte of the data.
//!
//! Only the trailing metadata region is replaced. The data pages keep the exact
//! byte offsets the old footer already recorded, so nothing below `data_end`
//! is ever written:
//!
//! ```text
//!  before:  [ PAR1 | row group data ............ ][ footer | len | PAR1 ]
//!  after:   [ PAR1 | row group data (untouched) ][ ColumnIndex | OffsetIndex | footer | len | PAR1 ]
//!                                                ^ data_end: the only byte offset that is written to
//! ```
//!
//! The index is reconstructed from information already in the file:
//!   * page offsets / compressed sizes, by walking the page headers;
//!   * per-page min/max/null_count, from the statistics in each data page header;
//!   * per-page row counts: `num_values` for non-repeated columns, and the number
//!     of zero repetition levels for repeated (list) columns.
//!
//! The result is meant to be semantically equivalent to
//! `pyarrow.parquet.write_table(..., write_page_index=True)`.
//!
//! Usage:
//!   ./add-page-index.rs write <file-or-dir> [--jobs N] [--force] [--dry-run]
//!   ./add-page-index.rs scan  <file.parquet>
//!   ./add-page-index.rs dump  <file.parquet> [max_pages_per_column]
//!
//! Given a directory, every *.parquet file below it is indexed in place, using
//! `--jobs` worker threads (8 by default).

use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use humansize::{format_size, BINARY};
use indicatif::{ProgressBar, ProgressStyle};

use parquet::basic::{BoundaryOrder, Compression, ConvertedType, LogicalType, Type as PhysicalType};
use parquet::file::metadata::{
    ColumnIndexBuilder, OffsetIndexBuilder, PageIndexPolicy, ParquetColumnIndex, ParquetMetaData,
    ParquetMetaDataReader, ParquetMetaDataWriter, ParquetOffsetIndex,
};
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::file::writer::TrackedWrite;
use parquet::schema::types::ColumnDescriptor;

type R<T> = Result<T, Box<dyn Error>>;

// ---------------------------------------------------------------------------
// Thrift compact protocol reader (just enough of it to parse a PageHeader)
// ---------------------------------------------------------------------------

const T_BOOL_TRUE: u8 = 1;
const T_BOOL_FALSE: u8 = 2;
const T_I8: u8 = 3;
const T_I16: u8 = 4;
const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_DOUBLE: u8 = 7;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_SET: u8 = 10;
const T_MAP: u8 = 11;
const T_STRUCT: u8 = 12;
const T_UUID: u8 = 13;

/// Returned when the buffer does not hold a complete page header.
#[derive(Debug)]
struct NeedMore;

impl std::fmt::Display for NeedMore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "truncated thrift input")
    }
}

impl Error for NeedMore {}

struct Thrift<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Thrift<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Thrift { buf, pos: 0 }
    }

    fn byte(&mut self) -> R<u8> {
        let b = *self.buf.get(self.pos).ok_or(NeedMore)?;
        self.pos += 1;
        Ok(b)
    }

    fn take(&mut self, n: usize) -> R<&'a [u8]> {
        let end = self.pos.checked_add(n).ok_or(NeedMore)?;
        let s = self.buf.get(self.pos..end).ok_or(NeedMore)?;
        self.pos = end;
        Ok(s)
    }

    fn uvarint(&mut self) -> R<u64> {
        let mut result: u64 = 0;
        let mut shift = 0;
        loop {
            let b = self.byte()?;
            result |= ((b & 0x7f) as u64) << shift;
            if b & 0x80 == 0 {
                return Ok(result);
            }
            shift += 7;
            if shift > 63 {
                return Err("varint overflow".into());
            }
        }
    }

    fn zigzag(&mut self) -> R<i64> {
        let v = self.uvarint()?;
        Ok(((v >> 1) as i64) ^ -((v & 1) as i64))
    }

    fn binary(&mut self) -> R<&'a [u8]> {
        let len = self.uvarint()? as usize;
        self.take(len)
    }

    /// Reads the next field header; `None` on the STOP byte.
    fn field(&mut self, last_id: &mut i16) -> R<Option<(i16, u8)>> {
        let b = self.byte()?;
        if b == 0 {
            return Ok(None);
        }
        let ttype = b & 0x0f;
        let delta = (b >> 4) as i16;
        let id = if delta == 0 {
            self.zigzag()? as i16
        } else {
            *last_id + delta
        };
        *last_id = id;
        Ok(Some((id, ttype)))
    }

    fn skip(&mut self, ttype: u8) -> R<()> {
        match ttype {
            T_BOOL_TRUE | T_BOOL_FALSE => {}
            T_I8 => {
                self.byte()?;
            }
            T_I16 | T_I32 | T_I64 => {
                self.zigzag()?;
            }
            T_DOUBLE => {
                self.take(8)?;
            }
            T_BINARY => {
                self.binary()?;
            }
            T_UUID => {
                self.take(16)?;
            }
            T_LIST | T_SET => {
                let b = self.byte()?;
                let elem = b & 0x0f;
                let mut size = (b >> 4) as usize;
                if size == 15 {
                    size = self.uvarint()? as usize;
                }
                for _ in 0..size {
                    self.skip(elem)?;
                }
            }
            T_MAP => {
                let size = self.uvarint()? as usize;
                if size > 0 {
                    let kv = self.byte()?;
                    let (k, v) = (kv >> 4, kv & 0x0f);
                    for _ in 0..size {
                        self.skip(k)?;
                        self.skip(v)?;
                    }
                }
            }
            T_STRUCT => {
                let mut last = 0i16;
                while let Some((_, ft)) = self.field(&mut last)? {
                    self.skip(ft)?;
                }
            }
            other => return Err(format!("unknown thrift type {other}").into()),
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// PageHeader
// ---------------------------------------------------------------------------

const PAGE_DATA_V1: i32 = 0;
const PAGE_DICTIONARY: i32 = 2;

const ENCODING_RLE: i32 = 3;

#[derive(Debug, Default, Clone)]
struct Stats {
    min: Option<Vec<u8>>,
    max: Option<Vec<u8>>,
    null_count: Option<i64>,
}

#[derive(Debug, Default)]
struct PageHeader {
    page_type: i32,
    uncompressed_page_size: i32,
    compressed_page_size: i32,
    num_values: i32,
    /// v2 only: the row count is stated outright.
    num_rows: Option<i32>,
    /// v1 only.
    rep_level_encoding: i32,
    stats: Option<Stats>,
}

fn parse_statistics(t: &mut Thrift) -> R<Stats> {
    let mut s = Stats::default();
    // The deprecated `min`/`max` fields are a fallback for `min_value`/`max_value`.
    let (mut legacy_min, mut legacy_max) = (None, None);
    let mut last = 0i16;
    while let Some((id, ft)) = t.field(&mut last)? {
        match (id, ft) {
            (1, T_BINARY) => legacy_max = Some(t.binary()?.to_vec()),
            (2, T_BINARY) => legacy_min = Some(t.binary()?.to_vec()),
            (3, T_I64) => s.null_count = Some(t.zigzag()?),
            (5, T_BINARY) => s.max = Some(t.binary()?.to_vec()),
            (6, T_BINARY) => s.min = Some(t.binary()?.to_vec()),
            _ => t.skip(ft)?,
        }
    }
    s.min = s.min.or(legacy_min);
    s.max = s.max.or(legacy_max);
    Ok(s)
}

/// Parses a page header, returning it along with its serialized length.
fn parse_page_header(buf: &[u8]) -> R<(usize, PageHeader)> {
    let mut t = Thrift::new(buf);
    let mut h = PageHeader::default();
    let mut last = 0i16;
    while let Some((id, ft)) = t.field(&mut last)? {
        match (id, ft) {
            (1, T_I32) => h.page_type = t.zigzag()? as i32,
            (2, T_I32) => h.uncompressed_page_size = t.zigzag()? as i32,
            (3, T_I32) => h.compressed_page_size = t.zigzag()? as i32,
            (5, T_STRUCT) => {
                // DataPageHeader (v1)
                let mut inner = 0i16;
                while let Some((iid, ift)) = t.field(&mut inner)? {
                    match (iid, ift) {
                        (1, T_I32) => h.num_values = t.zigzag()? as i32,
                        (4, T_I32) => h.rep_level_encoding = t.zigzag()? as i32,
                        (5, T_STRUCT) => h.stats = Some(parse_statistics(&mut t)?),
                        _ => t.skip(ift)?,
                    }
                }
            }
            (8, T_STRUCT) => {
                // DataPageHeaderV2
                let mut inner = 0i16;
                while let Some((iid, ift)) = t.field(&mut inner)? {
                    match (iid, ift) {
                        (1, T_I32) => h.num_values = t.zigzag()? as i32,
                        (3, T_I32) => h.num_rows = Some(t.zigzag()? as i32),
                        (8, T_STRUCT) => h.stats = Some(parse_statistics(&mut t)?),
                        _ => t.skip(ift)?,
                    }
                }
            }
            _ => t.skip(ft)?,
        }
    }
    Ok((t.pos, h))
}

// ---------------------------------------------------------------------------
// Repetition levels: a row starts at every level equal to zero
// ---------------------------------------------------------------------------

fn bit_width(max_level: i16) -> u8 {
    if max_level <= 0 {
        0
    } else {
        (16 - (max_level as u16).leading_zeros()) as u8
    }
}

/// Counts zeros among the first `num_levels` values of an RLE/bit-packed hybrid
/// run, the encoding used for repetition levels.
fn count_zero_levels(data: &[u8], width: u8, num_levels: usize) -> R<usize> {
    if width == 0 {
        return Ok(num_levels); // every level is implicitly zero
    }
    let mut pos = 0usize;
    let mut left = num_levels;
    let mut zeros = 0usize;
    while left > 0 {
        let mut t = Thrift::new(data.get(pos..).ok_or(NeedMore)?);
        let header = t.uvarint()? as usize;
        pos += t.pos;
        if header & 1 == 1 {
            // bit-packed run: (header >> 1) groups of 8 values
            let groups = header >> 1;
            let values = groups * 8;
            let nbytes = groups * width as usize;
            let chunk = data.get(pos..pos + nbytes).ok_or(NeedMore)?;
            let take = values.min(left);
            zeros += count_zeros_bitpacked(chunk, width, take);
            pos += nbytes;
            left -= take;
        } else {
            // RLE run: (header >> 1) repeats of a single value
            let run = header >> 1;
            if run == 0 {
                return Err("zero-length RLE run".into());
            }
            let nbytes = (width as usize).div_ceil(8);
            let chunk = data.get(pos..pos + nbytes).ok_or(NeedMore)?;
            let value = chunk
                .iter()
                .enumerate()
                .fold(0u64, |acc, (i, b)| acc | ((*b as u64) << (8 * i)));
            pos += nbytes;
            let take = run.min(left);
            if value == 0 {
                zeros += take;
            }
            left -= take;
        }
    }
    Ok(zeros)
}

fn count_zeros_bitpacked(data: &[u8], width: u8, count: usize) -> usize {
    // Fast path for the common list case (max repetition level 1).
    if width == 1 {
        let full = count / 8;
        let mut zeros: usize = data[..full].iter().map(|b| b.count_zeros() as usize).sum();
        let rem = count % 8;
        if rem > 0 {
            let b = data[full];
            zeros += (0..rem).filter(|k| (b >> k) & 1 == 0).count();
        }
        return zeros;
    }
    let mut zeros = 0usize;
    for i in 0..count {
        let mut value = 0u64;
        for k in 0..width as usize {
            let bit = i * width as usize + k;
            if (data[bit / 8] >> (bit % 8)) & 1 == 1 {
                value |= 1 << k;
            }
        }
        if value == 0 {
            zeros += 1;
        }
    }
    zeros
}

/// Extracts the repetition-level block from the front of a v1 data page.
///
/// A v1 page is laid out as `[u32 rep_len][rep levels][def levels][values]`, all
/// compressed together, and only the first two fields are of interest here. zstd
/// can be decoded as a stream, so for it just the level prefix is decompressed
/// and the rest of the page is never touched; snappy has no partial-decode API,
/// so the page is decompressed whole.
fn rep_level_bytes(codec: Compression, input: &[u8], uncompressed_size: usize) -> R<Vec<u8>> {
    fn split_prefix(data: &[u8]) -> R<Vec<u8>> {
        let len = u32::from_le_bytes(
            data.get(..4)
                .ok_or("data page too short for repetition levels")?
                .try_into()
                .unwrap(),
        ) as usize;
        Ok(data
            .get(4..4 + len)
            .ok_or("truncated repetition levels")?
            .to_vec())
    }

    match codec {
        Compression::UNCOMPRESSED => split_prefix(input),
        Compression::SNAPPY => {
            let mut out = vec![0u8; uncompressed_size];
            let n = snap::raw::Decoder::new().decompress(input, &mut out)?;
            out.truncate(n);
            split_prefix(&out)
        }
        // Parquet stores a bare zstd frame, which decodes as a stream: read the
        // 4-byte length, then exactly that many bytes, and stop.
        Compression::ZSTD(_) => {
            let mut decoder = zstd::stream::read::Decoder::new(input)?;
            let mut head = [0u8; 4];
            decoder.read_exact(&mut head)?;
            let mut levels = vec![0u8; u32::from_le_bytes(head) as usize];
            decoder.read_exact(&mut levels)?;
            Ok(levels)
        }
        other => Err(format!(
            "codec {other} is unsupported (decompression is only needed for repeated columns; \
             add the matching crate to the frontmatter to support it)"
        )
        .into()),
    }
}

// ---------------------------------------------------------------------------
// Typed comparison, used for the boundary order
// ---------------------------------------------------------------------------

fn is_unsigned(descr: &ColumnDescriptor) -> bool {
    match descr.logical_type_ref() {
        Some(LogicalType::Integer(int_type)) => !int_type.is_signed,
        _ => matches!(
            descr.converted_type(),
            ConvertedType::UINT_8
                | ConvertedType::UINT_16
                | ConvertedType::UINT_32
                | ConvertedType::UINT_64
        ),
    }
}

/// Compares two serialized statistics values, or `None` when they are not
/// comparable (NaN, or a type whose sort order is not modelled here).
fn compare_values(descr: &ColumnDescriptor, a: &[u8], b: &[u8]) -> Option<std::cmp::Ordering> {
    let unsigned = is_unsigned(descr);
    match descr.physical_type() {
        PhysicalType::INT32 => {
            let x = i32::from_le_bytes(a.try_into().ok()?);
            let y = i32::from_le_bytes(b.try_into().ok()?);
            Some(if unsigned {
                (x as u32).cmp(&(y as u32))
            } else {
                x.cmp(&y)
            })
        }
        PhysicalType::INT64 => {
            let x = i64::from_le_bytes(a.try_into().ok()?);
            let y = i64::from_le_bytes(b.try_into().ok()?);
            Some(if unsigned {
                (x as u64).cmp(&(y as u64))
            } else {
                x.cmp(&y)
            })
        }
        PhysicalType::FLOAT => {
            f32::from_le_bytes(a.try_into().ok()?).partial_cmp(&f32::from_le_bytes(b.try_into().ok()?))
        }
        PhysicalType::DOUBLE => {
            f64::from_le_bytes(a.try_into().ok()?).partial_cmp(&f64::from_le_bytes(b.try_into().ok()?))
        }
        PhysicalType::BOOLEAN => Some(a.first().cmp(&b.first())),
        PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY => {
            // Unsigned lexicographic order, which is right for strings and plain
            // binary; decimals and intervals sort differently, so give up there.
            let decimal = matches!(descr.logical_type_ref(), Some(LogicalType::Decimal { .. }))
                || descr.converted_type() == ConvertedType::DECIMAL
                || descr.converted_type() == ConvertedType::INTERVAL;
            (!decimal).then(|| a.cmp(b))
        }
        PhysicalType::INT96 => None,
    }
}

fn boundary_order(descr: &ColumnDescriptor, pages: &[DataPage]) -> BoundaryOrder {
    let present: Vec<(&[u8], &[u8])> = pages
        .iter()
        .filter(|p| !p.is_null_page())
        .filter_map(|p| {
            let s = p.stats.as_ref()?;
            Some((s.min.as_deref()?, s.max.as_deref()?))
        })
        .collect();
    match present.len() {
        // Nothing comparable to order.
        0 => return BoundaryOrder::UNORDERED,
        // A single page is trivially sorted, which is what arrow-cpp records.
        1 => return BoundaryOrder::ASCENDING,
        _ => {}
    }
    let (mut ascending, mut descending) = (true, true);
    for w in present.windows(2) {
        let (Some(min_cmp), Some(max_cmp)) = (
            compare_values(descr, w[0].0, w[1].0),
            compare_values(descr, w[0].1, w[1].1),
        ) else {
            return BoundaryOrder::UNORDERED;
        };
        ascending &= min_cmp.is_le() && max_cmp.is_le();
        descending &= min_cmp.is_ge() && max_cmp.is_ge();
        if !ascending && !descending {
            return BoundaryOrder::UNORDERED;
        }
    }
    if ascending {
        BoundaryOrder::ASCENDING
    } else if descending {
        BoundaryOrder::DESCENDING
    } else {
        BoundaryOrder::UNORDERED
    }
}

// ---------------------------------------------------------------------------
// Walking a column chunk
// ---------------------------------------------------------------------------

/// One data page, as far as the two index structures care.
struct DataPage {
    offset: i64,
    /// Header plus compressed payload: what `PageLocation::compressed_page_size` means.
    size: i32,
    /// Length of the thrift page header alone, reported by `scan`.
    header_len: usize,
    rows: i64,
    num_values: i64,
    stats: Option<Stats>,
}

impl DataPage {
    /// A page holding no non-null values; its min/max are written as empty.
    fn is_null_page(&self) -> bool {
        self.stats
            .as_ref()
            .and_then(|s| s.null_count)
            .is_some_and(|nulls| nulls == self.num_values)
    }
}

fn read_at(file: &File, offset: u64, len: usize) -> R<Vec<u8>> {
    let mut buf = vec![0u8; len];
    file.read_exact_at(&mut buf, offset)?;
    Ok(buf)
}

/// Walks the page headers of one column chunk, decoding repetition levels when
/// the column is repeated and the row count cannot be read from the header.
fn scan_column_chunk(
    file: &File,
    start: u64,
    len: u64,
    codec: Compression,
    max_rep_level: i16,
) -> R<Vec<DataPage>> {
    let end = start + len;
    let mut pos = start;
    let mut pages = Vec::new();

    while pos < end {
        // Page headers are small, but statistics can make them large; start with
        // a modest window and grow it if the header does not fit.
        let mut window = 64 * 1024;
        let (header_len, header) = loop {
            let n = window.min((end - pos) as usize);
            let buf = read_at(file, pos, n)?;
            match parse_page_header(&buf) {
                Ok(v) => break v,
                Err(e)
                    if e.is::<NeedMore>()
                        && n < (end - pos) as usize
                        && window < 16 * 1024 * 1024 =>
                {
                    window *= 8;
                }
                Err(e) => return Err(format!("bad page header at offset {pos}: {e}").into()),
            }
        };

        let total = header_len as u64 + header.compressed_page_size as u64;
        if header.page_type != PAGE_DICTIONARY {
            let rows = page_row_count(file, pos, header_len, &header, codec, max_rep_level)?;
            pages.push(DataPage {
                offset: pos as i64,
                size: total as i32,
                header_len,
                rows,
                num_values: header.num_values as i64,
                stats: header.stats,
            });
        }
        pos += total;
    }
    if pos != end {
        return Err(format!("page walk ended at {pos}, expected chunk end {end}").into());
    }
    Ok(pages)
}

fn page_row_count(
    file: &File,
    page_offset: u64,
    header_len: usize,
    header: &PageHeader,
    codec: Compression,
    max_rep_level: i16,
) -> R<i64> {
    // Without repetition levels, every value is its own row.
    if max_rep_level == 0 {
        return Ok(header.num_values as i64);
    }
    // A v2 header states the row count directly.
    if let Some(rows) = header.num_rows {
        return Ok(rows as i64);
    }
    if header.page_type != PAGE_DATA_V1 {
        return Err(format!("unexpected page type {}", header.page_type).into());
    }
    if header.rep_level_encoding != ENCODING_RLE {
        return Err(format!(
            "repetition levels use encoding {}, only RLE is supported",
            header.rep_level_encoding
        )
        .into());
    }

    // In a v1 page the repetition levels sit at the front of the compressed
    // payload, so some of the page has to be decompressed to reach them.
    let raw = read_at(
        file,
        page_offset + header_len as u64,
        header.compressed_page_size as usize,
    )?;
    let levels = rep_level_bytes(codec, &raw, header.uncompressed_page_size as usize)?;
    let zeros = count_zero_levels(&levels, bit_width(max_rep_level), header.num_values as usize)?;
    Ok(zeros as i64)
}

// ---------------------------------------------------------------------------
// Building the index
// ---------------------------------------------------------------------------

fn build_indexes(
    file: &File,
    metadata: &ParquetMetaData,
) -> R<(ParquetColumnIndex, ParquetOffsetIndex, usize)> {
    let schema = metadata.file_metadata().schema_descr_ptr();
    let mut column_indexes = Vec::with_capacity(metadata.num_row_groups());
    let mut offset_indexes = Vec::with_capacity(metadata.num_row_groups());
    let mut total_pages = 0usize;

    for (rg_idx, rg) in metadata.row_groups().iter().enumerate() {
        let mut rg_column = Vec::with_capacity(rg.num_columns());
        let mut rg_offset = Vec::with_capacity(rg.num_columns());

        for (col_idx, chunk) in rg.columns().iter().enumerate() {
            let descr = schema.column(col_idx);
            let (start, len) = chunk.byte_range();
            let pages = scan_column_chunk(
                file,
                start,
                len,
                chunk.compression(),
                descr.max_rep_level(),
            )?;
            total_pages += pages.len();

            // The row counts must add up to the row group's row count; this is
            // the check that the repetition-level decoding is right.
            let rows: i64 = pages.iter().map(|p| p.rows).sum();
            if rows != rg.num_rows() {
                return Err(format!(
                    "row group {rg_idx} column {col_idx} ({}): pages cover {rows} rows, \
                     row group declares {}",
                    descr.path(),
                    rg.num_rows()
                )
                .into());
            }

            let mut offset_builder = OffsetIndexBuilder::new();
            let mut column_builder = ColumnIndexBuilder::new(descr.physical_type());
            column_builder.set_boundary_order(boundary_order(&descr, &pages));

            for page in &pages {
                offset_builder.append_offset_and_size(page.offset, page.size);
                offset_builder.append_row_count(page.rows);

                match (&page.stats, page.is_null_page()) {
                    // A null page carries empty min/max by convention.
                    (Some(s), true) => {
                        column_builder.append(true, Vec::new(), Vec::new(), s.null_count.unwrap())
                    }
                    (Some(Stats { min: Some(min), max: Some(max), null_count: Some(nulls) }), false) => {
                        column_builder.append(false, min.clone(), max.clone(), *nulls)
                    }
                    // No usable page statistics: no column index for this chunk.
                    _ => column_builder.to_invalid(),
                }
                if !column_builder.valid() {
                    break;
                }
            }

            rg_offset.push(offset_builder.build());
            rg_column.push(if column_builder.valid() {
                column_builder.build()?
            } else {
                ColumnIndexMetaData::NONE
            });
        }
        column_indexes.push(rg_column);
        offset_indexes.push(rg_offset);
    }
    Ok((column_indexes, offset_indexes, total_pages))
}

/// Offset of the footer's thrift metadata, i.e. the first byte this tool replaces.
fn footer_start(file: &File, file_len: u64) -> R<u64> {
    let tail = read_at(file, file_len - 8, 8)?;
    if &tail[4..] != b"PAR1" {
        return Err("not a parquet file: missing PAR1 trailer".into());
    }
    let footer_len = u32::from_le_bytes(tail[..4].try_into().unwrap()) as u64;
    Ok(file_len - 8 - footer_len)
}

/// A sink that drops the first `skip` bytes written to it and collects the rest.
///
/// [`TrackedWrite`] counts from zero, but the offsets embedded in the footer have
/// to be absolute file offsets. Feeding it `data_end` throwaway bytes first sets
/// its counter to the position the metadata will actually occupy, while this sink
/// keeps only the bytes we intend to write.
struct SkipPrefix<'a> {
    skip: u64,
    out: &'a mut Vec<u8>,
}

impl Write for SkipPrefix<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let skipped = (buf.len() as u64).min(self.skip) as usize;
        self.skip -= skipped as u64;
        self.out.extend_from_slice(&buf[skipped..]);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// What happened to one file.
struct Outcome {
    pages: usize,
    /// Column chunks left without a ColumnIndex because their pages had no statistics.
    chunks_without_stats: usize,
    tail_len: u64,
    bytes_added: i64,
}

/// Builds and installs the page index for one file. Returns `None` if the file
/// already has an index and `force` was not requested.
fn index_file(path: &Path, force: bool, dry_run: bool) -> R<Option<Outcome>> {
    let file = File::open(path)?;
    let file_len = file.metadata()?.len();

    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Skip)
        .parse_and_finish(&file)?;
    let data_end = footer_start(&file, file_len)?;

    let indexed_already = metadata
        .row_groups()
        .iter()
        .flat_map(|rg| rg.columns())
        .any(|c| c.column_index_offset().is_some() || c.offset_index_offset().is_some());
    if indexed_already && !force {
        return Ok(None);
    }

    let (column_index, offset_index, pages) = build_indexes(&file, &metadata)?;
    let chunks_without_stats = column_index
        .iter()
        .flatten()
        .filter(|c| matches!(c, ColumnIndexMetaData::NONE))
        .count();

    let new_metadata = metadata
        .into_builder()
        .set_column_index(Some(column_index))
        .set_offset_index(Some(offset_index))
        .build();

    // Build the whole new tail (page index + footer + trailer) in memory, so the
    // file itself is touched by exactly one write.
    let mut tail = Vec::new();
    {
        let mut tracked = TrackedWrite::new(SkipPrefix {
            skip: data_end,
            out: &mut tail,
        });
        let filler = vec![0u8; 1024 * 1024];
        while (tracked.bytes_written() as u64) < data_end {
            let n = filler.len().min((data_end - tracked.bytes_written() as u64) as usize);
            tracked.write_all(&filler[..n])?;
        }
        ParquetMetaDataWriter::new_with_tracked(tracked, &new_metadata).finish()?;
    }
    if tail.is_empty() {
        return Err("metadata writer produced no output".into());
    }

    let new_len = data_end + tail.len() as u64;
    let outcome = Outcome {
        pages,
        chunks_without_stats,
        tail_len: tail.len() as u64,
        bytes_added: new_len as i64 - file_len as i64,
    };
    if dry_run {
        return Ok(Some(outcome));
    }

    // The only mutation of the file: overwrite the old footer with the new tail.
    let out = OpenOptions::new().write(true).open(path)?;
    out.write_all_at(&tail, data_end)?;
    out.set_len(new_len)?;
    out.sync_all()?;

    verify(path, data_end)?;
    Ok(Some(outcome))
}

/// Single file: report in detail.
fn write_one(path: &Path, force: bool, dry_run: bool) -> R<()> {
    let started = Instant::now();
    let file = File::open(path)?;
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Skip)
        .parse_and_finish(&file)?;
    println!(
        "{}: {} row groups, {} columns, {} rows",
        path.display(),
        metadata.num_row_groups(),
        metadata.file_metadata().schema_descr().num_columns(),
        metadata.file_metadata().num_rows(),
    );
    drop(file);

    match index_file(path, force, dry_run)? {
        None => println!("already has a page index; pass --force to rebuild it"),
        Some(o) => {
            if o.chunks_without_stats > 0 {
                println!(
                    "note: {} column chunks had no page statistics and got no ColumnIndex",
                    o.chunks_without_stats
                );
            }
            println!(
                "{} {} data pages, tail {} ({:+} bytes), {:.1}s{}",
                if dry_run { "would index" } else { "indexed" },
                o.pages,
                human_bytes(o.tail_len),
                o.bytes_added,
                started.elapsed().as_secs_f64(),
                if dry_run { " (dry run, nothing written)" } else { "" },
            );
            if !dry_run {
                println!("verified: page index is consistent with the data pages it describes");
            }
        }
    }
    Ok(())
}

/// Collects `*.parquet` files below `root`, skipping dot- and underscore-prefixed
/// names (`_metadata`, `_common_metadata` and friends are not data files).
fn collect_parquet_files(root: &Path, out: &mut Vec<PathBuf>) -> R<()> {
    let dir = std::fs::read_dir(root).map_err(|e| format!("{}: {e}", root.display()))?;
    for entry in dir {
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with('.') || name.starts_with('_') {
            continue;
        }
        if entry.file_type()?.is_dir() {
            collect_parquet_files(&path, out)?;
        } else if path
            .extension()
            .is_some_and(|e| e.eq_ignore_ascii_case("parquet"))
        {
            out.push(path);
        }
    }
    Ok(())
}

/// Directory tree: `jobs` workers, one progress bar, per-file failures collected
/// rather than aborting the run.
fn write_tree(root: &Path, jobs: usize, force: bool, dry_run: bool) -> R<()> {
    let started = Instant::now();
    let mut files = Vec::new();
    collect_parquet_files(root, &mut files)?;
    files.sort();
    if files.is_empty() {
        println!("no *.parquet files found under {}", root.display());
        return Ok(());
    }
    println!(
        "{} parquet files under {}, {jobs} threads{}",
        files.len(),
        root.display(),
        if dry_run { ", dry run" } else { "" }
    );

    let bar = ProgressBar::new(files.len() as u64);
    bar.set_style(ProgressStyle::with_template(
        "[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} files {msg}",
    )?);

    let next = AtomicUsize::new(0);
    let (written, skipped, pages, added) = (
        AtomicUsize::new(0),
        AtomicUsize::new(0),
        AtomicUsize::new(0),
        AtomicUsize::new(0),
    );
    let failures: Mutex<Vec<(PathBuf, String)>> = Mutex::new(Vec::new());

    std::thread::scope(|scope| {
        for _ in 0..jobs {
            scope.spawn(|| loop {
                let idx = next.fetch_add(1, Ordering::Relaxed);
                let Some(path) = files.get(idx) else { return };
                match index_file(path, force, dry_run) {
                    Ok(Some(o)) => {
                        written.fetch_add(1, Ordering::Relaxed);
                        pages.fetch_add(o.pages, Ordering::Relaxed);
                        added.fetch_add(o.bytes_added.max(0) as usize, Ordering::Relaxed);
                    }
                    Ok(None) => {
                        skipped.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        bar.println(format!("FAILED {}: {e}", path.display()));
                        failures.lock().unwrap().push((path.clone(), e.to_string()));
                    }
                }
                bar.inc(1);
                bar.set_message(format!(
                    "{} indexed, {} skipped, {} failed",
                    written.load(Ordering::Relaxed),
                    skipped.load(Ordering::Relaxed),
                    failures.lock().unwrap().len(),
                ));
            });
        }
    });
    bar.finish_and_clear();

    let failures = failures.into_inner().unwrap();
    println!(
        "{} {} files ({} data pages, {} of index added), {} skipped, {} failed, {:.1}s",
        if dry_run { "would index" } else { "indexed" },
        written.load(Ordering::Relaxed),
        pages.load(Ordering::Relaxed),
        human_bytes(added.load(Ordering::Relaxed) as u64),
        skipped.load(Ordering::Relaxed),
        failures.len(),
        started.elapsed().as_secs_f64(),
    );
    if !failures.is_empty() {
        for (path, err) in &failures {
            println!("  failed: {} -- {err}", path.display());
        }
        return Err(format!("{} file(s) failed", failures.len()).into());
    }
    Ok(())
}

/// Re-reads the output and checks the index against the data it describes.
fn verify(path: &Path, data_end: u64) -> R<()> {
    let file = File::open(path)?;
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .parse_and_finish(&file)?;
    let column_index = metadata.column_index().ok_or("output has no column index")?;
    let offset_index = metadata.offset_index().ok_or("output has no offset index")?;

    for (rg_idx, rg) in metadata.row_groups().iter().enumerate() {
        for (col_idx, chunk) in rg.columns().iter().enumerate() {
            let offsets = &offset_index[rg_idx][col_idx];
            let locations = offsets.page_locations();
            let (start, len) = chunk.byte_range();

            let mut rows = 0i64;
            let mut last_end = start;
            for (page_idx, loc) in locations.iter().enumerate() {
                if loc.first_row_index != rows {
                    return Err(format!(
                        "rg {rg_idx} col {col_idx} page {page_idx}: first_row_index \
                         {} != running row count {rows}",
                        loc.first_row_index
                    )
                    .into());
                }
                let page_end = loc.offset as u64 + loc.compressed_page_size as u64;
                if (loc.offset as u64) < start || page_end > start + len {
                    return Err(format!(
                        "rg {rg_idx} col {col_idx} page {page_idx}: [{}, {page_end}) outside \
                         chunk [{start}, {})",
                        loc.offset,
                        start + len
                    )
                    .into());
                }
                if page_idx > 0 && loc.offset as u64 != last_end {
                    return Err(format!(
                        "rg {rg_idx} col {col_idx} page {page_idx}: starts at {} but previous \
                         page ends at {last_end}",
                        loc.offset
                    )
                    .into());
                }
                last_end = page_end;
                // The next page's first_row_index tells us this page's row count.
                rows = locations
                    .get(page_idx + 1)
                    .map(|next| next.first_row_index)
                    .unwrap_or(rg.num_rows());
            }
            if rows != rg.num_rows() {
                return Err(format!(
                    "rg {rg_idx} col {col_idx}: index covers {rows} rows, row group has {}",
                    rg.num_rows()
                )
                .into());
            }

            let ci = &column_index[rg_idx][col_idx];
            if !matches!(ci, ColumnIndexMetaData::NONE)
                && ci.num_pages() as usize != locations.len()
            {
                return Err(format!(
                    "rg {rg_idx} col {col_idx}: column index has {} pages, offset index has {}",
                    ci.num_pages(),
                    locations.len()
                )
                .into());
            }

            // Every index structure must live past the data region.
            for (kind, off) in [
                ("column index", chunk.column_index_offset()),
                ("offset index", chunk.offset_index_offset()),
            ] {
                if let Some(off) = off {
                    if (off as u64) < data_end {
                        return Err(format!(
                            "rg {rg_idx} col {col_idx}: {kind} at {off} overlaps the data region \
                             (ends at {data_end})"
                        )
                        .into());
                    }
                }
            }
        }
    }
    Ok(())
}

/// Human-readable byte count in binary multiples: 1 KiB = 1024 B.
fn human_bytes(bytes: u64) -> String {
    format_size(bytes, BINARY)
}

// ---------------------------------------------------------------------------
// scan: report what the data page headers contain
// ---------------------------------------------------------------------------

/// Prints per-chunk page-header facts. The interesting one is whether the data
/// page headers carry statistics: without them no ColumnIndex can be rebuilt.
fn scan(path: &str) -> R<()> {
    let file = File::open(path)?;
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Skip)
        .parse_and_finish(&file)?;
    let schema = metadata.file_metadata().schema_descr_ptr();

    let (mut all_pages, mut all_with_stats) = (0usize, 0usize);
    let (mut chunks, mut with_bloom) = (0usize, 0usize);
    for (rg_idx, rg) in metadata.row_groups().iter().enumerate() {
        for (col_idx, chunk) in rg.columns().iter().enumerate() {
            chunks += 1;
            with_bloom += chunk.bloom_filter_offset().is_some() as usize;
            let descr = schema.column(col_idx);
            let (start, len) = chunk.byte_range();
            let pages =
                scan_column_chunk(&file, start, len, chunk.compression(), descr.max_rep_level())?;
            let with_stats = pages.iter().filter(|p| p.stats.is_some()).count();
            let header_bytes: usize = pages.iter().map(|p| p.header_len).sum();
            all_pages += pages.len();
            all_with_stats += with_stats;
            println!(
                "rg {rg_idx} col {col_idx} {:<34} pages={:<4} with_stats={:<4} \
                 header_bytes={:<7} mean_header={:.0}",
                descr.path().string(),
                pages.len(),
                with_stats,
                header_bytes,
                header_bytes as f64 / pages.len().max(1) as f64,
            );
        }
    }
    println!(
        "total: {all_pages} data pages, {all_with_stats} with page statistics{}",
        if all_with_stats < all_pages {
            " -- chunks whose pages lack statistics get no ColumnIndex"
        } else {
            ""
        }
    );
    println!("bloom filters: {with_bloom} of {chunks} column chunks");
    Ok(())
}

// ---------------------------------------------------------------------------
// dump: print the page index so two files can be compared
// ---------------------------------------------------------------------------

fn dump(path: &str, max_pages: usize) -> R<()> {
    let file = File::open(path)?;
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .parse_and_finish(&file)?;
    let schema = metadata.file_metadata().schema_descr_ptr();
    let column_index = metadata.column_index().ok_or("file has no column index")?;
    let offset_index = metadata.offset_index().ok_or("file has no offset index")?;

    for rg_idx in 0..metadata.num_row_groups() {
        for col_idx in 0..schema.num_columns() {
            let descr = schema.column(col_idx);
            let ci = &column_index[rg_idx][col_idx];
            let locations = offset_index[rg_idx][col_idx].page_locations();
            println!(
                "rg {rg_idx} col {col_idx} {} pages={} boundary_order={:?} unencoded_byte_array_data_bytes={:?}",
                descr.path(),
                locations.len(),
                ci.get_boundary_order(),
                offset_index[rg_idx][col_idx]
                    .unencoded_byte_array_data_bytes()
                    .map(|v| v.len()),
            );
            for (page_idx, loc) in locations.iter().enumerate().take(max_pages) {
                println!(
                    "  page {page_idx} first_row={} size={} null_count={:?} null_page={} \
                     min={} max={}",
                    loc.first_row_index,
                    loc.compressed_page_size,
                    ci.null_count(page_idx),
                    ci.is_null_page(page_idx),
                    min_max(ci, page_idx).0,
                    min_max(ci, page_idx).1,
                );
            }
            if locations.len() > max_pages {
                println!("  ... {} more pages", locations.len() - max_pages);
            }
        }
    }
    Ok(())
}

/// Formats one page's min/max, whatever the column type is.
fn min_max(index: &ColumnIndexMetaData, page: usize) -> (String, String) {
    macro_rules! fmt {
        ($idx:expr) => {
            (
                format!("{:?}", $idx.min_value(page)),
                format!("{:?}", $idx.max_value(page)),
            )
        };
    }
    match index {
        ColumnIndexMetaData::NONE => ("-".into(), "-".into()),
        ColumnIndexMetaData::BOOLEAN(i) => fmt!(i),
        ColumnIndexMetaData::INT32(i) => fmt!(i),
        ColumnIndexMetaData::INT64(i) => fmt!(i),
        ColumnIndexMetaData::INT96(i) => fmt!(i),
        ColumnIndexMetaData::FLOAT(i) => fmt!(i),
        ColumnIndexMetaData::DOUBLE(i) => fmt!(i),
        ColumnIndexMetaData::BYTE_ARRAY(i) => fmt!(i),
        ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(i) => fmt!(i),
    }
}

// ---------------------------------------------------------------------------

/// `write` flags: --jobs N / --jobs=N, --force, --dry-run.
fn parse_write_args(args: &[String]) -> R<(PathBuf, usize, bool, bool)> {
    let (mut target, mut jobs, mut force, mut dry_run) = (None, 8usize, false, false);
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--force" => force = true,
            "--dry-run" => dry_run = true,
            "--jobs" | "-j" => jobs = it.next().ok_or("--jobs needs a value")?.parse()?,
            other if other.starts_with("--jobs=") => jobs = other[7..].parse()?,
            other if other.starts_with('-') => return Err(format!("unknown flag {other}").into()),
            other if target.is_none() => target = Some(PathBuf::from(other)),
            other => return Err(format!("unexpected argument {other}").into()),
        }
    }
    let target = target.ok_or("write needs a file or directory")?;
    if jobs == 0 {
        return Err("--jobs must be at least 1".into());
    }
    Ok((target, jobs, force, dry_run))
}

fn main() -> R<()> {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("write") if args.len() >= 3 => {
            let (target, jobs, force, dry_run) = parse_write_args(&args[2..])?;
            if !target.exists() {
                return Err(format!("no such file or directory: {}", target.display()).into());
            }
            if target.is_dir() {
                write_tree(&target, jobs, force, dry_run)
            } else {
                write_one(&target, force, dry_run)
            }
        }
        Some("scan") if args.len() == 3 => scan(&args[2]),
        Some("dump") if args.len() == 3 || args.len() == 4 => {
            let max_pages = args.get(3).map_or(Ok(8), |s| s.parse())?;
            dump(&args[2], max_pages)
        }
        _ => {
            eprintln!(
                "usage:\n  {0} write <file-or-dir> [--jobs N] [--force] [--dry-run]\n      \
                 modifies files in place; a directory is walked recursively\n  \
                 {0} scan <file.parquet>\n  \
                 {0} dump <file.parquet> [max_pages_per_column]",
                args[0]
            );
            std::process::exit(2);
        }
    }
}
