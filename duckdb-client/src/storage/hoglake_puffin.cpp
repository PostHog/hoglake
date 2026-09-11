#include "storage/hoglake_puffin.hpp"

#include "duckdb/common/bswap.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/main/client_context.hpp"

#include "yyjson.hpp"

#include "roaring/roaring.hh"

#include <array>
#include <cstring>

// Adapted from DuckLake's puffin/deletion-vector implementation (MIT),
// reduced to the hoglake contract: a puffin v1 container carrying
// exactly one uncompressed deletion-vector-v1 blob (see the server's
// PuffinDeletionVector.kt — the wire's only delete encoding).

namespace duckdb {

using namespace duckdb_yyjson; // NOLINT

// Puffin container: "PFA1" magic, blobs, then the footer
// Footer: Magic | FooterPayload | FooterPayloadSize (4, LE) | Flags (4) | Magic
static constexpr const data_t PUFFIN_MAGIC[4] = {'P', 'F', 'A', '1'};
static constexpr idx_t PUFFIN_MAGIC_SIZE = 4;
static constexpr idx_t PUFFIN_FOOTER_SIZE_FIELD_SIZE = 4;
static constexpr idx_t PUFFIN_FOOTER_FLAGS_SIZE = 4;
static constexpr idx_t PUFFIN_FOOTER_TAIL_SIZE =
    PUFFIN_FOOTER_SIZE_FIELD_SIZE + PUFFIN_FOOTER_FLAGS_SIZE + PUFFIN_MAGIC_SIZE;
static constexpr idx_t PUFFIN_FOOTER_STRUCT_SIZE = PUFFIN_MAGIC_SIZE + PUFFIN_FOOTER_TAIL_SIZE;
static constexpr idx_t PUFFIN_MIN_FILE_SIZE = PUFFIN_MAGIC_SIZE + PUFFIN_FOOTER_STRUCT_SIZE;
static constexpr uint32_t PUFFIN_FOOTER_COMPRESSED_FLAG = 1;
static constexpr const char *DELETION_VECTOR_BLOB_TYPE = "deletion-vector-v1";
static constexpr const data_t DELETION_VECTOR_MAGIC[4] = {0xD1, 0xD3, 0x39, 0x64};

//===--------------------------------------------------------------------===//
// CRC-32 (over magic + vector, stored big-endian)
//===--------------------------------------------------------------------===//

namespace {

class CRC32 {
public:
	CRC32() : crc(0xFFFFFFFF) {
	}
	void Update(const_data_ptr_t data, idx_t length) {
		auto table = GetTable();
		for (idx_t i = 0; i < length; i++) {
			crc = table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
		}
	}
	uint32_t GetValue() const {
		return crc ^ 0xFFFFFFFF;
	}

private:
	static const uint32_t *GetTable() {
		static const auto table = []() {
			std::array<uint32_t, 256> t {};
			for (uint32_t i = 0; i < 256; i++) {
				uint32_t c = i;
				for (int j = 0; j < 8; j++) {
					c = (c & 1) ? (0xEDB88320 ^ (c >> 1)) : (c >> 1);
				}
				t[i] = c;
			}
			return t;
		}();
		return table.data();
	}
	uint32_t crc;
};

struct YyjsonDocHolder {
	explicit YyjsonDocHolder(yyjson_doc *doc) : doc(doc) {
	}
	~YyjsonDocHolder() {
		if (doc) {
			yyjson_doc_free(doc);
		}
	}
	yyjson_doc *doc;
};

struct YyjsonMutDocHolder {
	explicit YyjsonMutDocHolder(yyjson_mut_doc *doc) : doc(doc) {
	}
	~YyjsonMutDocHolder() {
		if (doc) {
			yyjson_mut_doc_free(doc);
		}
	}
	yyjson_mut_doc *doc;
};

struct RoaringIterateContext {
	set<idx_t> *out;
	idx_t high;
};

} // namespace

//===--------------------------------------------------------------------===//
// deletion-vector-v1 blob codec
//===--------------------------------------------------------------------===//

void HoglakePuffin::DecodeBlob(const_data_ptr_t blob_start, idx_t blob_length, const string &path, set<idx_t> &out) {
	// blob = 4-byte BE length of (magic + vector) | magic D1 D3 39 64 |
	//        64-bit portable roaring | 4-byte BE CRC of (magic + vector)
	if (blob_length < 12) {
		throw InvalidInputException("Deletion vector in \"%s\" is too small (%llu bytes)", path, blob_length);
	}
	auto declared = BSwap(Load<uint32_t>(blob_start));
	if (idx_t(declared) != blob_length - 8) {
		throw InvalidInputException("Deletion vector in \"%s\" is corrupt - length prefix %u does not match blob "
		                            "length %llu",
		                            path, declared, blob_length);
	}
	auto checksummed_start = blob_start + sizeof(uint32_t);
	if (memcmp(checksummed_start, DELETION_VECTOR_MAGIC, 4) != 0) {
		throw InvalidInputException("Deletion vector in \"%s\" is corrupt - magic mismatch", path);
	}
	idx_t vector_size = declared - 4;
	auto vector_start = checksummed_start + 4;

	// CRC covers magic + vector
	CRC32 crc;
	crc.Update(checksummed_start, 4 + vector_size);
	auto stored_crc = BSwap(Load<uint32_t>(vector_start + vector_size));
	if (crc.GetValue() != stored_crc) {
		throw InvalidInputException("Deletion vector in \"%s\" is corrupt - CRC mismatch", path);
	}

	// 64-bit portable roaring: 8-byte LE bucket count, then per bucket a
	// 4-byte LE high key + portable 32-bit roaring bitmap
	auto ptr = vector_start;
	idx_t remaining = vector_size;
	if (remaining < sizeof(uint64_t)) {
		throw InvalidInputException("Deletion vector in \"%s\" is corrupt - truncated bitmap count", path);
	}
	auto bucket_count = Load<uint64_t>(ptr);
	ptr += sizeof(uint64_t);
	remaining -= sizeof(uint64_t);
	for (uint64_t i = 0; i < bucket_count; i++) {
		if (remaining < sizeof(uint32_t)) {
			throw InvalidInputException("Deletion vector in \"%s\" is corrupt - truncated bucket key", path);
		}
		auto key = Load<int32_t>(ptr);
		ptr += sizeof(int32_t);
		remaining -= sizeof(int32_t);
		if (key < 0) {
			// a negative high-32 key would sign-extend into a garbage
			// position; no valid file position has bit 63 set
			throw InvalidInputException("Deletion vector in \"%s\" is corrupt - negative bucket key %d", path, key);
		}
		auto bitmap_size = roaring::api::roaring_bitmap_portable_deserialize_size(const_char_ptr_cast(ptr), remaining);
		if (bitmap_size == 0 || bitmap_size > remaining) {
			throw InvalidInputException("Deletion vector in \"%s\" is corrupt - bucket %llu bitmap out of range", path,
			                            i);
		}
		auto bitmap = roaring::Roaring::readSafe(const_char_ptr_cast(ptr), bitmap_size);
		ptr += bitmap_size;
		remaining -= bitmap_size;

		RoaringIterateContext ctx {&out, static_cast<idx_t>(key)};
		bitmap.iterate(
		    [](uint32_t value, void *ptr_p) -> bool {
			    auto *c = static_cast<RoaringIterateContext *>(ptr_p);
			    c->out->insert((c->high << 32) | static_cast<idx_t>(value));
			    return true;
		    },
		    &ctx);
	}
	if (remaining != 0) {
		throw InvalidInputException("Deletion vector in \"%s\" is corrupt - %llu trailing bytes after the bitmap",
		                            path, remaining);
	}
}

vector<data_t> HoglakePuffin::EncodeBlob(const set<idx_t> &positions) {
	// group by high 32 bits; buckets in ascending key order (map)
	map<int32_t, roaring::Roaring> bitmaps;
	for (auto pos : positions) {
		bitmaps[static_cast<int32_t>(pos >> 32)].add(static_cast<uint32_t>(pos & 0xFFFFFFFF));
	}
	idx_t vector_size = sizeof(uint64_t);
	for (auto &entry : bitmaps) {
		entry.second.runOptimize();
		vector_size += sizeof(int32_t) + entry.second.getSizeInBytes(true);
	}
	idx_t total_size = sizeof(uint32_t) + 4 + vector_size + sizeof(uint32_t);

	vector<data_t> blob(total_size);
	auto ptr = blob.data();
	Store<uint32_t>(BSwap(NumericCast<uint32_t>(4 + vector_size)), ptr);
	ptr += sizeof(uint32_t);
	auto checksummed_start = ptr;
	memcpy(ptr, DELETION_VECTOR_MAGIC, 4);
	ptr += 4;
	Store<uint64_t>(bitmaps.size(), ptr);
	ptr += sizeof(uint64_t);
	for (auto &entry : bitmaps) {
		Store<int32_t>(entry.first, ptr);
		ptr += sizeof(int32_t);
		ptr += entry.second.write(char_ptr_cast(ptr), true);
	}
	CRC32 crc;
	crc.Update(checksummed_start, 4 + vector_size);
	Store<uint32_t>(BSwap(crc.GetValue()), ptr);
	return blob;
}

//===--------------------------------------------------------------------===//
// Puffin container writer (one DV blob; server-compatible)
//===--------------------------------------------------------------------===//

static void AppendMagic(vector<data_t> &file_data) {
	file_data.insert(file_data.end(), PUFFIN_MAGIC, PUFFIN_MAGIC + PUFFIN_MAGIC_SIZE);
}

vector<data_t> HoglakePuffin::WritePuffinFile(const set<idx_t> &positions, const string &data_file_path) {
	vector<data_t> file_data;
	AppendMagic(file_data);
	auto blob = EncodeBlob(positions);
	idx_t blob_offset = file_data.size();
	file_data.insert(file_data.end(), blob.begin(), blob.end());

	YyjsonMutDocHolder doc_holder(yyjson_mut_doc_new(nullptr));
	auto doc = doc_holder.doc;
	auto root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);
	auto blob_arr = yyjson_mut_obj_add_arr(doc, root, "blobs");
	auto blob_obj = yyjson_mut_arr_add_obj(doc, blob_arr);
	yyjson_mut_obj_add_str(doc, blob_obj, "type", DELETION_VECTOR_BLOB_TYPE);
	yyjson_mut_obj_add_arr(doc, blob_obj, "fields");
	yyjson_mut_obj_add_int(doc, blob_obj, "snapshot-id", -1);
	yyjson_mut_obj_add_int(doc, blob_obj, "sequence-number", -1);
	yyjson_mut_obj_add_uint(doc, blob_obj, "offset", blob_offset);
	yyjson_mut_obj_add_uint(doc, blob_obj, "length", blob.size());
	auto properties = yyjson_mut_obj_add_obj(doc, blob_obj, "properties");
	yyjson_mut_obj_add_strcpy(doc, properties, "referenced-data-file", data_file_path.c_str());
	yyjson_mut_obj_add_strcpy(doc, properties, "cardinality", to_string(positions.size()).c_str());
	auto file_properties = yyjson_mut_obj_add_obj(doc, root, "properties");
	yyjson_mut_obj_add_str(doc, file_properties, "created-by", "hoglake-duckdb");

	size_t len = 0;
	auto json = yyjson_mut_write(doc, 0, &len);
	if (!json) {
		throw InternalException("Failed to write puffin footer payload");
	}
	string payload(json, len);
	free(json); // NOLINT: yyjson allocates with malloc

	AppendMagic(file_data);
	file_data.insert(file_data.end(), payload.begin(), payload.end());
	data_t footer_tail[PUFFIN_FOOTER_SIZE_FIELD_SIZE + PUFFIN_FOOTER_FLAGS_SIZE] = {};
	Store<uint32_t>(NumericCast<uint32_t>(payload.size()), footer_tail);
	file_data.insert(file_data.end(), footer_tail, footer_tail + sizeof(footer_tail));
	AppendMagic(file_data);
	return file_data;
}

//===--------------------------------------------------------------------===//
// Puffin container reader
//===--------------------------------------------------------------------===//

vector<idx_t> HoglakePuffin::ReadDeletionVector(ClientContext &context, const string &path) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto file_handle = fs.OpenFile(path, FileOpenFlags::FILE_FLAGS_READ);
	auto file_size = NumericCast<idx_t>(file_handle->GetFileSize());
	auto buffer = make_unsafe_uniq_array<data_t>(file_size);
	file_handle->Read(buffer.get(), file_size);
	auto data = buffer.get();

	if (file_size < PUFFIN_MIN_FILE_SIZE || memcmp(data, PUFFIN_MAGIC, PUFFIN_MAGIC_SIZE) != 0) {
		throw InvalidInputException("Deletion vector file \"%s\" is not a puffin container - magic mismatch", path);
	}
	if (memcmp(data + file_size - PUFFIN_MAGIC_SIZE, PUFFIN_MAGIC, PUFFIN_MAGIC_SIZE) != 0) {
		throw InvalidInputException("Puffin file \"%s\" is corrupt - trailing magic mismatch", path);
	}
	auto flags = Load<uint32_t>(data + file_size - PUFFIN_MAGIC_SIZE - PUFFIN_FOOTER_FLAGS_SIZE);
	if (flags & PUFFIN_FOOTER_COMPRESSED_FLAG) {
		throw InvalidInputException("Puffin file \"%s\" has a compressed footer payload, which is not supported",
		                            path);
	}
	if (flags != 0) {
		throw InvalidInputException("Puffin file \"%s\" has unknown footer flags 0x%x", path, flags);
	}
	idx_t payload_size = Load<uint32_t>(data + file_size - PUFFIN_FOOTER_TAIL_SIZE);
	if (payload_size > file_size - PUFFIN_MIN_FILE_SIZE) {
		throw InvalidInputException("Puffin file \"%s\" is corrupt - footer payload size out of range", path);
	}
	auto payload_start = file_size - PUFFIN_FOOTER_TAIL_SIZE - payload_size;
	if (memcmp(data + payload_start - PUFFIN_MAGIC_SIZE, PUFFIN_MAGIC, PUFFIN_MAGIC_SIZE) != 0) {
		throw InvalidInputException("Puffin file \"%s\" is corrupt - footer magic mismatch", path);
	}
	auto blob_section_end = payload_start - PUFFIN_MAGIC_SIZE;

	YyjsonDocHolder doc_holder(yyjson_read(const_char_ptr_cast(data + payload_start), payload_size, 0));
	if (!doc_holder.doc) {
		throw InvalidInputException("Puffin file \"%s\" is corrupt - failed to parse footer payload", path);
	}
	auto root = yyjson_doc_get_root(doc_holder.doc);
	auto blobs_val = yyjson_obj_get(root, "blobs");
	if (!blobs_val || !yyjson_is_arr(blobs_val)) {
		throw InvalidInputException("Puffin file \"%s\" is corrupt - footer has no \"blobs\" list", path);
	}

	// hoglake ships exactly one deletion-vector-v1 blob per DV file
	idx_t dv_blobs = 0;
	idx_t blob_offset = 0;
	idx_t blob_length = 0;
	size_t arr_idx, arr_max;
	yyjson_val *blob_val;
	yyjson_arr_foreach(blobs_val, arr_idx, arr_max, blob_val) {
		auto type_val = yyjson_obj_get(blob_val, "type");
		if (!type_val || !yyjson_is_str(type_val)) {
			throw InvalidInputException("Puffin file \"%s\" is corrupt - blob without a type", path);
		}
		if (string(yyjson_get_str(type_val), yyjson_get_len(type_val)) != DELETION_VECTOR_BLOB_TYPE) {
			continue;
		}
		auto codec_val = yyjson_obj_get(blob_val, "compression-codec");
		if (codec_val && !yyjson_is_null(codec_val)) {
			throw InvalidInputException("Puffin file \"%s\" has a compressed deletion vector, which is not supported",
			                            path);
		}
		auto offset_val = yyjson_obj_get(blob_val, "offset");
		auto length_val = yyjson_obj_get(blob_val, "length");
		if (!offset_val || !yyjson_is_int(offset_val) || !length_val || !yyjson_is_int(length_val)) {
			throw InvalidInputException("Puffin file \"%s\" is corrupt - blob without offset/length", path);
		}
		auto raw_offset = yyjson_get_sint(offset_val);
		auto raw_length = yyjson_get_sint(length_val);
		if (raw_offset < NumericCast<int64_t>(PUFFIN_MAGIC_SIZE) || raw_length < 12) {
			throw InvalidInputException("Puffin file \"%s\" is corrupt - blob range out of bounds", path);
		}
		// range-check WITHOUT adding the two untrusted values (a signed
		// int64 add of hostile offset+length is UB before the check
		// could reject it)
		auto checked_offset = NumericCast<idx_t>(raw_offset);
		auto checked_length = NumericCast<idx_t>(raw_length);
		if (checked_length > blob_section_end || checked_offset > blob_section_end - checked_length) {
			throw InvalidInputException("Puffin file \"%s\" is corrupt - blob range out of bounds", path);
		}
		blob_offset = checked_offset;
		blob_length = checked_length;
		dv_blobs++;
	}
	if (dv_blobs != 1) {
		throw InvalidInputException("Puffin file \"%s\" must carry exactly one %s blob, found %llu", path,
		                            DELETION_VECTOR_BLOB_TYPE, dv_blobs);
	}

	set<idx_t> positions;
	DecodeBlob(data + blob_offset, blob_length, path, positions);
	vector<idx_t> result;
	result.reserve(positions.size());
	for (auto pos : positions) {
		result.push_back(pos);
	}
	return result;
}

} // namespace duckdb
