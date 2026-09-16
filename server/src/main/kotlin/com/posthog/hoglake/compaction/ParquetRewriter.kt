package com.posthog.hoglake.compaction

import com.posthog.hoglake.hydrator.FooterStats
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import com.posthog.hoglake.model.maxUnsignedParquetWidth
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types
import java.math.BigInteger
import java.nio.file.Path

/**
 * A compaction group whose inputs cannot be rewritten under the live
 * schema: some live column's type cannot be produced from an input
 * file's parquet type (anything outside identity or the int->long /
 * float->double promotions). The sweep treats this as skip-with-reason
 * (CompactionResult.unconvertibleSchema), never a failure.
 */
class UnconvertibleSchemaException(message: String) : IllegalArgumentException(message)

/**
 * The compaction rewrite writer, on parquet-java — the project's one
 * parquet library (decision 2026-09-05: Hardwood is out entirely).
 * Compaction outputs MUST carry field ids on every column (files bind
 * to catalog columns by id, never by name; field ids are a registration
 * contract, enforced at hydration via
 * hog_data_file.missing_field_ids + the AlterService rename guard).
 *
 * What a rewrite does: read every input file's rows, APPLY each input's
 * live deletion vector (skip the deleted physical ordinals), re-shape
 * the survivors under the table's LIVE schema, concatenate them in
 * row-id order, and write one output file in which each row's hoglake
 * row id rides an explicit physical int64 column [ROW_ID_COLUMN]
 * (reserved field id [ROW_ID_FIELD_ID]). Explicit ids are the point:
 * merged inputs need not be row-id-contiguous — and DV application
 * punches holes inside a file's range — so the output can never rely on
 * positional ids (row_id_start + offset), and, because every row
 * carries its id, reordering the merged rows by the table's sort order
 * is SAFE. This kills the predecessor's sorted-compaction hazard
 * (DuckLake's sorted merge_adjacent_files silently REMAPPED rowids,
 * breaking CDC identity downstream; hoglake row ids survive any
 * ordering because they are data, not position).
 *
 * Heterogeneous inputs (files written across ALTERs) map to the live
 * schema by FIELD ID: a live column absent from an input null-fills; an
 * input column whose field id the live schema no longer knows (dropped
 * column) drops its data; promoted columns up-cast (int32->int64,
 * float->double). Id-less input columns (pre-field-id writers) fall
 * back to live-name matching. The one refusal is a live column whose
 * type cannot be produced from the input's physical type —
 * [UnconvertibleSchemaException], the group stays uncompacted. Because
 * the output must hold every live column (null-filled ones included),
 * all data columns are written OPTIONAL; catalog nullability is
 * metadata, not parquet repetition.
 *
 * **Nested columns rewrite, they are not copied around.** The record
 * pipeline above is already the parquet-java Group API, and a `Group`
 * is a tree: `GroupRecordConverter` materializes the whole nested
 * record and `addGroup`/`getGroup` reach into it. So list, struct and
 * map extend the SAME plan-and-copy shape one level at a time — the
 * plan becomes a tree of [Step]s instead of a flat array — rather than
 * needing a copy-only escape hatch. That matters: making nested tables
 * `unconvertible_schema` would have meant a table with one `map` column
 * could never be compacted, which is a permanent debt leak, not a
 * deferral. The cost is per-ROW heap proportional to the nested payload
 * (the unsorted path still holds exactly one Group at a time) and a
 * recursive copy per row.
 *
 * Nested structure is spec-shaped on both sides: the 3-level LIST
 * encoding (`optional group x (LIST) { repeated group list { optional
 * <t> element } }`) and the MAP encoding (`optional group x (MAP) {
 * repeated group key_value { required <k> key; optional <v> value } }`).
 * Inputs are matched by SHAPE, not by the synthetic names, because the
 * parquet spec says those names are not significant — but an input whose
 * shape disagrees with the live column's type is
 * [UnconvertibleSchemaException], never a guess.
 */
object ParquetRewriter {
    /** The explicit row-id column compaction outputs carry. */
    const val ROW_ID_COLUMN = "_hog_row_id"

    /**
     * Reserved parquet field id for [ROW_ID_COLUMN] (documented in
     * V1__init.sql on hog_data_file.explicit_row_ids and in AGENT.md):
     * Int.MAX_VALUE - 1, far outside hog_table.next_field_id's reach.
     */
    const val ROW_ID_FIELD_ID = 2147483646

    /**
     * The synthetic repeated-group names the parquet LIST and MAP
     * encodings use. Written, never required on read: the parquet spec
     * says these names are not significant, and writers disagree about
     * them, so inputs are matched by SHAPE.
     */
    const val LIST_ENTRY_GROUP = "list"
    const val MAP_ENTRY_GROUP = "key_value"

    /**
     * One input file staged to local disk: its row-id range start and
     * the decoded live DV to apply (null = no live DV).
     */
    data class Input(
        val localPath: Path,
        val rowIdStart: Long,
        val deletes: DeletionVector? = null,
    )

    /**
     * [rowsWritten] survivors; [minRowId] their smallest row id (the
     * output's row_id_start), null when every input row was deleted.
     */
    data class RewriteResult(val rowsWritten: Long, val minRowId: Long?)

    private class Row(val group: Group, val rowId: Long)

    /**
     * How one matched input column lands in the output.
     *
     * [UINT32_TO_LONG] exists because unsigned parquet int32s must NOT
     * sign-extend: a uint32 above 2^31 would become negative, and a
     * foreign writer's unsigned int32 file under a `long` column is
     * exactly that pairing.
     *
     * There is deliberately no millis -> micros mode. It existed to
     * serve a timestamp_ms -> timestamp promotion, and that promotion
     * left the matrix once PROMOTIONS was pinned to DuckLake's
     * documented set (which has no timestamp rungs at all). With no
     * legal path producing a millis file under a micros column, the only
     * way to reach one is a writer disagreeing with its own DDL, and
     * refusing that is the rewriter's job.
     */
    private enum class CopyMode {
        IDENTITY,
        INT_TO_LONG,
        UINT32_TO_LONG,
        FLOAT_TO_DOUBLE,
        DECIMAL_INT32,
        DECIMAL_INT64,
        DECIMAL_BINARY,
    }

    /**
     * Merge [inputs] (caller orders them by rowIdStart) into [output]
     * under the live schema [liveColumns] (ordinal order). [sortFields]
     * non-empty sorts the merged survivors by the table's sort order
     * (nulls per spec); empty keeps row-id order.
     */
    fun rewrite(
        inputs: List<Input>,
        liveColumns: List<Column>,
        sortFields: List<SortFieldDef>,
        output: Path,
    ): RewriteResult {
        require(inputs.isNotEmpty()) { "rewrite needs at least one input" }
        val outputSchema = outputSchema(liveColumns)
        val dataFields = outputSchema.fields.dropLast(1) // all but _hog_row_id
        val rowIdIndex = outputSchema.fieldCount - 1
        val factory = SimpleGroupFactory(outputSchema)

        if (sortFields.isEmpty()) {
            // No sort order: STREAM — write each survivor as it is read,
            // never materializing the group. Materialize-then-write put
            // the whole group's Group objects on the heap and OOM'd the
            // server on a 400 MB catalog; heap must stay flat in group
            // size (parquet-java's own row-group buffering bounds it).
            //
            // Flat in GROUP size, not in ROW size. One row still
            // materializes whole — a million-element list is a million
            // SimpleGroups at once — and nothing here bounds that,
            // because nothing upstream bounds a row's element count.
            // Accepted limit, stated rather than implied.
            var written = 0L
            var minRowId: Long? = null
            newWriter(outputSchema, output).use { writer ->
                for (input in inputs) {
                    forEachSurvivor(input, liveColumns, dataFields, rowIdIndex, factory) { group, rowId ->
                        writer.write(group)
                        written++
                        minRowId = minOf(minRowId ?: rowId, rowId)
                    }
                }
            }
            return RewriteResult(written, minRowId)
        }

        // Sorted: survivors must be materialized to sort. Sorting is safe
        // ONLY because ids are explicit; sortedWith is stable, so ties
        // keep row-id order.
        //
        // The heap here is the group's OBJECT GRAPH, which is emphatically
        // NOT its byte budget — an earlier version of this comment
        // claimed the planner's targetBytes bounded it, and that is
        // false. A measured `list<long>` table with five elements per
        // row peaked at 343 MiB from a 4.6 MiB compressed input (70x):
        // every element is its own SimpleGroup with an object header, a
        // field array and a boxed value, and the compression that packs
        // an int64 column 10:1 does nothing for any of that. Flat rows
        // are a few boxed values each and stay near their byte size.
        //
        // The bound is therefore the planner's, not this loop's:
        // CompactionService derates the group budget by
        // CompactionConfig.nestedSortExpansion for a table that both
        // nests and sorts, so the materialized graph lands back under
        // roughly targetBytes. Per GROUP only — one pathological ROW
        // still materializes whole, as on the streaming path above.
        val rows = ArrayList<Row>()
        var minRowId: Long? = null
        for (input in inputs) {
            forEachSurvivor(input, liveColumns, dataFields, rowIdIndex, factory) { group, rowId ->
                rows.add(Row(group, rowId))
                minRowId = minOf(minRowId ?: rowId, rowId)
            }
        }
        val ordered = rows.sortedWith(comparator(outputSchema, sortFields))
        newWriter(outputSchema, output).use { writer -> for (row in ordered) writer.write(row.group) }
        return RewriteResult(ordered.size.toLong(), minRowId)
    }

    /** The per-input pipeline: apply the DV, map to the live schema, stamp the row id, emit. */
    private fun forEachSurvivor(
        input: Input,
        liveColumns: List<Column>,
        dataFields: List<Type>,
        rowIdIndex: Int,
        factory: SimpleGroupFactory,
        emit: (Group, Long) -> Unit,
    ) {
        val schema = readSchema(input.localPath)
        // A previously-compacted input carries its ids in its own
        // row-id column; positional ids would be wrong for it.
        val srcRowIdIndex =
            schema.fields.indexOfFirst { it.name == ROW_ID_COLUMN }.takeIf { it >= 0 }
        val plan = columnPlan(schema, liveColumns, input.localPath)
        var applied = 0L
        readRows(input.localPath, schema) { src, ordinal ->
            if (input.deletes?.contains(ordinal) == true) {
                applied++
                return@readRows
            }
            val dst = factory.newGroup()
            for ((outIdx, step) in plan.withIndex()) {
                if (step != null && src.getFieldRepetitionCount(step.srcIndex) > 0) {
                    copyField(src, step, dst, outIdx, dataFields[outIdx])
                }
            }
            val rowId =
                if (srcRowIdIndex != null) {
                    src.getLong(srcRowIdIndex, 0)
                } else {
                    input.rowIdStart + ordinal
                }
            dst.add(rowIdIndex, rowId)
            emit(dst, rowId)
        }
        val expected = input.deletes?.cardinality ?: 0L
        check(applied == expected) {
            "deletion vector for ${input.localPath} claims $expected positions but only " +
                "$applied fell inside the file — refusing a lossy compaction"
        }
    }

    private fun newWriter(
        outputSchema: MessageType,
        output: Path,
    ): ParquetWriter<Group> =
        ExampleParquetWriter.builder(LocalOutputFile(output))
            .withType(outputSchema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .build()

    // ---- schema ----------------------------------------------------------

    private fun readSchema(path: Path): MessageType =
        ParquetFileReader.open(LocalInputFile(path)).use { it.footer.fileMetaData.schema }

    /**
     * The output schema is the LIVE schema: every live column in
     * ordinal order (optional, field-id-stamped, type synthesized from
     * the catalog type), plus the required [ROW_ID_COLUMN].
     */
    private fun outputSchema(liveColumns: List<Column>): MessageType {
        require(liveColumns.isNotEmpty()) { "table has no live columns" }
        val dataFields = liveColumns.map { parquetTypeFor(it) }
        val rowIdField: Type =
            Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                .id(ROW_ID_FIELD_ID)
                .named(ROW_ID_COLUMN)
        return MessageType("hoglake_compacted", dataFields + rowIdField)
    }

    /**
     * Catalog type -> parquet type (pyhoglake's writer conventions:
     * micros times, fixed(16) uuid), recursive for the containers.
     *
     * [repetition] is OPTIONAL everywhere except a map's key, which the
     * parquet MAP encoding requires — catalog nullability is metadata,
     * so the output null-fills freely, but a required key is structure,
     * not metadata.
     */
    private fun parquetTypeFor(
        column: Column,
        repetition: Type.Repetition = Type.Repetition.OPTIONAL,
    ): Type {
        val id = Math.toIntExact(column.fieldId)
        val name = column.def.name

        // The CATALOG's arity, checked where the OUTPUT schema is built
        // — which happens before any planning, so a corrupt container
        // row reached `single()` here first and threw a raw
        // NoSuchElementException out of the sweep. Every other
        // disagreement with a file or a catalog is a typed skip; this
        // one has to be too.
        column.def.type.requiredChildCount?.let { required ->
            if (column.children.size != required) {
                throw UnconvertibleSchemaException(
                    "live ${column.def.type.wire} column '$name' has ${column.children.size} " +
                        "children, not $required; its catalog row is inconsistent",
                )
            }
        }
        if (column.def.type == ColType.STRUCT && column.children.isEmpty()) {
            throw UnconvertibleSchemaException(
                "live struct column '$name' has no children; its catalog row is inconsistent",
            )
        }

        fun prim(physical: PrimitiveType.PrimitiveTypeName) = Types.primitive(physical, repetition)

        return when (column.def.type) {
            ColType.BOOLEAN ->
                prim(PrimitiveType.PrimitiveTypeName.BOOLEAN).id(id).named(name)
            ColType.INT8 -> intColumn(id, name, 8, signed = true, repetition = repetition)
            ColType.INT16 -> intColumn(id, name, 16, signed = true, repetition = repetition)
            ColType.UINT8 -> intColumn(id, name, 8, signed = false, repetition = repetition)
            ColType.UINT16 -> intColumn(id, name, 16, signed = false, repetition = repetition)
            ColType.INT ->
                prim(PrimitiveType.PrimitiveTypeName.INT32).id(id).named(name)
            // uint32 is written as a plain INT64, NOT as the INT32 +
            // INT(32, unsigned) pyarrow and DuckDB emit natively: it maps
            // to Iceberg long, and an Iceberg reader takes an INT32 column
            // as SIGNED, so values above 2^31 would read back negative
            // through the facade. Reads still accept both forms; rewriting
            // converges files on the facade-readable one.
            ColType.UINT32 ->
                prim(PrimitiveType.PrimitiveTypeName.INT64).id(id).named(name)
            // uint64 keeps its native physical form. Its facade mapping is
            // decimal(20,0), which parquet cannot express as an INT64, so
            // uint64 is the one type whose FILES are not facade-readable in
            // place even though its BOUNDS already are (iceberg-federation.md §2).
            ColType.UINT64 -> intColumn(id, name, 64, signed = false, repetition = repetition)
            ColType.LONG ->
                prim(PrimitiveType.PrimitiveTypeName.INT64).id(id).named(name)
            ColType.FLOAT ->
                prim(PrimitiveType.PrimitiveTypeName.FLOAT).id(id).named(name)
            ColType.DOUBLE ->
                prim(PrimitiveType.PrimitiveTypeName.DOUBLE).id(id).named(name)
            ColType.DECIMAL ->
                prim(PrimitiveType.PrimitiveTypeName.BINARY)
                    .`as`(LogicalTypeAnnotation.decimalType(decimalScale(column) ?: 0, decimalPrecision(column)))
                    .id(id).named(name)
            ColType.DATE ->
                prim(PrimitiveType.PrimitiveTypeName.INT32)
                    .`as`(LogicalTypeAnnotation.dateType()).id(id).named(name)
            ColType.TIME ->
                prim(PrimitiveType.PrimitiveTypeName.INT64)
                    .`as`(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
                    .id(id).named(name)
            // Parquet has no seconds timestamp unit, so timestamp_s files
            // are physically MILLIS (pyarrow 25 coerces timestamp[s] on
            // write; verified) and rewrite to MILLIS unchanged. The
            // declared precision lives in the catalog, never in the file.
            ColType.TIMESTAMP_S, ColType.TIMESTAMP_MS ->
                prim(PrimitiveType.PrimitiveTypeName.INT64)
                    .`as`(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
                    .id(id).named(name)
            ColType.TIMESTAMP ->
                prim(PrimitiveType.PrimitiveTypeName.INT64)
                    .`as`(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
                    .id(id).named(name)
            ColType.TIMESTAMP_NS ->
                prim(PrimitiveType.PrimitiveTypeName.INT64)
                    .`as`(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.NANOS))
                    .id(id).named(name)
            ColType.TIMESTAMPTZ ->
                prim(PrimitiveType.PrimitiveTypeName.INT64)
                    .`as`(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
                    .id(id).named(name)
            ColType.STRING ->
                prim(PrimitiveType.PrimitiveTypeName.BINARY)
                    .`as`(LogicalTypeAnnotation.stringType()).id(id).named(name)
            // json maps to Iceberg string; the JSON annotation is the only
            // thing that distinguishes it physically, and the bytes are
            // copied verbatim — compaction never reformats a document.
            ColType.JSON ->
                prim(PrimitiveType.PrimitiveTypeName.BINARY)
                    .`as`(LogicalTypeAnnotation.jsonType()).id(id).named(name)
            ColType.UUID_T ->
                prim(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(16)
                    .`as`(LogicalTypeAnnotation.uuidType()).id(id).named(name)
            ColType.BINARY ->
                prim(PrimitiveType.PrimitiveTypeName.BINARY).id(id).named(name)
            // A struct is a plain group; its FIELDS are optional for the
            // same reason top-level columns are (an input predating one
            // null-fills it).
            ColType.STRUCT ->
                Types.buildGroup(repetition)
                    .addFields(*column.children.map { parquetTypeFor(it) }.toTypedArray())
                    .id(id).named(name)
            // The 3-level LIST encoding. The middle `list` group is
            // synthetic and carries NO field id: Iceberg puts the
            // element's id on the element, and a reader has nothing to
            // match an id on the repetition layer against.
            ColType.LIST ->
                Types.buildGroup(repetition)
                    .addField(
                        Types.repeatedGroup()
                            .addField(parquetTypeFor(column.children.single()))
                            .named(LIST_ENTRY_GROUP),
                    )
                    .`as`(LogicalTypeAnnotation.listType())
                    .id(id).named(name)
            // The MAP encoding. The key is REQUIRED — Iceberg map keys are
            // non-nullable and the parquet MAP shape says so too — which
            // is the one place the "write everything optional" rule yields.
            ColType.MAP ->
                Types.buildGroup(repetition)
                    .addFields(
                        Types.repeatedGroup()
                            .addFields(
                                parquetTypeFor(column.children[0], Type.Repetition.REQUIRED),
                                parquetTypeFor(column.children[1]),
                            )
                            .named(MAP_ENTRY_GROUP),
                    )
                    .`as`(LogicalTypeAnnotation.mapType())
                    .id(id).named(name)
        }
    }

    /** An INT32/INT64 column carrying parquet's INT(width, signed) annotation. */
    private fun intColumn(
        id: Int,
        name: String,
        width: Int,
        signed: Boolean,
        repetition: Type.Repetition = Type.Repetition.OPTIONAL,
    ): Type =
        Types.primitive(
            if (width == 64) PrimitiveType.PrimitiveTypeName.INT64 else PrimitiveType.PrimitiveTypeName.INT32,
            repetition,
        )
            .`as`(LogicalTypeAnnotation.intType(width, signed))
            .id(id).named(name)

    private fun decimalScale(column: Column): Int? = (column.def.typeParams?.get("scale") as? Number)?.toInt()

    private fun decimalPrecision(column: Column): Int =
        (column.def.typeParams?.get("precision") as? Number)?.toInt() ?: 38

    /**
     * One node of the copy plan: where this output field's value comes
     * from in the INPUT group holding it ([srcIndex], an index into the
     * parent group), and what to do with it.
     *
     * The plan is a tree because the record is: a struct copies its
     * children, a list copies its element once per repetition, a map
     * copies key and value per entry. [srcIndex] is always relative to
     * the group the step is read from, never absolute.
     */
    private sealed class Step {
        abstract val srcIndex: Int

        /** A primitive: copy the value, up-casting per [mode]. */
        class Scalar(override val srcIndex: Int, val mode: CopyMode) : Step()

        /** A struct: per OUTPUT child, its step (null = null-fill). */
        class StructStep(override val srcIndex: Int, val children: List<Step?>) : Step()

        /** A 3-level list: [element] reads out of each repeated entry group. */
        class ListStep(override val srcIndex: Int, val element: Step) : Step()

        /** A map: [key] and [value] read out of each repeated key_value group. */
        class MapStep(override val srcIndex: Int, val key: Step, val value: Step) : Step()
    }

    /**
     * Per live column (output order): where its value comes from in
     * [schema] and how it is up-cast, or null when the input predates
     * the column (null-fill). Matching is by parquet field id, with a
     * live-NAME fallback for id-less input columns. Unmatched input
     * columns (dropped field ids, unknown id-less names) simply do not
     * appear in any plan — their data drops with the rewrite, exactly
     * like every reader already treats them.
     */
    private fun columnPlan(
        schema: MessageType,
        liveColumns: List<Column>,
        inputPath: Path,
    ): List<Step?> {
        // DUPLICATE IDS, before any binding. planChildren elects the
        // FIRST field with a matching id, so a file declaring one id
        // twice would have had the rewrite source live data from
        // whichever came first — and then end-snapshot the input, making
        // the guess permanent. There is no correct resolution, so the
        // group skips with unconvertible_schema instead.
        val duplicates = duplicateFieldIds(schema.fields)
        if (duplicates.isNotEmpty()) {
            throw UnconvertibleSchemaException(
                "$inputPath declares field id(s) ${duplicates.sorted()} more than once; field ids " +
                    "are the binding contract and a duplicate has no correct resolution",
            )
        }
        return planChildren(schema.fields, liveColumns, inputPath)
    }

    /** Field ids [fields] declares more than once, at any depth. */
    private fun duplicateFieldIds(fields: List<Type>): Set<Int> {
        val seen = HashSet<Int>()
        val dupes = HashSet<Int>()

        fun walk(level: List<Type>) {
            for (field in level) {
                field.id?.intValue()?.let { if (!seen.add(it)) dupes.add(it) }
                if (!field.isPrimitive) walk(field.asGroupType().fields)
            }
        }
        walk(fields)
        return dupes
    }

    /** [columnPlan]'s recursion: one step per live column among [srcFields]. */
    private fun planChildren(
        srcFields: List<Type>,
        liveColumns: List<Column>,
        inputPath: Path,
    ): List<Step?> =
        liveColumns.map { column ->
            val srcIndex =
                srcFields.indexOfFirst { it.id?.intValue()?.toLong() == column.fieldId }
                    .takeIf { it >= 0 }
                    ?: srcFields.indexOfFirst { it.id == null && it.name == column.def.name }
                        .takeIf { it >= 0 }
                    ?: return@map null
            planNode(srcFields[srcIndex], srcIndex, column, inputPath)
        }

    /**
     * The step for one SYNTHETIC child — a list's element, a map's key
     * or value — at [position] inside the repetition layer.
     *
     * Bound by ID when the file declares one, by POSITION when it does
     * not, and never by NAME. The parquet spec says the synthetic names
     * are insignificant (this object's own `repeatedEntryGroup` says so
     * about the layer above), writers use `item`, `bag`, `entries` — but
     * [planChildren]'s id-less fallback matches on the LIVE column's
     * name, which for a list element is always `element`. So an id-less
     * element named `item` was refused here while the reader bound it
     * positionally and produced stats: the two surfaces disagreeing
     * about the same file, which is the drift the shared
     * `maxUnsignedParquetWidth` exists to prevent.
     *
     * The positional fallback is the same exemption `missingFieldIds`
     * grants the repetition layer, and safe for the same reason: such a
     * file is already flagged, so renames on its table are blocked and
     * position cannot drift out from under it.
     */
    private fun planSynthetic(
        srcFields: List<Type>,
        position: Int,
        column: Column,
        inputPath: Path,
    ): Step? {
        // POSITION decides, and the id only VERIFIES — the reader's rule
        // (FooterStats.childBinds), and the two have to hold the same
        // one. Searching for the id ANYWHERE accepted an entry group
        // whose key and value are in the other order, and then the copy
        // below writes the key unconditionally into slot 0 on the
        // strength of the input key being REQUIRED — which is only true
        // of the field actually in slot 0. Measured: a swapped-order
        // file planned fine and threw `not found 1(key) element number
        // 0` mid-copy, which the sweep counts as a FAILED group (error
        // level, retried forever) rather than the skip-with-reason it is.
        val candidate = srcFields.getOrNull(position) ?: return null
        val id = candidate.id
        if (id != null && id.intValue().toLong() != column.fieldId) return null
        return planNode(candidate, position, column, inputPath)
    }

    /**
     * The step producing [column] from the input field [src].
     *
     * **An unmatched child INSIDE a container aborts the group; an
     * unmatched TOP-LEVEL column null-fills.** The asymmetry is
     * deliberate. A top-level column the input lacks is ordinary schema
     * evolution — the column was added after the file was written, every
     * reader already shows null for it, and null-filling reproduces
     * exactly what a reader sees. Inside a container there is no such
     * reading: a list with no element, or a map with no key, is not a
     * column that arrived late, it is a shape disagreement about a
     * structure that cannot exist without that member. Null-filling it
     * would invent a row count and a repetition structure nothing in the
     * input implies. So the group skips with `unconvertible_schema` —
     * self-healing once the schema or the file set changes, and never
     * wrong bytes in the meantime. Never guess inside a container.
     *
     * A STRUCT field is the one interior that does null-fill, and for
     * the top-level reason: a struct's members ARE independently
     * evolvable columns (add_column with a `parent`), so one the input
     * predates is the same situation one level down.
     */

    private fun planNode(
        src: Type,
        srcIndex: Int,
        column: Column,
        inputPath: Path,
    ): Step {
        fun refuseShape(detail: String): Nothing =
            throw UnconvertibleSchemaException(
                "column '${column.def.name}' (live type ${column.def.type.wire}) cannot be " +
                    "produced from $inputPath: $detail",
            )

        // REPETITION, before anything else. Every catalog type reachable
        // here holds AT MOST ONE value per row: a scalar, a struct, or a
        // container whose repetition lives in its own synthetic layer
        // (which planNode is never handed — it descends THROUGH it). A
        // REPEATED node says "many per row", and the copy below reads
        // repetition 0 and only repetition 0. Measured before this
        // guard: a 2-repetition struct rewrote to its first repetition
        // alone, half the values gone, rowsWritten still equal to the
        // record count so nothing looked wrong — and the inputs were
        // then end-snapshotted and expired.
        if (src.isRepetition(Type.Repetition.REPEATED)) {
            refuseShape(
                "the input field is REPEATED, but '${column.def.type.wire}' holds one value per row",
            )
        }

        if (!column.def.type.isNested) {
            if (!src.isPrimitive) refuseShape("the input field is a group, not a primitive leaf")
            return Step.Scalar(srcIndex, copyMode(src.asPrimitiveType(), column, inputPath))
        }
        if (src.isPrimitive) refuseShape("the input field is a primitive leaf, not a group")
        val group = src.asGroupType()
        return when (column.def.type) {
            ColType.STRUCT -> {
                // A struct's parquet counterpart is a PLAIN group. A
                // LIST or MAP wrapper carrying the struct's field id is
                // not "a struct with unfamiliar children", it is a
                // different type wearing the same id — and treating it
                // as a struct is the one shape that loses data silently:
                // the wrapper's only child is the repetition layer, so
                // every one of the struct's fields fails to match, and
                // a struct's fields are the ONE interior that null-fills
                // (see the note above). The rewrite would then produce
                // rows of empty structs, and the commit would
                // end-snapshot the input that held the real values —
                // F1's shape through a different door. Refuse instead.
                // CONTAINER annotations only, matching the reader
                // (FooterStats.isContainerAnnotation). Refusing ANY
                // annotation made a struct-shaped group carrying a stray
                // unrelated one (ENUM, say) permanently uncompactable,
                // and the reader stopped bounding it too — a table that
                // worked before this phase would have quietly stopped.
                val annotation = group.logicalTypeAnnotation
                if (FooterStats.isContainerAnnotation(annotation)) {
                    refuseShape(
                        "the input field is a '$annotation' group, not a struct; a container " +
                            "wearing a struct's field id is a type mismatch, not a schema evolution",
                    )
                }
                Step.StructStep(srcIndex, planChildren(group.fields, column.children, inputPath))
            }
            ColType.LIST -> {
                val entry =
                    repeatedEntryGroup(group)
                        ?: refuseShape("the input field is not the 3-level LIST encoding")
                if (entry.fieldCount != 1) {
                    refuseShape("the input list's repeated group has ${entry.fieldCount} fields, not 1")
                }
                // The CATALOG's arity too: a container row with the wrong
                // child count is a corrupt catalog, and `single()` threw
                // a raw NoSuchElementException out of the sweep.
                if (column.children.size != 1) {
                    refuseShape(
                        "the live list column has ${column.children.size} children, not 1 (its element)",
                    )
                }
                val element =
                    planSynthetic(entry.fields, 0, column.children[0], inputPath)
                        ?: refuseShape("the input list's element does not match the live element field id")
                Step.ListStep(srcIndex, element)
            }
            ColType.MAP -> {
                val entry =
                    repeatedEntryGroup(group)
                        ?: refuseShape("the input field is not the MAP encoding")
                if (entry.fieldCount != 2) {
                    refuseShape("the input map's key_value group has ${entry.fieldCount} fields, not 2")
                }
                // A key the input marks OPTIONAL cannot be copied into
                // the required output key: some row may have none, and
                // parquet would fail the write halfway through the group.
                // Refuse at plan time instead — the whole point of
                // unconvertible_schema.
                if (!entry.getType(0).isRepetition(Type.Repetition.REQUIRED)) {
                    refuseShape("the input map's key is not REQUIRED; Iceberg map keys are non-nullable")
                }
                if (column.children.size != 2) {
                    refuseShape(
                        "the live map column has ${column.children.size} children, not 2 (key, value)",
                    )
                }
                val key =
                    planSynthetic(entry.fields, 0, column.children[0], inputPath)
                        ?: refuseShape("the input map's key does not match the live key field id")
                val value =
                    planSynthetic(entry.fields, 1, column.children[1], inputPath)
                        ?: refuseShape("the input map's value does not match the live value field id")
                Step.MapStep(srcIndex, key, value)
            }
            else -> error("unreachable: ${column.def.type} is not a container")
        }
    }

    /**
     * The single repeated group inside a LIST/MAP wrapper, or null when
     * [group] is not that shape. The group's NAME is not checked: the
     * parquet spec says the synthetic names are insignificant, and
     * writers disagree about them (`list` vs `bag`, `key_value` vs
     * `map`). The shape is what carries meaning.
     */
    private fun repeatedEntryGroup(group: org.apache.parquet.schema.GroupType): org.apache.parquet.schema.GroupType? {
        val only = group.fields.singleOrNull() ?: return null
        if (only.isPrimitive || !only.isRepetition(Type.Repetition.REPEATED)) return null
        return only.asGroupType()
    }

    /**
     * How the live column is produced from the input's physical type:
     * identity, int32->int64, or float->double. Anything else — a
     * narrowing, a physical mismatch, a decimal scale change, a
     * non-micros time(stamp) unit — is [UnconvertibleSchemaException].
     */
    private fun copyMode(
        src: PrimitiveType,
        column: Column,
        inputPath: Path,
    ): CopyMode {
        val srcName = src.primitiveTypeName
        val live = column.def.type

        fun refuse(): Nothing =
            throw UnconvertibleSchemaException(
                "column '${column.def.name}' (live type ${live.wire}) cannot be produced from " +
                    "$srcName${src.logicalTypeAnnotation?.let { " ($it)" } ?: ""} in $inputPath",
            )

        // The same domain rule the hydrator's footer decode applies
        // (ColType.maxUnsignedParquetWidth). Without it the two surfaces
        // DISAGREED: an INT(32, unsigned) file under an int8 column was
        // refused by the hydrator and copied through by the rewriter,
        // which then re-stamped it with the live column's INT(8, signed)
        // annotation — compaction laundering an annotation the hydrator
        // had rejected, and turning a file with no bounds into a file
        // with wrong ones. Both surfaces now refuse.
        val unsignedWidth = unsignedWidthOf(src)
        if (unsignedWidth != null && unsignedWidth > live.maxUnsignedParquetWidth) refuse()

        return when (live) {
            ColType.BOOLEAN ->
                if (srcName == PrimitiveType.PrimitiveTypeName.BOOLEAN) CopyMode.IDENTITY else refuse()
            // Every width <= 16 (signed or not) rides parquet INT32 and
            // holds its true value there, so the int8/int16/uint8/uint16
            // promotion ladder is a physical no-op: copy the int32.
            ColType.INT8, ColType.INT16, ColType.UINT8, ColType.UINT16, ColType.INT ->
                if (srcName == PrimitiveType.PrimitiveTypeName.INT32) CopyMode.IDENTITY else refuse()
            // uint32 reads back from either physical form (see
            // parquetTypeFor) and always writes INT64. A plain signed
            // INT32 is refused: nothing legal produces one for a uint32
            // column, and reading it would be a guess about the sign.
            ColType.UINT32 ->
                when {
                    srcName == PrimitiveType.PrimitiveTypeName.INT64 -> CopyMode.IDENTITY
                    srcName == PrimitiveType.PrimitiveTypeName.INT32 && isUnsigned(src) ->
                        CopyMode.UINT32_TO_LONG
                    else -> refuse()
                }
            ColType.UINT64 ->
                if (srcName == PrimitiveType.PrimitiveTypeName.INT64 && isUnsigned(src)) {
                    CopyMode.IDENTITY
                } else {
                    refuse()
                }
            ColType.LONG ->
                when {
                    srcName == PrimitiveType.PrimitiveTypeName.INT64 -> CopyMode.IDENTITY
                    // A foreign writer's unsigned int32 under a `long`
                    // column: zero-extend. Sign-extending an unsigned
                    // int32 above 2^31 silently negates it.
                    srcName == PrimitiveType.PrimitiveTypeName.INT32 && isUnsigned(src) ->
                        CopyMode.UINT32_TO_LONG
                    srcName == PrimitiveType.PrimitiveTypeName.INT32 -> CopyMode.INT_TO_LONG
                    else -> refuse()
                }
            ColType.FLOAT ->
                if (srcName == PrimitiveType.PrimitiveTypeName.FLOAT) CopyMode.IDENTITY else refuse()
            ColType.DOUBLE ->
                when (srcName) {
                    PrimitiveType.PrimitiveTypeName.DOUBLE -> CopyMode.IDENTITY
                    PrimitiveType.PrimitiveTypeName.FLOAT -> CopyMode.FLOAT_TO_DOUBLE
                    else -> refuse()
                }
            ColType.DATE ->
                if (srcName == PrimitiveType.PrimitiveTypeName.INT32) CopyMode.IDENTITY else refuse()
            ColType.TIME -> {
                val unit = (src.logicalTypeAnnotation as? LogicalTypeAnnotation.TimeLogicalTypeAnnotation)?.unit
                if (srcName == PrimitiveType.PrimitiveTypeName.INT64 &&
                    (unit == null || unit == LogicalTypeAnnotation.TimeUnit.MICROS)
                ) {
                    CopyMode.IDENTITY
                } else {
                    refuse()
                }
            }
            // timestamp_s and timestamp_ms are both physically MILLIS —
            // parquet has no seconds unit — so one arm serves both. They
            // are distinct catalog types, not promotable to each other.
            ColType.TIMESTAMP_S, ColType.TIMESTAMP_MS -> {
                val unit = timestampUnit(src)
                if (srcName == PrimitiveType.PrimitiveTypeName.INT64 &&
                    (unit == null || unit == LogicalTypeAnnotation.TimeUnit.MILLIS)
                ) {
                    CopyMode.IDENTITY
                } else {
                    refuse()
                }
            }
            ColType.TIMESTAMP, ColType.TIMESTAMPTZ -> {
                val unit = timestampUnit(src)
                if (srcName == PrimitiveType.PrimitiveTypeName.INT64 &&
                    (unit == null || unit == LogicalTypeAnnotation.TimeUnit.MICROS)
                ) {
                    CopyMode.IDENTITY
                } else {
                    refuse()
                }
            }
            ColType.TIMESTAMP_NS -> {
                // Nothing promotes INTO timestamp_ns, so a nanos input is
                // the only shape that can legally exist here.
                if (srcName == PrimitiveType.PrimitiveTypeName.INT64 &&
                    timestampUnit(src) == LogicalTypeAnnotation.TimeUnit.NANOS
                ) {
                    CopyMode.IDENTITY
                } else {
                    refuse()
                }
            }
            // json and string are both BYTE_ARRAY; the bytes pass through
            // untouched either way.
            ColType.STRING, ColType.JSON ->
                if (srcName == PrimitiveType.PrimitiveTypeName.BINARY) CopyMode.IDENTITY else refuse()
            ColType.BINARY ->
                if (srcName == PrimitiveType.PrimitiveTypeName.BINARY) CopyMode.IDENTITY else refuse()
            ColType.UUID_T ->
                if (srcName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY && src.typeLength == 16) {
                    CopyMode.IDENTITY
                } else {
                    refuse()
                }
            ColType.DECIMAL -> {
                val annotation =
                    src.logicalTypeAnnotation as? LogicalTypeAnnotation.DecimalLogicalTypeAnnotation
                        ?: refuse()
                val liveScale = decimalScale(column) ?: 0
                if (annotation.scale != liveScale ||
                    annotation.precision > decimalPrecision(column)
                ) {
                    refuse()
                }
                when (srcName) {
                    PrimitiveType.PrimitiveTypeName.INT32 -> CopyMode.DECIMAL_INT32
                    PrimitiveType.PrimitiveTypeName.INT64 -> CopyMode.DECIMAL_INT64
                    PrimitiveType.PrimitiveTypeName.BINARY,
                    PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
                    -> CopyMode.DECIMAL_BINARY
                    else -> refuse()
                }
            }
            // Unreachable: planNode routes containers to their own steps
            // and only ever calls copyMode for a scalar live column.
            ColType.LIST, ColType.STRUCT, ColType.MAP -> refuse()
        }
    }

    private fun isUnsigned(src: PrimitiveType): Boolean = unsignedWidthOf(src) != null

    /** The source's unsigned INT width, or null when it is not unsigned-annotated. */
    private fun unsignedWidthOf(src: PrimitiveType): Int? =
        (src.logicalTypeAnnotation as? LogicalTypeAnnotation.IntLogicalTypeAnnotation)
            ?.takeIf { !it.isSigned }
            ?.bitWidth

    private fun timestampUnit(src: PrimitiveType): LogicalTypeAnnotation.TimeUnit? =
        (src.logicalTypeAnnotation as? LogicalTypeAnnotation.TimestampLogicalTypeAnnotation)?.unit

    // ---- row IO ----------------------------------------------------------

    private fun readRows(
        path: Path,
        schema: MessageType,
        consume: (Group, Long) -> Unit,
    ) {
        ParquetFileReader.open(LocalInputFile(path)).use { reader ->
            val columnIO = ColumnIOFactory().getColumnIO(schema)
            var ordinal = 0L
            var pages = reader.readNextRowGroup()
            while (pages != null) {
                val recordReader = columnIO.getRecordReader(pages, GroupRecordConverter(schema))
                repeat(Math.toIntExact(pages.rowCount)) {
                    consume(recordReader.read(), ordinal++)
                }
                pages = reader.readNextRowGroup()
            }
        }
    }

    /**
     * Copy one present field from [src] into [dst] at [dstIdx], where
     * [dstType] is the OUTPUT type at that position. Recursive for the
     * containers; the caller has already checked the source field is
     * present (repetition count > 0).
     */
    private fun copyField(
        src: Group,
        step: Step,
        dst: Group,
        dstIdx: Int,
        dstType: Type,
    ) {
        when (step) {
            is Step.Scalar -> copyValue(src, step, dst, dstIdx, dstType.asPrimitiveType())
            is Step.StructStep -> {
                val srcGroup = src.getGroup(step.srcIndex, 0)
                val dstGroup = dst.addGroup(dstIdx)
                val dstStruct = dstType.asGroupType()
                for ((i, child) in step.children.withIndex()) {
                    if (child != null && srcGroup.getFieldRepetitionCount(child.srcIndex) > 0) {
                        copyField(srcGroup, child, dstGroup, i, dstStruct.getType(i))
                    }
                }
            }
            is Step.ListStep -> {
                // An EMPTY list stays an empty list, distinct from null:
                // the output group is created either way, and only the
                // entries repeat.
                val srcList = src.getGroup(step.srcIndex, 0)
                val dstList = dst.addGroup(dstIdx)
                val dstEntryType = dstType.asGroupType().getType(0).asGroupType()
                val n = srcList.getFieldRepetitionCount(0)
                for (i in 0 until n) {
                    val srcEntry = srcList.getGroup(0, i)
                    val dstEntry = dstList.addGroup(0)
                    if (srcEntry.getFieldRepetitionCount(step.element.srcIndex) > 0) {
                        copyField(srcEntry, step.element, dstEntry, 0, dstEntryType.getType(0))
                    }
                }
            }
            is Step.MapStep -> {
                val srcMap = src.getGroup(step.srcIndex, 0)
                val dstMap = dst.addGroup(dstIdx)
                val dstEntryType = dstType.asGroupType().getType(0).asGroupType()
                val n = srcMap.getFieldRepetitionCount(0)
                for (i in 0 until n) {
                    val srcEntry = srcMap.getGroup(0, i)
                    val dstEntry = dstMap.addGroup(0)
                    // The key is REQUIRED on both sides (planNode refuses
                    // an optional input key), so it is always present.
                    copyField(srcEntry, step.key, dstEntry, 0, dstEntryType.getType(0))
                    if (srcEntry.getFieldRepetitionCount(step.value.srcIndex) > 0) {
                        copyField(srcEntry, step.value, dstEntry, 1, dstEntryType.getType(1))
                    }
                }
            }
        }
    }

    private fun copyValue(
        src: Group,
        step: Step.Scalar,
        dst: Group,
        dstIdx: Int,
        primitive: PrimitiveType,
    ) {
        val srcIdx = step.srcIndex
        when (step.mode) {
            CopyMode.INT_TO_LONG -> dst.add(dstIdx, src.getInteger(srcIdx, 0).toLong())
            // Zero-extend, never sign-extend: this mode exists precisely
            // for the int32 bit patterns whose unsigned reading is > 2^31.
            CopyMode.UINT32_TO_LONG ->
                dst.add(dstIdx, src.getInteger(srcIdx, 0).toLong() and 0xFFFFFFFFL)
            CopyMode.FLOAT_TO_DOUBLE -> dst.add(dstIdx, src.getFloat(srcIdx, 0).toDouble())
            CopyMode.DECIMAL_INT32, CopyMode.DECIMAL_INT64, CopyMode.DECIMAL_BINARY -> {
                // Binary decimals can exceed Long.MAX_VALUE. Never narrow them through Long.
                val unscaled =
                    when (step.mode) {
                        CopyMode.DECIMAL_INT32 -> BigInteger.valueOf(src.getInteger(srcIdx, 0).toLong())
                        CopyMode.DECIMAL_INT64 -> BigInteger.valueOf(src.getLong(srcIdx, 0))
                        else -> BigInteger(src.getBinary(srcIdx, 0).bytes)
                    }
                val annotation = primitive.logicalTypeAnnotation as LogicalTypeAnnotation.DecimalLogicalTypeAnnotation
                val precision = annotation.precision
                if (unscaled.abs().toString().length > precision) {
                    throw UnconvertibleSchemaException("decimal value exceeds destination precision $precision")
                }
                dst.add(dstIdx, Binary.fromConstantByteArray(unscaled.toByteArray()))
            }
            CopyMode.IDENTITY ->
                when (primitive.primitiveTypeName) {
                    PrimitiveType.PrimitiveTypeName.BOOLEAN -> dst.add(dstIdx, src.getBoolean(srcIdx, 0))
                    PrimitiveType.PrimitiveTypeName.INT32 -> dst.add(dstIdx, src.getInteger(srcIdx, 0))
                    PrimitiveType.PrimitiveTypeName.INT64 -> dst.add(dstIdx, src.getLong(srcIdx, 0))
                    PrimitiveType.PrimitiveTypeName.FLOAT -> dst.add(dstIdx, src.getFloat(srcIdx, 0))
                    PrimitiveType.PrimitiveTypeName.DOUBLE -> dst.add(dstIdx, src.getDouble(srcIdx, 0))
                    PrimitiveType.PrimitiveTypeName.BINARY,
                    PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
                    -> dst.add(dstIdx, src.getBinary(srcIdx, 0))
                    PrimitiveType.PrimitiveTypeName.INT96, null ->
                        throw IllegalArgumentException(
                            "unsupported physical type ${primitive.primitiveTypeName}",
                        )
                }
        }
    }

    // ---- sorting ---------------------------------------------------------

    /**
     * One resolved sort key: the chain of field indexes from the record
     * root down to the primitive leaf, and the leaf's type.
     *
     * The chain has more than one element only for a STRUCT leaf, which
     * Iceberg (and AlterService) allow as a sort source. Nothing under a
     * list or a map can be one — a row has many such values — and the
     * container types themselves are not sortable at all; both are
     * refused at DDL time, and [sortKeyPath] refuses them again here so
     * a hand-built spec cannot reach the comparator.
     */
    internal class SortKey(
        val path: List<Int>,
        val primitive: PrimitiveType,
        val field: SortFieldDef,
    )

    private fun comparator(
        schema: MessageType,
        sortFields: List<SortFieldDef>,
    ): Comparator<Row> {
        val keys = sortFields.map { sortKeyPath(schema, it) }
        return Comparator { a, b ->
            for (key in keys) {
                val aGroup = navigate(a.group, key.path)
                val bGroup = navigate(b.group, key.path)
                val leafIdx = key.path.last()
                val aNull = aGroup == null || aGroup.getFieldRepetitionCount(leafIdx) == 0
                val bNull = bGroup == null || bGroup.getFieldRepetitionCount(leafIdx) == 0
                if (aNull || bNull) {
                    if (aNull && bNull) continue
                    val nullsFirst = key.field.nullOrder == com.posthog.hoglake.model.NullOrder.NULLS_FIRST
                    return@Comparator if (aNull == nullsFirst) -1 else 1
                }
                var c = compareNonNull(key.primitive, aGroup!!, bGroup!!, leafIdx)
                if (key.field.direction == SortDirection.DESC) c = -c
                if (c != 0) return@Comparator c
            }
            0
        }
    }

    /**
     * The group holding the leaf, walking [path]'s struct levels; null
     * when an ancestor struct is itself null (which makes the leaf null).
     */
    private fun navigate(
        root: Group,
        path: List<Int>,
    ): Group? {
        var g: Group = root
        for (i in 0 until path.size - 1) {
            if (g.getFieldRepetitionCount(path[i]) == 0) return null
            g = g.getGroup(path[i], 0)
        }
        return g
    }

    /**
     * Resolve one sort field to its index chain in the output schema.
     *
     * Internal rather than private so the refusals can be tested
     * directly: [rewrite] only ever hands this the schema [outputSchema]
     * just built, so the shapes it has to refuse — a repeated group with
     * no logical annotation, a container carrying the sort field's id —
     * are unreachable through the public entry point. A guard no test
     * can reach is not a guard.
     */
    internal fun sortKeyPath(
        schema: MessageType,
        field: SortFieldDef,
    ): SortKey {
        fun search(
            group: org.apache.parquet.schema.GroupType,
            prefix: List<Int>,
        ): Pair<List<Int>, Type>? {
            group.fields.forEachIndexed { i, type ->
                val here = prefix + i
                if (type.id?.intValue()?.toLong() == field.sourceFieldId) return here to type
                // Only STRUCT interiors are searched: a repeated group
                // (list/map) holds many values per row, so nothing under
                // one is addressable as a single sort key.
                if (!type.isPrimitive && !type.isRepetition(Type.Repetition.REPEATED)) {
                    val group2 = type.asGroupType()
                    if (group2.logicalTypeAnnotation == null) {
                        search(group2, here)?.let { return it }
                    }
                }
            }
            return null
        }
        val found =
            search(schema, emptyList())
                ?: throw UnconvertibleSchemaException(
                    "sort source field_id ${field.sourceFieldId} is not a top-level column or a " +
                        "struct leaf of the output schema; list/map internals and nested containers " +
                        "are not sortable",
                )
        val (path, type) = found
        if (!type.isPrimitive) {
            throw UnconvertibleSchemaException(
                "sort source field_id ${field.sourceFieldId} is a nested container " +
                    "('${type.name}'), and nested containers have no sort order",
            )
        }
        return SortKey(path, type.asPrimitiveType(), field)
    }

    /**
     * Typed comparison per physical type. Decimal-annotated binary
     * compares as the signed two's-complement BigInteger (an unsigned
     * byte compare mis-sorts negatives); other binary/fixed (string,
     * uuid, raw bytes) compare unsigned lexicographic, which for UTF-8
     * strings is code-point order. Integers annotated INT(w, unsigned)
     * compare UNSIGNED — a signed compare puts every uint64 above 2^63
     * (and every uint32 above 2^31) below zero, which is the same class
     * of bug as the decimal case. Float/Double order NaN greatest
     * (Kotlin's natural compareTo).
     */
    private fun compareNonNull(
        primitive: PrimitiveType,
        a: Group,
        b: Group,
        idx: Int,
    ): Int =
        when (primitive.primitiveTypeName) {
            PrimitiveType.PrimitiveTypeName.BOOLEAN ->
                a.getBoolean(idx, 0).compareTo(b.getBoolean(idx, 0))
            PrimitiveType.PrimitiveTypeName.INT32 ->
                if (isUnsigned(primitive)) {
                    Integer.compareUnsigned(a.getInteger(idx, 0), b.getInteger(idx, 0))
                } else {
                    a.getInteger(idx, 0).compareTo(b.getInteger(idx, 0))
                }
            PrimitiveType.PrimitiveTypeName.INT64 ->
                if (isUnsigned(primitive)) {
                    java.lang.Long.compareUnsigned(a.getLong(idx, 0), b.getLong(idx, 0))
                } else {
                    a.getLong(idx, 0).compareTo(b.getLong(idx, 0))
                }
            PrimitiveType.PrimitiveTypeName.FLOAT ->
                a.getFloat(idx, 0).compareTo(b.getFloat(idx, 0))
            PrimitiveType.PrimitiveTypeName.DOUBLE ->
                a.getDouble(idx, 0).compareTo(b.getDouble(idx, 0))
            PrimitiveType.PrimitiveTypeName.BINARY,
            PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
            -> {
                val ab = a.getBinary(idx, 0).bytes
                val bb = b.getBinary(idx, 0).bytes
                if (primitive.logicalTypeAnnotation is LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
                    BigInteger(ab).compareTo(BigInteger(bb))
                } else {
                    java.util.Arrays.compareUnsigned(ab, bb)
                }
            }
            PrimitiveType.PrimitiveTypeName.INT96, null ->
                throw IllegalArgumentException("unsupported sort key type ${primitive.primitiveTypeName}")
        }
}
