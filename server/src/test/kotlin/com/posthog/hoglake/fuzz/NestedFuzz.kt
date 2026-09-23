@file:Suppress("TooManyFunctions")

package com.posthog.hoglake.fuzz

import com.code_intelligence.jazzer.api.FuzzedDataProvider
import com.posthog.hoglake.hydrator.CatalogColumn
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.MAX_COLUMN_NESTING_DEPTH
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.ParquetReadOptions
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.io.OutputFile
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types
import java.math.BigInteger
import java.nio.file.Path

/**
 * Shared generator for the phase-2 nested fuzz campaigns.
 *
 * Entropy is abstracted so the SAME generator drives both a jazzer
 * `@FuzzTest` (coverage-guided, house pattern) and a deterministic
 * seed-loop soak runner (replayable findings: seed N always rebuilds the
 * same catalog tree, parquet schema and data file).
 */
interface Entropy {
    fun bool(): Boolean

    /** Uniform in [lo, hi]. */
    fun int(
        lo: Int,
        hi: Int,
    ): Int

    fun rawInt(): Int

    fun rawLong(): Long

    fun bytes(n: Int): ByteArray
}

class RandomEntropy(seed: Long) : Entropy {
    private val r = java.util.Random(seed)

    override fun bool(): Boolean = r.nextBoolean()

    override fun int(
        lo: Int,
        hi: Int,
    ): Int = if (hi <= lo) lo else lo + r.nextInt(hi - lo + 1)

    override fun rawInt(): Int = r.nextInt()

    override fun rawLong(): Long = r.nextLong()

    override fun bytes(n: Int): ByteArray = ByteArray(n).also { r.nextBytes(it) }
}

class FdpEntropy(private val fdp: FuzzedDataProvider) : Entropy {
    private val fallback = java.util.Random(0xC0FFEE)

    override fun bool(): Boolean = if (fdp.remainingBytes() > 0) fdp.consumeBoolean() else fallback.nextBoolean()

    override fun int(
        lo: Int,
        hi: Int,
    ): Int =
        if (hi <= lo) {
            lo
        } else if (fdp.remainingBytes() > 0) {
            fdp.consumeInt(lo, hi)
        } else {
            lo + fallback.nextInt(hi - lo + 1)
        }

    override fun rawInt(): Int = if (fdp.remainingBytes() >= 4) fdp.consumeInt() else fallback.nextInt()

    override fun rawLong(): Long = if (fdp.remainingBytes() >= 8) fdp.consumeLong() else fallback.nextLong()

    override fun bytes(n: Int): ByteArray {
        val got = if (fdp.remainingBytes() > 0) fdp.consumeBytes(n) else ByteArray(0)
        return if (got.size == n) got else got + ByteArray(n - got.size) { fallback.nextInt().toByte() }
    }
}

object NestedFuzz {
    /**
     * The scalar pool this campaign draws from.
     *
     * VARIANT is excluded although it IS a catalog scalar. Its parquet
     * shape is a group of `metadata`/`value`/`typed_value`, and
     * `deriveSchema` would emit a primitive for it — a file no writer
     * would produce, exercising the reader's variant arm against
     * nonsense rather than against variants. Including it properly
     * means teaching the generator that shape, which is worth doing
     * when variant compaction exists to test; until then the honest
     * move is to leave it out and say so.
     */
    val SCALARS: List<ColType> = ColType.entries.filter { !it.isNested && it != ColType.VARIANT }

    // ---- catalog side ---------------------------------------------------

    /** A valid-by-construction column forest: arities, synthetic names, depth cap. */
    fun genDefs(
        e: Entropy,
        count: Int,
        depthLeft: Int,
    ): List<ColumnDef> = (0 until count).map { i -> genDef(e, depthLeft, "c$i") }

    private fun genDef(
        e: Entropy,
        depthLeft: Int,
        name: String,
    ): ColumnDef {
        val nested = depthLeft > 1 && e.int(0, 99) < 45
        if (!nested) return scalarDef(e, name, nullable = e.bool())
        return when (e.int(0, 2)) {
            0 ->
                ColumnDef(
                    name,
                    ColType.LIST,
                    null,
                    e.bool(),
                    listOf(genDef(e, depthLeft - 1, "x").renamed(ColType.LIST_ELEMENT)),
                )
            1 -> {
                // Iceberg map keys are non-nullable. Keys are usually
                // scalar; occasionally a container, which is legal.
                val key =
                    if (e.int(0, 9) < 8) {
                        scalarDef(e, ColType.MAP_KEY, nullable = false)
                    } else {
                        genDef(e, depthLeft - 1, "x").renamed(ColType.MAP_KEY).copy(nullable = false)
                    }
                ColumnDef(
                    name,
                    ColType.MAP,
                    null,
                    e.bool(),
                    listOf(key, genDef(e, depthLeft - 1, "x").renamed(ColType.MAP_VALUE)),
                )
            }
            else ->
                ColumnDef(
                    name,
                    ColType.STRUCT,
                    null,
                    e.bool(),
                    (0 until e.int(1, 3)).map { genDef(e, depthLeft - 1, "f$it") },
                )
        }
    }

    private fun ColumnDef.renamed(n: String) = copy(name = n)

    private fun scalarDef(
        e: Entropy,
        name: String,
        nullable: Boolean,
    ): ColumnDef {
        val t = SCALARS[e.int(0, SCALARS.size - 1)]
        return ColumnDef(name, t, typeParams(e, t), nullable)
    }

    private fun typeParams(
        e: Entropy,
        t: ColType,
    ): Map<String, Any?>? =
        if (t == ColType.DECIMAL) {
            // Precision boundaries are deliberately over-sampled.
            val p = if (e.bool()) e.int(1, 38) else listOf(1, 2, 9, 10, 18, 19, 38)[e.int(0, 6)]
            mapOf("precision" to p, "scale" to e.int(0, p))
        } else {
            null
        }

    fun toCatalogColumns(cols: List<Column>): List<CatalogColumn> =
        cols.map { c ->
            CatalogColumn(
                fieldId = c.fieldId,
                name = c.def.name,
                type = c.def.type,
                decimalScale = (c.def.typeParams?.get("scale") as? Number)?.toInt(),
                children = toCatalogColumns(c.children),
            )
        }

    // ---- parquet schema side --------------------------------------------

    /** What the generator did to the derived schema, for oracle gating. */
    class Derived(
        val schema: MessageType,
        val mutations: Int,
    )

    /**
     * Derive a parquet MessageType from the catalog forest. [mutRate] is
     * the per-node mutation probability in percent; 0 produces the
     * canonical shape the rewriter itself would write.
     */
    fun deriveSchema(
        e: Entropy,
        cols: List<Column>,
        mutRate: Int,
    ): Derived {
        val used = ArrayList<Int>()
        var muts = 0

        fun hit(): Boolean =
            if (mutRate > 0 && e.int(0, 99) < mutRate) {
                muts++
                true
            } else {
                false
            }

        fun idOf(col: Column): Int? {
            if (!hit()) {
                used.add(Math.toIntExact(col.fieldId))
                return Math.toIntExact(col.fieldId)
            }
            return when (e.int(0, 2)) {
                0 -> null
                1 -> e.int(1, 60).also { used.add(it) }
                else -> if (used.isEmpty()) Math.toIntExact(col.fieldId) else used[e.int(0, used.size - 1)]
            }
        }

        fun repOf(default: Type.Repetition): Type.Repetition =
            if (!hit()) {
                default
            } else {
                listOf(
                    Type.Repetition.OPTIONAL,
                    Type.Repetition.REQUIRED,
                    Type.Repetition.REPEATED,
                )[e.int(0, 2)]
            }

        fun field(
            col: Column,
            defaultRep: Type.Repetition,
        ): Type? {
            if (hit() && e.bool()) return null // omit the field entirely
            val rep = repOf(defaultRep)
            val id = idOf(col)
            val name = if (hit()) "z${e.int(0, 9)}" else col.def.name
            return when (col.def.type) {
                ColType.STRUCT -> {
                    val kids =
                        col.children.mapNotNull { field(it, Type.Repetition.OPTIONAL) }.toMutableList()
                    if (kids.isEmpty()) return null
                    // A decoy INSIDE the struct, on the same terms as
                    // the top-level one. Without it the generator could
                    // not reach the name-versus-id shape below depth 1
                    // at all: struct children are named f0..f2 while the
                    // rename mutation emits z0..z9, so no id-less field
                    // ever wore a sibling's catalog name. Struct binding
                    // is correct today — this is the blind spot, not a
                    // live bug, and a blind spot one level down from a
                    // defect we already shipped is worth closing.
                    if (mutRate > 0 && e.int(0, 99) < 25) {
                        val victim = col.children[e.int(0, col.children.size - 1)]
                        val victimIndex =
                            kids.indexOfFirst { it.id?.intValue()?.toLong() == victim.fieldId }
                        if (victimIndex >= 0) {
                            kids[victimIndex] = renamed(kids[victimIndex], "zz${e.int(0, 99)}")
                            kids.add(
                                0,
                                Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                                    .named(victim.def.name),
                            )
                            muts++
                        }
                    }
                    val ordered = if (hit()) kids.reversed() else kids
                    var b = Types.buildGroup(rep).addFields(*ordered.toTypedArray())
                    if (hit()) {
                        b =
                            b.`as`(
                                listOf(
                                    LogicalTypeAnnotation.listType(),
                                    LogicalTypeAnnotation.mapType(),
                                    LogicalTypeAnnotation.enumType(),
                                    LogicalTypeAnnotation.jsonType(),
                                )[e.int(0, 3)],
                            )
                    }
                    if (id != null) b = b.id(id)
                    b.named(name)
                }
                ColType.LIST -> {
                    if (col.children.size != 1) return null
                    val element = field(col.children[0], Type.Repetition.OPTIONAL) ?: return null
                    val layerName = if (hit()) listOf("list", "bag", "entries", "array")[e.int(0, 3)] else "list"
                    val inner: Type =
                        if (hit()) {
                            // 2-level legacy list: the repeated node IS the element.
                            Types.buildGroup(Type.Repetition.REPEATED)
                                .addField(element)
                                .named(layerName)
                        } else if (hit()) {
                            // arity violation inside the repetition layer
                            Types.repeatedGroup()
                                .addFields(
                                    element,
                                    Types.optional(PrimitiveType.PrimitiveTypeName.INT32).named("extra"),
                                )
                                .named(layerName)
                        } else {
                            Types.repeatedGroup().addField(element).named(layerName)
                        }
                    var b = Types.buildGroup(rep).addField(inner)
                    b = if (hit()) b else b.`as`(LogicalTypeAnnotation.listType())
                    if (id != null) b = b.id(id)
                    b.named(name)
                }
                ColType.MAP -> {
                    if (col.children.size != 2) return null
                    val keyRep = if (hit()) Type.Repetition.OPTIONAL else Type.Repetition.REQUIRED
                    val key = field(col.children[0], keyRep) ?: return null
                    val value = field(col.children[1], Type.Repetition.OPTIONAL) ?: return null
                    val pair = if (hit()) listOf(value, key) else listOf(key, value)
                    val layerName = if (hit()) listOf("key_value", "map", "entries")[e.int(0, 2)] else "key_value"
                    val inner =
                        if (hit()) {
                            Types.repeatedGroup().addFields(*pair.take(1).toTypedArray()).named(layerName)
                        } else {
                            Types.repeatedGroup().addFields(*pair.toTypedArray()).named(layerName)
                        }
                    var b = Types.buildGroup(rep).addField(inner)
                    b =
                        if (hit()) {
                            b
                        } else {
                            b.`as`(
                                if (e.int(0, 9) < 9) {
                                    LogicalTypeAnnotation.mapType()
                                } else {
                                    LogicalTypeAnnotation.MapKeyValueTypeAnnotation.getInstance()
                                },
                            )
                        }
                    if (id != null) b = b.id(id)
                    b.named(name)
                }
                else ->
                    if (hit()) {
                        // parquet-java refuses illegal annotation/physical
                        // pairs at build time; that is the GENERATOR being
                        // wrong, not the subject, so fall back.
                        try {
                            randomPrimitive(e, rep, id, name)
                        } catch (_: RuntimeException) {
                            canonicalPrimitive(col, rep, id, name)
                        }
                    } else {
                        canonicalPrimitive(col, rep, id, name)
                    }
            }
        }

        val fields = cols.mapNotNull { field(it, Type.Repetition.OPTIONAL) }.toMutableList()

        // DECOYS: an id-less field wearing a real column's NAME, placed
        // BEFORE the field that carries that column's id.
        //
        // Generated deliberately because the mutation machinery above
        // reaches this shape only by a conjunction of two rare choices
        // (drop one column's id AND reuse that id on a later column),
        // and it is the shape that matters most: field ids are the
        // binding contract, so a name must never outrank one. A
        // first-match-wins scan made the decoy win, the reader bounded
        // it and compaction copied its values into the other column's
        // slot — and no agreement oracle could see it, because both
        // surfaces were wrong identically. Files that stamp ids on only
        // SOME columns are real: foreign writers produce them.
        if (mutRate > 0 && fields.isNotEmpty() && e.int(0, 99) < 25) {
            val victim = cols[e.int(0, cols.size - 1)]
            // The decoy wears the victim's name, so the victim's OWN
            // generated field must not — parquet refuses a schema with
            // two identically named siblings at write time, and when it
            // did, `writeFile` threw and the whole execution was
            // discarded before either surface ran. Measured: 79.4% of
            // decoy fires wasted, ~10% of all executions, and only 37%
            // of decoy shapes ever reached the subject.
            //
            // Renaming the victim's own field keeps the shape intact
            // (the decoy still wears the catalog NAME, the victim's
            // field still carries the catalog ID) while making the two
            // distinguishable to the writer.
            val victimIndex =
                fields.indexOfFirst { it.id?.intValue()?.toLong() == victim.fieldId }
            if (victimIndex >= 0) {
                fields[victimIndex] = renamed(fields[victimIndex], "zz${e.int(0, 99)}")
            }
            fields.add(
                0,
                Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named(victim.def.name),
            )
            muts++
        }

        if (fields.isEmpty()) {
            fields.add(Types.optional(PrimitiveType.PrimitiveTypeName.INT32).id(9999).named("filler"))
        }
        return Derived(MessageType("fuzz", fields), muts)
    }

    /** [field] under a different name, id and shape unchanged. */
    private fun renamed(
        field: Type,
        name: String,
    ): Type {
        val id = field.id?.intValue()
        return if (field.isPrimitive) {
            val p = field.asPrimitiveType()
            var b = Types.primitive(p.primitiveTypeName, p.repetition)
            if (p.typeLength > 0) b = b.length(p.typeLength)
            p.logicalTypeAnnotation?.let { b = b.`as`(it) }
            if (id != null) b = b.id(id)
            b.named(name)
        } else {
            val g = field.asGroupType()
            var b = Types.buildGroup(g.repetition).addFields(*g.fields.toTypedArray())
            g.logicalTypeAnnotation?.let { b = b.`as`(it) }
            if (id != null) b = b.id(id)
            b.named(name)
        }
    }

    private fun randomPrimitive(
        e: Entropy,
        rep: Type.Repetition,
        id: Int?,
        name: String,
    ): Type {
        val names = PrimitiveType.PrimitiveTypeName.entries.toList()
        val p = names[e.int(0, names.size - 1)]
        var b: Types.PrimitiveBuilder<PrimitiveType> = Types.primitive(p, rep)
        if (p == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
            b = b.length(if (e.bool()) 16 else e.int(1, 20))
        }
        if (e.bool()) {
            val ann =
                listOf(
                    LogicalTypeAnnotation.stringType(),
                    LogicalTypeAnnotation.jsonType(),
                    LogicalTypeAnnotation.enumType(),
                    LogicalTypeAnnotation.intType(8, true),
                    LogicalTypeAnnotation.intType(16, false),
                    LogicalTypeAnnotation.intType(32, false),
                    LogicalTypeAnnotation.intType(64, false),
                    LogicalTypeAnnotation.dateType(),
                    LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MICROS),
                    LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MILLIS),
                    LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS),
                    LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS),
                    LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS),
                    e.int(1, 38).let { prec -> LogicalTypeAnnotation.decimalType(e.int(0, minOf(prec, 5)), prec) },
                    LogicalTypeAnnotation.uuidType(),
                )[e.int(0, 14)]
            b = b.`as`(ann)
        }
        if (id != null) b = b.id(id)
        return b.named(name)
    }

    /** Mirrors ParquetRewriter.parquetTypeFor for the scalar arms. */
    @Suppress("CyclomaticComplexMethod")
    private fun canonicalPrimitive(
        col: Column,
        rep: Type.Repetition,
        id: Int?,
        name: String,
    ): Type {
        val t = col.def.type

        fun p(n: PrimitiveType.PrimitiveTypeName): Types.PrimitiveBuilder<PrimitiveType> = Types.primitive(n, rep)
        val i32 = PrimitiveType.PrimitiveTypeName.INT32
        val i64 = PrimitiveType.PrimitiveTypeName.INT64
        val bin = PrimitiveType.PrimitiveTypeName.BINARY
        var b: Types.PrimitiveBuilder<PrimitiveType> =
            when (t) {
                ColType.BOOLEAN -> p(PrimitiveType.PrimitiveTypeName.BOOLEAN)
                ColType.INT8 -> p(i32).`as`(LogicalTypeAnnotation.intType(8, true))
                ColType.INT16 -> p(i32).`as`(LogicalTypeAnnotation.intType(16, true))
                ColType.UINT8 -> p(i32).`as`(LogicalTypeAnnotation.intType(8, false))
                ColType.UINT16 -> p(i32).`as`(LogicalTypeAnnotation.intType(16, false))
                ColType.INT -> p(i32)
                ColType.UINT32 -> p(i64)
                ColType.UINT64 -> p(i64).`as`(LogicalTypeAnnotation.intType(64, false))
                ColType.LONG -> p(i64)
                ColType.FLOAT -> p(PrimitiveType.PrimitiveTypeName.FLOAT)
                ColType.DOUBLE -> p(PrimitiveType.PrimitiveTypeName.DOUBLE)
                ColType.DECIMAL ->
                    p(bin).`as`(
                        LogicalTypeAnnotation.decimalType(
                            (col.def.typeParams?.get("scale") as? Number)?.toInt() ?: 0,
                            (col.def.typeParams?.get("precision") as? Number)?.toInt() ?: 38,
                        ),
                    )
                ColType.DATE -> p(i32).`as`(LogicalTypeAnnotation.dateType())
                ColType.TIME ->
                    p(
                        i64,
                    ).`as`(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
                ColType.TIMESTAMP_S, ColType.TIMESTAMP_MS ->
                    p(i64).`as`(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
                ColType.TIMESTAMP ->
                    p(i64).`as`(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
                ColType.TIMESTAMP_NS ->
                    p(i64).`as`(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.NANOS))
                ColType.TIMESTAMPTZ ->
                    p(i64).`as`(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
                ColType.STRING -> p(bin).`as`(LogicalTypeAnnotation.stringType())
                ColType.JSON -> p(bin).`as`(LogicalTypeAnnotation.jsonType())
                ColType.UUID_T ->
                    Types.primitive(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, rep)
                        .length(16).`as`(LogicalTypeAnnotation.uuidType())
                ColType.BINARY -> p(bin)
                else -> p(i32)
            }
        if (id != null) b = b.id(id)
        return b.named(name)
    }

    // ---- data ------------------------------------------------------------

    /**
     * When true, generated values stay inside the domain their logical
     * annotation declares (an INT(8, unsigned) leaf gets 0..255, a
     * DECIMAL(p,s) leaf an unscaled value of at most p digits). That
     * separates "hoglake mishandles a well-formed file" from "hoglake
     * mishandles a file whose writer lied about its own annotation".
     */
    @JvmStatic
    var strictDomains: Boolean = true

    /**
     * A generated ROW's node ceiling.
     *
     * The generator repeats up to `maxRep` times at EVERY repeated level,
     * so a five-deep schema with maxRep=400 asks for 400^5 nodes and the
     * harness OOMs building its own input — 28 wasted iterations in a
     * measured 51k-execution campaign, all of them reported as findings
     * against code that never ran. This caps the generator, not the
     * product: hoglake's own per-row bound is
     * ParquetRewriter.DEFAULT_MAX_NODES_PER_ROW, and it is deliberately
     * far above this so the fuzzer reaches it on purpose (via an
     * explicitly small budget) rather than by accident.
     */
    private const val MAX_GENERATED_NODES_PER_ROW = 200_000

    /** Write [rows] generated records under [schema] to [path]; returns the record count. */
    fun writeFile(
        e: Entropy,
        schema: MessageType,
        path: Path,
        rows: Int,
        maxRep: Int,
    ): Int = writeFile(e, schema, LocalOutputFile(path), rows, maxRep)

    /**
     * The same write onto any parquet [OutputFile].
     *
     * The campaigns use a memory sink (`MemoryOutputFile`): the subject
     * of the nested fuzzers is the reader/rewriter pair, never the
     * filesystem, and a temp file per iteration was costing more than
     * every oracle in the target put together.
     */
    fun writeFile(
        e: Entropy,
        schema: MessageType,
        out: OutputFile,
        rows: Int,
        maxRep: Int,
    ): Int {
        val factory = SimpleGroupFactory(schema)
        ExampleParquetWriter.builder(out)
            .withConf(SHARED_CONF)
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .build()
            .use { w ->
                repeat(rows) {
                    val g = factory.newGroup()
                    fill(e, g, schema, maxRep, Budget())
                    w.write(g)
                }
            }
        return rows
    }

    /** One generated row's remaining node allowance. */
    private class Budget {
        var left: Int = MAX_GENERATED_NODES_PER_ROW

        fun take(): Boolean {
            if (left <= 0) return false
            left--
            return true
        }
    }

    private fun fill(
        e: Entropy,
        g: Group,
        type: GroupType,
        maxRep: Int,
        budget: Budget,
    ) {
        type.fields.forEachIndexed { i, f ->
            val n =
                when {
                    f.isRepetition(Type.Repetition.REQUIRED) -> 1
                    f.isRepetition(Type.Repetition.REPEATED) -> e.int(0, maxRep)
                    else -> if (e.int(0, 9) < 8) 1 else 0
                }
            repeat(n) {
                // A REQUIRED field still has to be written even past the
                // budget: skipping one produces a file parquet refuses,
                // which would be a harness failure of a different colour.
                if (budget.take() || f.isRepetition(Type.Repetition.REQUIRED)) {
                    if (f.isPrimitive) {
                        addValue(e, g, i, f.asPrimitiveType())
                    } else {
                        fill(e, g.addGroup(i), f.asGroupType(), maxRep, budget)
                    }
                }
            }
        }
    }

    private fun addValue(
        e: Entropy,
        g: Group,
        i: Int,
        p: PrimitiveType,
    ) {
        when (p.primitiveTypeName) {
            PrimitiveType.PrimitiveTypeName.BOOLEAN -> g.add(i, e.bool())
            PrimitiveType.PrimitiveTypeName.INT32 -> g.add(i, clampInt(intValue(e), p))
            PrimitiveType.PrimitiveTypeName.INT64 -> g.add(i, clampLong(longValue(e), p))
            PrimitiveType.PrimitiveTypeName.INT96 -> g.add(i, Binary.fromConstantByteArray(e.bytes(12)))
            PrimitiveType.PrimitiveTypeName.FLOAT -> g.add(i, floatValue(e))
            PrimitiveType.PrimitiveTypeName.DOUBLE -> g.add(i, doubleValue(e))
            PrimitiveType.PrimitiveTypeName.BINARY -> g.add(i, Binary.fromConstantByteArray(binaryValue(e, p)))
            PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY ->
                g.add(i, Binary.fromConstantByteArray(e.bytes(p.typeLength)))
            null -> Unit
        }
    }

    /** Fold an int32 into the domain its INT(w, signed?) annotation declares. */
    private fun clampInt(
        v: Int,
        p: PrimitiveType,
    ): Int {
        if (!strictDomains) return v
        val a = p.logicalTypeAnnotation as? LogicalTypeAnnotation.IntLogicalTypeAnnotation ?: return v
        if (a.bitWidth >= 32 && a.isSigned) return v
        return if (a.isSigned) {
            val half = 1 shl (a.bitWidth - 1)
            ((v % half) + half) % (2 * half) - half
        } else {
            val span = if (a.bitWidth >= 32) return v and Int.MAX_VALUE else 1 shl a.bitWidth
            ((v % span) + span) % span
        }
    }

    private fun clampLong(
        v: Long,
        p: PrimitiveType,
    ): Long {
        if (!strictDomains) return v
        val a = p.logicalTypeAnnotation as? LogicalTypeAnnotation.IntLogicalTypeAnnotation ?: return v
        if (a.bitWidth >= 64) return v
        val span = 1L shl a.bitWidth
        return if (a.isSigned) ((v % (span / 2)) + span) % span - span / 2 else ((v % span) + span) % span
    }

    private fun intValue(e: Entropy): Int =
        when (e.int(0, 9)) {
            0 -> Int.MAX_VALUE
            1 -> Int.MIN_VALUE
            2 -> 0
            3 -> -1
            else -> e.rawInt()
        }

    private fun longValue(e: Entropy): Long =
        when (e.int(0, 9)) {
            0 -> Long.MAX_VALUE
            1 -> Long.MIN_VALUE
            2 -> 0L
            3 -> -1L
            4 -> Long.MAX_VALUE - 1
            else -> e.rawLong()
        }

    private fun floatValue(e: Entropy): Float =
        when (e.int(0, 9)) {
            0 -> Float.NaN
            1 -> Float.POSITIVE_INFINITY
            2 -> Float.NEGATIVE_INFINITY
            3 -> -0.0f
            else -> Float.fromBits(e.rawInt())
        }

    private fun doubleValue(e: Entropy): Double =
        when (e.int(0, 9)) {
            0 -> Double.NaN
            1 -> Double.POSITIVE_INFINITY
            2 -> Double.NEGATIVE_INFINITY
            3 -> -0.0
            else -> Double.fromBits(e.rawLong())
        }

    /**
     * BINARY payload. For a decimal-annotated leaf the bytes are a
     * two's-complement unscaled value, sampled hard at the precision
     * boundary; EMPTY is deliberately in the domain (parquet permits a
     * zero-length byte array, and both surfaces have to survive it).
     */
    private fun binaryValue(
        e: Entropy,
        p: PrimitiveType,
    ): ByteArray {
        val dec = p.logicalTypeAnnotation as? LogicalTypeAnnotation.DecimalLogicalTypeAnnotation
        if (dec != null) {
            val max = BigInteger.TEN.pow(dec.precision).subtract(BigInteger.ONE)
            return when (e.int(0, 9)) {
                // EMPTY stays in the domain in both modes: parquet permits
                // a zero-length byte array, and the surfaces must survive it.
                0 -> ByteArray(0)
                1 -> max.toByteArray()
                2 -> if (strictDomains) max.negate().toByteArray() else BigInteger.TEN.pow(dec.precision).toByteArray()
                3 ->
                    if (strictDomains) {
                        BigInteger.ONE.toByteArray()
                    } else {
                        BigInteger.TEN.pow(dec.precision).negate().toByteArray()
                    }
                4 -> BigInteger.ZERO.toByteArray()
                else -> {
                    val raw = BigInteger(e.bytes(e.int(1, 17)))
                    if (strictDomains) raw.mod(max.add(BigInteger.ONE)).toByteArray() else raw.toByteArray()
                }
            }
        }
        val ann = p.logicalTypeAnnotation
        val stringy =
            ann is LogicalTypeAnnotation.StringLogicalTypeAnnotation ||
                ann is LogicalTypeAnnotation.JsonLogicalTypeAnnotation ||
                ann is LogicalTypeAnnotation.EnumLogicalTypeAnnotation
        if (strictDomains && stringy) {
            // A STRING/JSON annotation declares UTF-8; stay inside it so a
            // non-UTF-8 bound is reported only when it is deliberately sought.
            return listOf("", "a", "zz", "é", "𝔘𝔫𝔦", " ", "~~~~", "{}", "[1]", "ß")[e.int(0, 9)]
                .toByteArray(Charsets.UTF_8)
        }
        return e.bytes(e.int(0, 8))
    }

    // ---- footer readback -------------------------------------------------

    class LeafStat(
        val path: List<String>,
        val fieldId: Int?,
        val valueCount: Long,
        val nullCount: Long?,
    )

    fun leafStats(path: Path): Pair<MessageType, List<LeafStat>> = leafStats(LocalInputFile(path))

    fun leafStats(input: InputFile): Pair<MessageType, List<LeafStat>> =
        ParquetFileReader.open(input, SHARED_READ_OPTIONS).use { r ->
            val footer = r.footer
            val schema = footer.fileMetaData.schema
            val ids = HashMap<List<String>, Int?>()
            collectLeafIds(schema.fields, emptyList(), ids)
            val agg = LinkedHashMap<List<String>, LongArray>()
            val nullKnown = HashMap<List<String>, Boolean>()
            for (block in footer.blocks) {
                for (chunk in block.columns) {
                    val p = chunk.path.toArray().toList()
                    val a = agg.getOrPut(p) { longArrayOf(0, 0) }
                    a[0] += chunk.valueCount
                    val st = chunk.statistics
                    if (st == null || !st.isNumNullsSet) {
                        nullKnown[p] = false
                    } else {
                        a[1] += st.numNulls
                        nullKnown.putIfAbsent(p, true)
                    }
                }
            }
            schema to
                agg.map { (p, a) ->
                    LeafStat(p, ids[p], a[0], if (nullKnown[p] == false) null else a[1])
                }
        }

    private fun collectLeafIds(
        fields: List<Type>,
        prefix: List<String>,
        out: MutableMap<List<String>, Int?>,
    ) {
        for (f in fields) {
            val p = prefix + f.name
            if (f.isPrimitive) out[p] = f.id?.intValue() else collectLeafIds(f.asGroupType().fields, p, out)
        }
    }

    /** Field ids that appear more than once anywhere in the schema. */
    fun duplicateIds(schema: MessageType): Set<Int> {
        val seen = HashSet<Int>()
        val dup = HashSet<Int>()

        fun walk(fs: List<Type>) {
            for (f in fs) {
                f.id?.intValue()?.let { if (!seen.add(it)) dup.add(it) }
                if (!f.isPrimitive) walk(f.asGroupType().fields)
            }
        }
        walk(schema.fields)
        return dup
    }

    fun maxDepth(): Int = MAX_COLUMN_NESTING_DEPTH

    /**
     * One Hadoop [Configuration] and one [ParquetReadOptions] for the
     * whole JVM, handed to every writer and reader this generator
     * builds.
     *
     * Not a micro-optimisation. parquet-java's no-argument
     * `ParquetWriter.Builder` and `ParquetFileReader.open(InputFile)`
     * each construct a fresh `Configuration`, and a fresh
     * `Configuration` re-parses `core-default.xml` out of the
     * hadoop-common jar on first use: 0.74 ms, measured, every time.
     * A single nested iteration opened enough of them to spend most of
     * its budget parsing the same XML over and over — reading one
     * generated footer went from 0.83 ms to 0.012 ms once the options
     * were prebuilt. A Hadoop `Configuration` is mutable, so "shared" is
     * a claim that has to hold: nothing in this harness and nothing in
     * parquet-hadoop's writer path sets a value on it, so it is never
     * mutated after construction and sharing it changes nothing the
     * campaigns can observe. `ParquetReadOptions` is immutable.
     *
     * The same charge is still paid inside `FooterParse.parse` and
     * `ParquetRewriter.rewrite`, which build their own; that is
     * production's to decide, not the harness's, and it is what remains
     * between this target and a faster one.
     */
    private val SHARED_CONF = Configuration()

    private val SHARED_READ_OPTIONS: ParquetReadOptions = ParquetReadOptions.builder().build()
}
