package com.posthog.hoglake.api

import com.fasterxml.jackson.core.JsonProcessingException
import com.posthog.hoglake.wireObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class OperationMetadataDtoTest {
    @Test
    fun `every attribution DTO rejects nontext values without coercion`() {
        val mapper = wireObjectMapper()
        val shapes =
            mapOf(
                CreateCatalogRequestDto::class.java to """{"name":"c","data_path":"s3://b"}""",
                CreateNamespaceRequestDto::class.java to """{"name":"ns"}""",
                CreateTableRequestDto::class.java to """{"name":"t","columns":[]}""",
                CreateViewRequestDto::class.java to """{"name":"v","sql":"SELECT 1"}""",
                AlterTableRequestDto::class.java to """{"ops":[]}""",
                PrepareTableCreationDto::class.java to """{"namespace":"ns","name":"t","columns":[]}""",
                OperationMetadataDto::class.java to "{}",
            )
        for ((type, shape) in shapes) {
            for (field in listOf("author", "message")) {
                for (invalid in listOf("42", "true", "[]", "{}")) {
                    val request = shape.dropLast(1) + (if (shape == "{}") "" else ",") + "\"$field\":$invalid}"
                    assertThatThrownBy { mapper.readValue(request, type) }
                        .describedAs("%s %s=%s", type.simpleName, field, invalid)
                        .isInstanceOf(JsonProcessingException::class.java)
                }
                for (valid in listOf("null", "\"  λ\\n  \"")) {
                    val request = shape.dropLast(1) + (if (shape == "{}") "" else ",") + "\"$field\":$valid}"
                    assertThat(mapper.readValue(request, type)).isNotNull()
                }
            }
        }
    }
}
