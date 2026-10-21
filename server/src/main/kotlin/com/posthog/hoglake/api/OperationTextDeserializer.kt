package com.posthog.hoglake.api

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonMappingException

/** Attribution must be text; numeric and boolean values are never coerced into identities. */
class OperationTextDeserializer : JsonDeserializer<String>() {
    override fun deserialize(
        parser: JsonParser,
        context: DeserializationContext,
    ): String {
        if (parser.currentToken() != JsonToken.VALUE_STRING) {
            throw JsonMappingException.from(parser, "'${parser.currentName()}' must be a string or null")
        }
        return parser.text
    }
}
