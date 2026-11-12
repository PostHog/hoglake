package com.posthog.hoglake.auth

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import java.nio.file.Files
import java.nio.file.Path

/**
 * Regenerates `oauth/contract-vectors.json` from [ContractVectors].
 *
 * Skipped unless `-Dhoglake.updateContractVectors=true`, which
 * `just server contract-vectors` passes (through Gradle's
 * `-PupdateContractVectors`). Every other run leaves the file alone and
 * [ContractVectorsTest] checks it instead.
 */
class ContractVectorsGenerator {
    @Test
    @EnabledIfSystemProperty(named = "hoglake.updateContractVectors", matches = "true")
    fun `write the contract vectors`() {
        val path = Path.of(ContractVectors.PATH)
        check(Files.isDirectory(path.parent)) {
            "run from the server project directory: ${path.toAbsolutePath().parent} does not exist"
        }
        Files.writeString(path, ContractVectors.render(ContractVectors.document()))
    }
}
