package com.posthog.hoglake.service

import com.posthog.hoglake.Config
import com.posthog.hoglake.model.HoglakeException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class ReservedNamespacesTest {
    private val posthog = ReservedNamespaces(listOf("posthog"))

    @Test
    fun `a reserved name matches in any ASCII case and nothing else`() {
        for (name in listOf("posthog", "PostHog", "POSTHOG", "pOSTHOg")) {
            assertThat(posthog.isReserved(name)).describedAs(name).isTrue()
        }
        for (name in listOf(
            "posthog_", "_posthog", "posthog-events", "posthog2", "my_posthog", "posthogx", "posthoG_raw",
            "posthog ", " posthog", "post_hog", "posthoh", "", "p",
        )) {
            assertThat(posthog.isReserved(name)).describedAs(name).isFalse()
        }
    }

    @Test
    fun `only ASCII letters fold`() {
        // Kotlin's lowercase() maps the Kelvin sign to 'k'; equals(ignoreCase) also maps the
        // long s to 's'. Neither is an ASCII case variant, and both fail the identifier pattern.
        val reserved = ReservedNamespaces(listOf("kafka", "s"))
        assertThat(reserved.isReserved("Kafka")).isFalse()
        assertThat(reserved.isReserved("ſ")).isFalse()
        assertThat(reserved.isReserved("KAFKA")).isTrue()
        assertThat(reserved.isReserved("S")).isTrue()
    }

    @Test
    fun `configured names are folded and the refusal is a 422 validation`() {
        val reserved = ReservedNamespaces(listOf("PostHog", "Other_One"))
        assertThat(reserved.names).containsExactly("other_one", "posthog")
        assertThat(reserved.isReserved("posthog")).isTrue()
        assertThatThrownBy { reserved.refuse("POSTHOG") }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessage("namespace name 'POSTHOG' is reserved on this server")
        assertThatCode { reserved.refuse("posthog_raw") }.doesNotThrowAnyException()
    }

    @Test
    fun `none reserves nothing`() {
        for (name in listOf("posthog", "PostHog", "team_1", "_")) {
            assertThat(ReservedNamespaces.NONE.isReserved(name)).isFalse()
            assertThatCode { ReservedNamespaces.NONE.refuse(name) }.doesNotThrowAnyException()
        }
    }

    @Test
    fun `the setting is a comma-separated list, empty by default, checked at boot`() {
        assertThat(Config().reservedNamespaceNames).isEmpty()
        assertThat(Config().reservedNamespaces.names).isEmpty()
        val parsed = Config.nameList(" posthog , Other ,,")
        assertThat(parsed).containsExactly("posthog", "Other")
        assertThat(Config(reservedNamespaceNames = parsed).reservedNamespaces.names).containsExactly("other", "posthog")
        for (bad in listOf("post hog", "9posthog", "post/hog", "a".repeat(129), "posthög")) {
            assertThatThrownBy { Config(reservedNamespaceNames = setOf(bad)) }
                .describedAs(bad)
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("HOGLAKE_RESERVED_NAMESPACES")
        }
    }
}
