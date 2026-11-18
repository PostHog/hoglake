package com.posthog.hoglake.service

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class TeamNamespacesTest {
    @Test
    fun `the canonical id is team_ and the decimal digits of a Long, lowercase, with no leading zero`() {
        assertThat(TeamNamespaces.id("team_42")).isEqualTo(42L)
        assertThat(TeamNamespaces.id("team_0")).isEqualTo(0L)
        assertThat(TeamNamespaces.id("team_9223372036854775807")).isEqualTo(Long.MAX_VALUE)
        assertThat(TeamNamespaces.name(42)).isEqualTo("team_42")
        for (name in listOf(
            "team_007",
            "team_00",
            "TEAM_7",
            "Team_7",
            "team_",
            "team_-1",
            "team_+1",
            "team_1_",
            "team_1 ",
            "team_1\n",
            " team_1",
            "team_x",
            "teams",
            "team7",
            "xteam_1",
            "team_9223372036854775808",
            "team_99999999999999999999",
            "team_١٢",
            "",
        )) {
            assertThat(TeamNamespaces.id(name)).describedAs(name).isNull()
        }
    }

    @Test
    fun `the shape is wider and takes any case, any leading zeros and no overflow check`() {
        for (name in listOf("team_1", "TEAM_1", "Team_007", "team_0", "team_99999999999999999999")) {
            assertThat(TeamNamespaces.hasShape(name)).describedAs(name).isTrue()
        }
        for (name in listOf("team_", "team_x", "teams", "team_1x", "team-1", "team_-1", "team_1\n", "team_١٢", "")) {
            assertThat(TeamNamespaces.hasShape(name)).describedAs(name).isFalse()
        }
    }
}
