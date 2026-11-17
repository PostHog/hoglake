package com.posthog.hoglake.service

import com.posthog.hoglake.Config
import com.posthog.hoglake.model.HoglakeException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class OperatorTeamNamespacesTest {
    private val listed = OperatorTeamNamespaces(listOf("imports-dev"))

    @Test
    fun `a team name is matched on its shape, in any case and with any leading zeros`() {
        for (name in listOf("team_5", "TEAM_5", "Team_05", "team_007", "tEaM_123456789012345678901234567890")) {
            assertThat(listed.isOperatorOnly("imports-dev", name)).describedAs(name).isTrue()
        }
        val others = listOf("team_", "team", "team5", "team_5a", "team_a", "myteam_5", "team_5_x", "team-5", "teams_5")
        for (name in others) {
            assertThat(listed.isOperatorOnly("imports-dev", name)).describedAs(name).isFalse()
        }
    }

    @Test
    fun `only a listed catalog is held to it`() {
        assertThat(listed.isOperatorOnly("imports-dev", "team_5")).isTrue()
        assertThat(listed.isOperatorOnly("imports", "team_5")).isFalse()
        assertThat(listed.isOperatorOnly("Imports-dev", "team_5")).isFalse()
        assertThat(OperatorTeamNamespaces.NONE.isOperatorOnly("imports-dev", "team_5")).isFalse()
    }

    @Test
    fun `a non-operator is refused with forbidden and an operator is not`() {
        assertThatThrownBy { listed.refuse("imports-dev", "TEAM_5", operator = false) }
            .isInstanceOf(HoglakeException.Forbidden::class.java)
        listed.refuse("imports-dev", "TEAM_5", operator = true)
        listed.refuse("imports-dev", "main", operator = false)
        listed.refuse("other", "team_5", operator = false)
    }

    @Test
    fun `the setting is a trimmed comma list of catalog names, empty by default`() {
        assertThat(Config.nameList(" imports-dev , lake,,")).containsExactlyInAnyOrder("imports-dev", "lake")
        assertThat(Config().operatorTeamNamespaces.catalogs).isEmpty()
        val cfg = Config(operatorTeamNamespaceCatalogs = Config.nameList("lake, imports-dev"))
        assertThat(cfg.operatorTeamNamespaces.catalogs).containsExactly("imports-dev", "lake")
        assertThat(cfg.operatorTeamNamespaces.isOperatorOnly("lake", "team_9")).isTrue()
    }

    @Test
    fun `a name that is not a catalog name is refused at boot`() {
        for (bad in listOf("Imports", "imports dev", "team_5/x", "-lake")) {
            assertThatThrownBy { Config(operatorTeamNamespaceCatalogs = setOf(bad)) }
                .describedAs(bad)
                .hasMessageContaining("HOGLAKE_OPERATOR_TEAM_NAMESPACE_CATALOGS")
        }
    }
}
