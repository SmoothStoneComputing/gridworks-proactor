# """Test communication issues"""

from gwproactor_test import CommTestHelper, ProactorCommTests
from gwproactor_test.collections import ProactorTreeCommTests
from gwproactor_test.dummies import (
    DummyChild,
    DummyChildSettings,
    DummyParent,
    DummyParentSettings,
)
from gwproactor_test.dummies.tree.atn import DummyAtn
from gwproactor_test.dummies.tree.atn_settings import DummyAtnSettings
from gwproactor_test.dummies.tree.scada1 import DummyScada1
from gwproactor_test.dummies.tree.scada1_settings import DummyScada1Settings
from gwproactor_test.dummies.tree.scada2 import DummyScada2
from gwproactor_test.dummies.tree.scada2_settings import DummyScada2Settings
from gwproactor_test.tree_comm_test_helper import TreeCommTestHelper


class DummyCommTestHelper(
    CommTestHelper[DummyParent, DummyChild, DummyParentSettings, DummyChildSettings]
):
    parent_t = DummyParent
    child_t = DummyChild
    parent_settings_t = DummyParentSettings
    child_settings_t = DummyChildSettings


class TestDummyProactorComm(
    ProactorCommTests[DummyParent, DummyChild, DummyParentSettings, DummyChildSettings]
):
    CTH = DummyCommTestHelper


class DummyTreeCommTestHelper(
    TreeCommTestHelper[DummyAtn, DummyScada1, DummyAtnSettings, DummyScada1Settings]
):
    parent_t = DummyAtn
    child_t = DummyScada1
    child2_t = DummyScada2
    parent_settings_t = DummyAtnSettings
    child_settings_t = DummyScada1Settings
    child2_settings_t = DummyScada2Settings


class TestDummyTreeComm(ProactorTreeCommTests):
    CTH = DummyTreeCommTestHelper
