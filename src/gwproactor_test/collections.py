# ruff: noqa: PLR2004, ERA001
import typing
from typing import Type

import pytest

from gwproactor_test.awaiting_setup_tests import ProactorCommAwaitingSetupTests
from gwproactor_test.basic_tests import ProactorCommBasicTests
from gwproactor_test.comm_test_helper import (
    ChildSettingsT,
    ChildT,
    CommTestHelper,
    ParentSettingsT,
    ParentT,
)
from gwproactor_test.dummies.tree.atn import DummyAtn
from gwproactor_test.dummies.tree.atn_settings import DummyAtnSettings
from gwproactor_test.dummies.tree.scada1 import DummyScada1
from gwproactor_test.dummies.tree.scada1_settings import DummyScada1Settings
from gwproactor_test.reupload_tests import ProactorReuploadTests
from gwproactor_test.timeout_tests import ProactorCommTimeoutTests
from gwproactor_test.tree_comm_test_helper import TreeCommTestHelper
from gwproactor_test.tree_tests import ProactorTreeCommBasicTests


@pytest.mark.asyncio
class ProactorCommTests(
    typing.Generic[ParentT, ChildT, ParentSettingsT, ChildSettingsT],
    ProactorCommBasicTests[ParentT, ChildT, ParentSettingsT, ChildSettingsT],
    ProactorCommAwaitingSetupTests[ParentT, ChildT, ParentSettingsT, ChildSettingsT],
    ProactorCommTimeoutTests[ParentT, ChildT, ParentSettingsT, ChildSettingsT],
    ProactorReuploadTests[ParentT, ChildT, ParentSettingsT, ChildSettingsT],
):
    CTH: Type[CommTestHelper[ParentT, ChildT, ParentSettingsT, ChildSettingsT]]


class ProactorTreeCommTests(
    ProactorTreeCommBasicTests[
        DummyAtn, DummyScada1, DummyAtnSettings, DummyScada1Settings
    ]
):
    CTH: Type[
        TreeCommTestHelper[DummyAtn, DummyScada1, DummyAtnSettings, DummyScada1Settings]
    ]
