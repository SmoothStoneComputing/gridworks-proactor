# ruff: noqa: C408
from pathlib import Path

import pytest
from gwproto import HardwareLayout
from gwproto.enums import ActorClass

from gwproactor import ProactorSettings
from gwproactor.config import Paths
from gwproactor_test import DefaultTestEnv, LiveTest, set_hardware_layout_test_path
from gwproactor_test.clean import DUMMY_HARDWARE_LAYOUT_PATH, hardware_layout_test_path
from gwproactor_test.dummies.pair.child import DummyChildSettings
from gwproactor_test.tree_live_test_helper import TreeLiveTest
from tests.test_misc.test_clean import (
    DEFAULT_TEST_SCADA_DISPLAY_NAME,
    DUMMY_SCADA_DISPLAY_NAME,
)


@pytest.mark.asyncio
async def test_live_test_accesors(request: pytest.FixtureRequest) -> None:
    async with LiveTest(start_all=True, request=request) as h:
        assert h.child_to_parent_link == h.child.links.link(h.child.upstream_client)
        assert h.child_to_parent_stats == h.child.stats.link(h.child.upstream_client)
        assert h.parent_to_child_link == h.parent.links.link(h.parent.downstream_client)
        assert h.parent_to_child_stats == h.parent.stats.link(
            h.parent.downstream_client
        )

    async with TreeLiveTest(start_all=True, request=request) as h:
        assert h.child1_to_parent_link == h.child1.links.link(h.child1.upstream_client)
        assert h.child1_to_parent_stats == h.child1.stats.link(h.child1.upstream_client)
        assert h.child1_to_child2_link == h.child1.links.link(
            h.child1.downstream_client
        )
        assert h.child1_to_child2_stats == h.child1.stats.link(
            h.child1.downstream_client
        )
        assert h.child2_to_child1_link == h.child2.links.link(h.child2.upstream_client)
        assert h.child2_to_child1_stats == h.child2.stats.link(h.child2.upstream_client)
        assert h.parent_to_child1_link == h.parent.links.link(
            h.parent.downstream_client
        )
        assert h.parent_to_child1_stats == h.parent.stats.link(
            h.parent.downstream_client
        )


@pytest.mark.asyncio
async def test_hardware_layout_defaults(request: pytest.FixtureRequest) -> None:
    async with TreeLiveTest(add_all=True, request=request) as h:
        for app in [h.parent, h.child1, h.child2]:
            assert app.hardware_layout.node("s").actor_class == ActorClass.Scada


@pytest.mark.asyncio
async def test_hardware_layout_parameter(request: pytest.FixtureRequest) -> None:
    layout1 = HardwareLayout.load(hardware_layout_test_path())
    layout2 = HardwareLayout.load(hardware_layout_test_path())
    layout3 = HardwareLayout.load(hardware_layout_test_path())

    display_text1 = "Heat all the things"
    display_text2 = "efficiently"
    display_text3 = "and cheaply"

    default_text = layout1.node("s").display_name
    layout1.node("s").DisplayName = display_text1
    layout2.node("s").DisplayName = display_text2
    layout3.node("s").DisplayName = display_text3

    # set all the layouts
    async with TreeLiveTest(
        layout=layout1,
        add_all=True,
        request=request,
    ) as h:
        for app in [h.parent, h.child1, h.child2]:
            scada_node = app.hardware_layout.node("s")
            assert scada_node.display_name == display_text1

    # set one layout
    async with TreeLiveTest(
        child1_layout=layout1,
        add_all=True,
        request=request,
    ) as h:
        assert h.child1.hardware_layout.node("s").display_name == display_text1
        assert h.child2.hardware_layout.node("s").display_name == default_text
        assert h.parent.hardware_layout.node("s").display_name == default_text

    # set layouts individually
    async with TreeLiveTest(
        child1_layout=layout1,
        child2_layout=layout2,
        parent_layout=layout3,
        start_all=True,
        request=request,
    ) as h:
        assert h.child1.hardware_layout.node("s").display_name == display_text1
        assert h.child2.hardware_layout.node("s").display_name == display_text2
        assert h.parent.hardware_layout.node("s").display_name == display_text3

    # set all layouts and individual
    async with TreeLiveTest(
        child1_layout=layout1,
        layout=layout2,
        start_all=True,
        request=request,
    ) as h:
        assert h.child1.hardware_layout.node("s").display_name == display_text1
        assert h.child2.hardware_layout.node("s").display_name == display_text2
        assert h.parent.hardware_layout.node("s").display_name == display_text2


@pytest.mark.asyncio
async def test_setting_hardware_layout_test_path(
    request: pytest.FixtureRequest, tmp_path: Path
) -> None:
    original_layout_path = hardware_layout_test_path()
    try:
        # Verify you get the new layout if you change the test layout path
        set_hardware_layout_test_path(DUMMY_HARDWARE_LAYOUT_PATH)
        with DefaultTestEnv(tmp_path / "1").context():
            async with LiveTest(add_child=True, request=request) as h_new:
                copied_layout_path = h_new.child_app.settings.paths.hardware_layout
                assert (
                    h_new.child.hardware_layout.node("s").display_name
                    == DUMMY_SCADA_DISPLAY_NAME
                )

        # Verify that even after changing the test layout path back we don't
        # overwrite an existing layout (which allows test writers to explicitly
        # modify the contents of a layout file and have their change take
        # effect).
        set_hardware_layout_test_path(original_layout_path)
        with DefaultTestEnv(tmp_path / "1").context():
            async with LiveTest(add_child=True, request=request) as h_orig:
                assert (
                    h_orig.child.hardware_layout.node("s").display_name
                    == DUMMY_SCADA_DISPLAY_NAME
                )

        # Verify with no existing layout file we get the layout from the restored
        # test layout path.
        copied_layout_path.unlink()
        with DefaultTestEnv(tmp_path / "1").context():
            async with LiveTest(add_child=True, request=request) as h_orig:
                assert (
                    h_orig.child.hardware_layout.node("s").display_name
                    == DEFAULT_TEST_SCADA_DISPLAY_NAME
                )

        # Verify a new home gets the original value
        with DefaultTestEnv(tmp_path / "2").context():
            async with LiveTest(add_child=True, request=request) as h_orig:
                assert (
                    h_orig.child.hardware_layout.node("s").display_name
                    == DEFAULT_TEST_SCADA_DISPLAY_NAME
                )

    finally:
        set_hardware_layout_test_path(original_layout_path)


@pytest.mark.asyncio
async def test_setting_child_name() -> None:
    for kwargs, exp in [
        (dict(), "child"),
        (dict(child_app_settings=DummyChildSettings()), "child"),
        (
            dict(
                child_app_settings=DummyChildSettings(
                    proactor=ProactorSettings(num_initial_event_reuploads=5)
                )
            ),
            "child",
        ),
        (
            dict(
                child_app_settings=DummyChildSettings(
                    proactor=ProactorSettings(num_initial_event_reuploads=5),
                    paths=Paths(),
                )
            ),
            "child",
        ),
        (
            dict(
                child_app_settings=DummyChildSettings(
                    proactor=ProactorSettings(num_initial_event_reuploads=5),
                    paths=Paths(base="bar"),
                )
            ),
            "child",
        ),
        (
            dict(
                child_app_settings=DummyChildSettings(
                    proactor=ProactorSettings(num_initial_event_reuploads=5),
                    paths=Paths(name="foo"),
                )
            ),
            "foo",
        ),
    ]:
        async with LiveTest(**kwargs) as h:
            assert str(h.child_app.settings.paths.name) == exp
