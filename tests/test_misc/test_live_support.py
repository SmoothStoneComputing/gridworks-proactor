import pytest
from gwproto import HardwareLayout
from gwproto.enums import ActorClass

from gwproactor_test.clean import TEST_HARDWARE_LAYOUT_PATH
from gwproactor_test.tree_live_test_helper import TreeLiveTest


@pytest.mark.asyncio
async def test_hardware_layout_defaults(request: pytest.FixtureRequest) -> None:
    async with TreeLiveTest(add_all=True, request=request) as h:
        for app in [h.parent, h.child1, h.child2]:
            assert app.hardware_layout.node("s").actor_class == ActorClass.Scada


@pytest.mark.asyncio
async def test_hardware_layout_parameter(request: pytest.FixtureRequest) -> None:
    layout1 = HardwareLayout.load(TEST_HARDWARE_LAYOUT_PATH)
    layout2 = HardwareLayout.load(TEST_HARDWARE_LAYOUT_PATH)
    layout3 = HardwareLayout.load(TEST_HARDWARE_LAYOUT_PATH)

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
