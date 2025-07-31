import pytest


def add_live_test_options(parser: pytest.Parser, *, include_tree: bool = False) -> None:
    group = parser.getgroup("gridworks-proactor")
    group.addoption(
        "--live-test-verbose",
        action="store_true",
        help="Run LiveTest with verbose logging for parent and child.",
    )
    group.addoption(
        "--live-test-message-summary",
        action="store_true",
        help="Run LiveTest with message summary logging for parent and child.",
    )
    group.addoption(
        "--child-verbose",
        action="store_true",
        help="Run LiveTest with verbose logging for child.",
    )
    group.addoption(
        "--child-message-summary",
        action="store_true",
        help="Run LiveTest with message summary logging for child.",
    )
    group.addoption(
        "--parent-verbose",
        action="store_true",
        help="Run LiveTest with verbose logging for parent.",
    )
    group.addoption(
        "--parent-message-summary",
        action="store_true",
        help="Run LiveTest with message summary logging for parent.",
    )
    group.addoption(
        "--parent-on-screen",
        action="store_true",
        help="Pass parent_on_screen=True to LiveTest",
    )
    if include_tree:
        group.addoption(
            "--child1-verbose",
            action="store_true",
            help="Run TreeLiveTest with verbose logging for child1. Identical to --child-verbose",
        )
        group.addoption(
            "--child1-message-summary",
            action="store_true",
            help="Run LiveTest with message summary logging for child1. Identical to --child-message-summary",
        )
        group.addoption(
            "--child2-verbose",
            action="store_true",
            help="Run TreeLiveTest with verbose logging for child2.",
        )
        group.addoption(
            "--child2-message-summary",
            action="store_true",
            help="Run LiveTest with message summary logging for child2.",
        )
        group.addoption(
            "--child2-on-screen",
            action="store_true",
            help="Pass child2_on_screen=True to TreeLiveTest",
        )
    group.addoption(
        "--ack-tracking",
        action="store_true",
        help="Print ack tracking information in LiveTest exceptions",
    )
