import asyncio
import traceback
from asyncio import Task
from typing import Any, Iterable, Optional


def str_tasks(
    loop_: asyncio.AbstractEventLoop,
    tag: str = "",
    tasks: Optional[Iterable[Task[Any]]] = None,
) -> str:
    s = ""
    try:
        if tasks is None:
            tasks = asyncio.all_tasks(loop_)
        tasks = list(tasks)
        s += f"Tasks: {len(tasks)}  [{tag}]\n"

        def _get_task_exception_str(task_: asyncio.Task[Any]) -> str:
            try:
                exception_ = task_.exception()
            except asyncio.CancelledError as _e:
                exception_ = _e
            except asyncio.InvalidStateError:
                exception_ = None
            if exception_ is None:
                return "exception: None"
            if isinstance(exception_, asyncio.CancelledError):
                return "(cancelled)"
            return f"exception: {type(exception_)}  {exception_}"

        for i, task in enumerate(tasks):
            s += (
                f"\t{i + 1}/{len(tasks)}  "
                f"{task.get_name():30s}  "
                f"done:{task.done()}   "
                f"{_get_task_exception_str(task)}  "
                f"{task.get_coro()}\n"
            )
    except Exception as e:  # noqa: BLE001
        try:
            s += "ERROR in str_tasks:\n"
            s += "".join(traceback.format_exception(e))
            s += "\n"
        except:  # noqa: E722, S110
            pass
    return s
