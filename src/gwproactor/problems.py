import textwrap
import traceback
from typing import Optional, Sequence

from gwproto.messages import ProblemEvent
from gwproto.messages import Problems as ProblemType

_EFFECTIVELY_NO_MAX = 1000000000


class Problems(ValueError):
    MAX_PROBLEMS = 10

    errors: list[BaseException]
    warnings: list[BaseException]
    max_problems: int = MAX_PROBLEMS

    def __init__(
        self,
        msg: str = "",
        warnings: Optional[Sequence[BaseException]] = None,
        errors: Optional[Sequence[BaseException]] = None,
        max_problems: Optional[int] = MAX_PROBLEMS,
    ) -> None:
        self.errors = [] if errors is None else list(errors)
        self.warnings = [] if warnings is None else list(warnings)
        if max_problems is None:
            max_problems = _EFFECTIVELY_NO_MAX
        self.max_problems = max_problems
        super().__init__(msg)

    def __bool__(self) -> bool:
        return bool(self.errors) or bool(self.warnings)

    def add_error(self, error: Exception) -> "Problems":
        if len(self.errors) < self.max_problems:
            self.errors.append(error)
        return self

    def add_warning(self, warning: Exception) -> "Problems":
        if len(self.warnings) < self.max_problems:
            self.warnings.append(warning)
        return self

    def add_problems(self, other: "Problems") -> "Problems":
        self.errors.extend(other.errors[: self.max_problems - len(self.errors)])
        self.warnings.extend(other.warnings[: self.max_problems - len(self.warnings)])
        return self

    def __str__(self) -> str:
        if bool(self):
            s = f"Problems: {len(self.errors)} errors, {len(self.warnings)} warnings, max: {self.max_problems}"
            for attr_name in ["errors", "warnings"]:
                lst = getattr(self, attr_name)
                if lst:
                    s += f"\n{attr_name.capitalize()}:\n"
                    for i, entry in enumerate(lst):
                        s += f"  {i:2d}: <{entry.__class__.__name__}>  " f"<{entry}>\n"
            return s
        return ""

    def error_traceback_str(self) -> str:
        s = ""
        for i, error in enumerate(self.errors):
            s += f"Traceback for error {i + 1} / {len(self.errors)}:\n"
            for line in traceback.format_exception(error):
                s += textwrap.indent(line, "  ")
        return s

    def __repr__(self) -> str:
        return str(self)

    def problem_event(self, summary: str, src: str = "") -> ProblemEvent:
        return ProblemEvent(
            Src=src,
            ProblemType=ProblemType.error if self.errors else ProblemType.warning,
            Summary=summary,
            Details=str(self),
        )
