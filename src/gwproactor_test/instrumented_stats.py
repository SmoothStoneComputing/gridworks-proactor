import typing
from collections import defaultdict
from dataclasses import dataclass, field

from gwproto.messages import CommEvent

from gwproactor.stats import LinkStats, ProactorStats


@dataclass
class RecorderLinkStats(LinkStats):
    comm_events: list[CommEvent] = field(default_factory=list)
    forwarded: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    event_counts: dict[str, dict[str, int]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(int))
    )

    def __str__(self) -> str:
        s = super().__str__()
        if self.comm_events:
            s += "\n  Comm events:"
            for comm_event in self.comm_events:
                copy_event = comm_event.model_copy(
                    update={"MessageId": comm_event.MessageId[:6] + "..."}
                )
                s += f"\n    {str(copy_event)[:154]}"
        if self.forwarded:
            s += "\n  Forwarded events *sent* by type:"
            for message_type in sorted(self.forwarded):
                s += f"\n    {self.forwarded[message_type]:3d}: [{message_type}]"
        if self.event_counts:
            s += "\n  Events *received* by src and type:"
            for event_src in sorted(self.event_counts):
                s += f"\n    src: {event_src}"
                forwards_from_src = self.event_counts[event_src]
                for message_type in sorted(forwards_from_src):
                    s += f"\n      {forwards_from_src[message_type]:3d}: [{message_type}]"
        return s


class RecorderStats(ProactorStats):
    @classmethod
    def make_link(cls, link_name: str) -> RecorderLinkStats:
        return RecorderLinkStats(link_name)

    def link(self, name: str) -> RecorderLinkStats:
        return typing.cast(RecorderLinkStats, super().link(name))
