import datetime
import textwrap

from gwproto.messages import Ack, AnyEvent, EventBase
from result import Err, Ok

from gwproactor import Problems
from gwproactor.persister import ByteDecodingError, UIDMissingWarning
from gwproactor_test.instrumented_proactor import InstrumentedProactor


class EventAckConsistencyError(Exception): ...


class UnexpectedMessage(EventAckConsistencyError): ...


class CountInconsistency(EventAckConsistencyError): ...


class _EventAckCountsIntermediate:
    problems: Problems

    paused_ack_list: list[Ack]

    in_flight_set: set[str]
    pending_set: set[str]
    paused_ack_set: set[str]
    in_flight_not_paused_set: set[str]
    pending_not_paused_set: set[str]
    paused_not_events_set: set[str]

    pending_events: dict[str, EventBase]
    in_flight_not_paused_events: dict[str, EventBase]
    pending_not_paused_events: dict[str, EventBase]
    paused_not_events_list: list[str]

    def __init__(
        self,
        *,
        parent: InstrumentedProactor,
        child: InstrumentedProactor,
        verbose: bool = False,
    ) -> None:
        self._find_errors(parent=parent, child=child)
        if self.ok() and not verbose:
            self.pending_events = {}
            self.in_flight_not_paused_events = {}
            self.pending_not_paused_events = {}
            self.paused_not_events_list = []
        else:
            self._sort_events(child=child)

    def _find_errors(
        self, *, parent: InstrumentedProactor, child: InstrumentedProactor
    ) -> None:
        in_flight_set = set(child.links.in_flight_events.keys())
        pending_set = set(child.event_persister.pending_ids())
        paused_ack_list: list[Ack] = []
        problems = Problems()
        for paused in parent.needs_ack:
            if paused.link_name == parent.downstream_client:
                match paused.message.Payload:
                    case Ack():
                        paused_ack_list.append(paused.message.Payload)
                    case _:
                        problems.add_error(
                            UnexpectedMessage(paused.message.Header.TypeName)
                        )
        paused_ack_set = {paused_ack.AckMessageID for paused_ack in paused_ack_list}
        in_flight_not_paused_set = in_flight_set - paused_ack_set
        pending_not_paused_set = pending_set - paused_ack_set
        paused_not_events_set = paused_ack_set - (in_flight_set | paused_ack_set)
        self.problems = problems
        self.in_flight_set = in_flight_set
        self.pending_set = pending_set
        self.paused_ack_list = paused_ack_list
        self.paused_ack_set = paused_ack_set
        self.in_flight_not_paused_set = in_flight_not_paused_set
        self.pending_not_paused_set = pending_not_paused_set
        self.paused_not_events_set = paused_not_events_set

    def _sort_pending_events(self, *, child: InstrumentedProactor) -> None:
        # Sort pending events by time (persister does not guarantee order)
        pending_event_list: list[AnyEvent] = []
        for event_id in self.pending_set:
            match child.event_persister.retrieve(event_id):
                case Ok(content):
                    if content is not None:
                        try:
                            pending_event_list.append(
                                AnyEvent.model_validate_json(content)
                            )
                        except Exception as e:  # noqa: BLE001
                            self.problems.add_error(e).add_error(
                                ByteDecodingError("reupload_events", uid=event_id)
                            )
                    else:
                        self.problems.add_error(
                            UIDMissingWarning("Ack consistency check", uid=event_id)
                        )
                case Err(one_retrieve_problems):
                    self.problems.add_error(one_retrieve_problems)
        child.event_persister._num_retrieves -= len(pending_event_list)  # type: ignore # noqa
        pending_event_list.sort(key=lambda event_: event_.TimeCreatedMs)
        self.pending_events = {
            pending_event.MessageId: pending_event
            for pending_event in pending_event_list
        }

    def _sort_events(self, *, child: InstrumentedProactor) -> None:
        self._sort_pending_events(child=child)
        self.in_flight_not_paused_events = {
            event.MessageId: event
            for event in sorted(
                [
                    child.links.in_flight_events[x]
                    for x in self.in_flight_not_paused_set
                ],
                key=lambda x: x.TimeCreatedMs,
            )
        }
        self.pending_not_paused_events = {
            event.MessageId: event
            for event in sorted(
                [self.pending_events[x] for x in self.pending_not_paused_set],
                key=lambda x: x.TimeCreatedMs,
            )
        }
        self.paused_not_events_list = [
            ack.AckMessageID
            for ack in self.paused_ack_list
            if ack.AckMessageID in self.paused_not_events_set
        ]

    def ok(self) -> bool:
        return (
            not self.problems
            and not self.in_flight_not_paused_set
            and not self.pending_not_paused_set
            and not self.paused_not_events_set
        )

    def __bool__(self) -> bool:
        return self.ok()


class _EventAckReportGenerator:
    child: InstrumentedProactor
    c: _EventAckCountsIntermediate
    verbose: bool
    summary: str
    non_error_report: str
    error_report: str
    report: str

    def __init__(
        self,
        child: InstrumentedProactor,
        c: _EventAckCountsIntermediate,
        *,
        verbose: bool = False,
    ) -> None:
        self.child = child
        self.c = c
        self.verbose = verbose
        self._summarize()
        if not verbose and self.c.ok():
            self.summary += "Acks CONSISTENT\n"
            self.report = self.summary
        else:
            self._make_non_error_report()
            self._make_error_report()
            if self.c.ok():
                self.summary += "Acks CONSISTENT\n"
            self.report = self.non_error_report + self.error_report + self.summary

    def _summarize(self) -> None:
        self.summary = (
            f"Parent paused acks: {len(self.c.paused_ack_set)}\n"
            f"Child pending events: {len(self.c.pending_set)}\n"
            f"Child in-flight events: {len(self.c.in_flight_set)}\n"
        )

    def _make_non_error_report(self) -> None:
        report = f"Parent paused acks: {len(self.c.paused_ack_list)}\n"
        event: EventBase | str
        for i, paused_ack in enumerate(self.c.paused_ack_list):
            if paused_ack.AckMessageID in self.child.links.in_flight_events:
                event = self.child.links.in_flight_events[paused_ack.AckMessageID]
                loc = "in-flight"
            elif paused_ack.AckMessageID in self.c.pending_events:
                event = self.c.pending_events[paused_ack.AckMessageID]
                loc = "pending"
            else:
                event = paused_ack.AckMessageID
                loc = "*UKNONWN*"
            report += self._event_line(event, loc, i + 1, len(self.c.paused_ack_list))
        report += f"Child in-flight events: {self.child.links.num_in_flight}\n"
        for i, event in enumerate(self.child.links.in_flight_events.values()):
            report += self._event_line(
                event, "in-flight", i + 1, len(self.c.pending_events)
            )
        report += f"Child pending events: {len(self.c.pending_events)}\n"
        for i, event in enumerate(self.c.pending_events.values()):
            report += self._event_line(
                event, "pending", i + 1, len(self.c.pending_events)
            )
        self.non_error_report = report

    def _make_error_report(self) -> None:
        report = ""
        events = self.c.in_flight_not_paused_events.values()
        line = f"Child in-flight events not in parent paused acks: {len(events)}\n"
        report += line
        self.summary += line
        for i, event in enumerate(events):
            report += self._event_line(event, "in-flight", i + 1, len(events))

        events = self.c.pending_not_paused_events.values()
        line = f"Child pending events not in parent paused acks: {len(events)}\n"
        report += line
        self.summary += line
        for i, event in enumerate(events):
            report += self._event_line(event, "pending", i + 1, len(events))

        event_ids = self.c.paused_not_events_list
        line = f"Paused acks not in child in-flight or pending: {len(event_ids)}\n"
        report += line
        self.summary += line
        for i, event_id in enumerate(event_ids):
            report += self._event_line(event_id, "", i + 1, len(event_ids))

        line = f"Problems making ack count calculation: {len(self.c.problems)}"
        report += line
        self.summary += line
        report += textwrap.indent(str(self.c.problems), "  ")

        self.error_report = report

    @classmethod
    def _event_line(cls, event: EventBase | str, loc: str, i: int, n: int) -> str:
        if isinstance(event, EventBase):
            event_id = event.MessageId
            dt = datetime.datetime.fromtimestamp(
                event.TimeCreatedMs / 1000, tz=datetime.UTC
            )
            info_s = f"{dt}   {event.TypeName}"
        else:
            event_id = event
            info_s = ""
        return f"  {i:3d} / {n:3d}   {event_id[:8]}   {loc:10s}   {info_s}\n"

    @property
    def problems(self) -> Problems:
        return self.c.problems


class EventAckCounts:
    summary: str = ""
    report: str = ""
    problems: Problems

    def __init__(
        self,
        *,
        parent: InstrumentedProactor,
        child: InstrumentedProactor,
        verbose: bool = False,
    ) -> None:
        calc = _EventAckCountsIntermediate(parent=parent, child=child, verbose=verbose)
        reporter = _EventAckReportGenerator(child=child, c=calc, verbose=verbose)
        self.problems = calc.problems
        self.summary = reporter.summary
        self.report = reporter.report

    def ok(self) -> bool:
        return not self.problems

    def __bool__(self) -> bool:
        return self.ok()
