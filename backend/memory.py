from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class Turn:
    role: str
    text: str
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


@dataclass
class ConversationMemory:
    turns: list[Turn] = field(default_factory=list)

    def add(self, role: str, text: str) -> None:
        clean = text.strip()
        if clean:
            self.turns.append(Turn(role=role, text=clean))

    def snapshot(self) -> list[dict[str, str]]:
        return [turn.__dict__.copy() for turn in self.turns]
