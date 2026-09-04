from __future__ import annotations

import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class TestJob:
    job_id: str
    task_id: str
    required_driver: str
    created_at: float
    node_id: str = ""


@dataclass
class TestBatch:
    batch_id: str
    created_at: float
    owner_master: str
    jobs: list[TestJob] = field(default_factory=list)
    status: str = "pending"

    def derive_status(self, tasks: dict) -> str:
        states: list[str] = []
        for job in self.jobs:
            task = tasks.get(job.task_id)
            if task is None:
                states.append("pending")
                continue
            states.append(task.status.value if hasattr(task.status, "value") else str(task.status))
        if not states:
            return self.status
        terminal = {"completed", "failed", "timeout", "cancelled", "migrated", "partial"}
        running = any(s in ("running", "pending") for s in states)
        all_done = all(s in terminal for s in states)
        if all_done:
            if all(s == "completed" for s in states):
                return "completed"
            if all(s in ("failed", "timeout", "cancelled") for s in states):
                return "failed"
            return "partial"
        if running:
            return "running"
        return self.status

    def to_snapshot(self) -> dict:
        return {
            "batch_id": self.batch_id,
            "created_at": self.created_at,
            "owner_master": self.owner_master,
            "status": self.status,
            "jobs": [
                {
                    "job_id": j.job_id,
                    "task_id": j.task_id,
                    "required_driver": j.required_driver,
                    "created_at": j.created_at,
                    "node_id": j.node_id,
                }
                for j in self.jobs
            ],
        }

    @classmethod
    def from_snapshot(cls, data: dict) -> TestBatch:
        jobs = [
            TestJob(
                job_id=j.get("job_id", ""),
                task_id=j.get("task_id", ""),
                required_driver=j.get("required_driver", ""),
                created_at=j.get("created_at", 0.0),
                node_id=j.get("node_id", ""),
            )
            for j in data.get("jobs", [])
        ]
        return cls(
            batch_id=data.get("batch_id", ""),
            created_at=data.get("created_at", 0.0),
            owner_master=data.get("owner_master", ""),
            jobs=jobs,
            status=data.get("status", "pending"),
        )


__all__ = ["TestJob", "TestBatch"]
