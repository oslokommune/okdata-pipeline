from dataclasses import dataclass
from typing import Optional


@dataclass
class S3Source:
    filename: str
    key: str


class Distribution:
    def __init__(self, filenames: list, content_type: Optional[str] = None):
        self.distribution_type = "file"
        self.filenames = filenames
        self.content_type = content_type

    def as_dict(self):
        d = {"distribution_type": self.distribution_type, "filenames": self.filenames}
        if self.content_type:
            d["content_type"] = self.content_type
        return d


class TaskConfig:
    def __init__(self, output_stage, write_to_latest, content_type=None):
        self.output_stage = output_stage
        self.write_to_latest = write_to_latest
        self.content_type = content_type

    @classmethod
    def from_dict(cls, config):
        return cls(
            output_stage=config["output_stage"],
            write_to_latest=config.get("write_to_latest", False),
            content_type=config.get("content_type"),
        )
