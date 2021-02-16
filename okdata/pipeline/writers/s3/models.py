from dataclasses import dataclass


@dataclass
class S3Source:
    filename: str
    key: str


class Distribution:
    def __init__(self, filenames: list):
        self.distribution_type = "file"
        self.filenames = filenames


class TaskConfig:
    def __init__(self, output_stage, write_to_latest):
        self.output_stage = output_stage
        self.write_to_latest = write_to_latest

    @classmethod
    def from_dict(cls, config):
        return cls(
            output_stage=config["output_stage"],
            write_to_latest=config.get("write_to_latest", False),
        )
