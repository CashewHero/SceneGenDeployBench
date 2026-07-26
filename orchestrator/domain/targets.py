from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DatasetTarget:
    dataset_name: str
    subset_prefix: str = ""

    @classmethod
    def parse(cls, value: str) -> "DatasetTarget":
        normalized = value.strip().strip("/")
        dataset_name, _, subset_prefix = normalized.partition("/")
        return cls(dataset_name=dataset_name, subset_prefix=subset_prefix)

    @property
    def path(self) -> str:
        if not self.subset_prefix:
            return self.dataset_name
        return f"{self.dataset_name}/{self.subset_prefix}"

    def matches(self, dataset_name: str, external_key: str) -> bool:
        if not self.dataset_name or dataset_name != self.dataset_name:
            return False
        if not self.subset_prefix:
            return True
        return external_key == self.subset_prefix or external_key.startswith(f"{self.subset_prefix}/")

    def relative_external_key(self, external_key: str) -> str | None:
        if not self.subset_prefix:
            return external_key
        if external_key == self.subset_prefix:
            return ""
        prefix = f"{self.subset_prefix}/"
        if external_key.startswith(prefix):
            return external_key[len(prefix):]
        return None

    def display_relative_external_key(self, external_key: str) -> str:
        relative = self.relative_external_key(external_key)
        if relative is None:
            return external_key
        if relative == "":
            return "/"
        return f"/{relative}" if self.subset_prefix else relative

    def child_path(self, child_name: str) -> str:
        if not child_name:
            return self.path
        base = self.path
        return f"{base}/{child_name}" if base else child_name

    def sql_filter(self) -> tuple[str, list[str]]:
        clauses = ["dataset_name = %s"]
        params = [self.dataset_name]
        if self.subset_prefix:
            clauses.append("(external_key = %s OR external_key LIKE %s)")
            params.extend([self.subset_prefix, f"{self.subset_prefix}/%"])
        return " AND ".join(clauses), params

