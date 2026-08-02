from __future__ import annotations

import argparse
import os
import sys

from cli.rendering import add_list_controls
from cli.rendering import effective_row_limit
from cli.rendering import format_duration
from cli.rendering import format_relative_time
from cli.rendering import format_timestamp
from cli.rendering import limit_rows
from cli.rendering import output_format
from cli.rendering import print_json
from cli.rendering import print_truncation_notice
from cli.rendering import render_key_value
from cli.rendering import render_table
from cli.rendering import schedule_window_rows
from cli.commands import (
    JobListOptions,
    add_job,
    cancel_pipeline,
    cancel_jobs_matching_filters,
    config_show_sections,
    config_sources_payload,
    config_validate_payload,
    configure_logging,
    download_dataset,
    event_message,
    get_runner_status,
    list_batches,
    list_dataset_samples,
    list_datasets,
    list_jobs,
    list_outputs,
    list_pipeline_runs,
    list_pipelines,
    list_runners,
    logger,
    rescan_datasets,
    run_script,
    show_batch,
    show_dataset,
    show_job,
    show_output,
    show_pipeline_run,
    show_runner,
    update_jobs_window_flag,
    add_pipeline,
    validate_pipeline,
)
from app.service import run_service

DEFAULT_ORCHESTRATOR_PORT = 58080


def parse_boolean(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise argparse.ArgumentTypeError("expected true or false")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="deploybench")
    parser.add_argument("--config", help="path to system YAML config")
    parser.add_argument("--json", action="store_true", help="print JSON output")
    parser.add_argument("--format", choices=["table", "text", "json"], help="explicit output format")
    parser.add_argument("--quiet", action="store_true", help="suppress summary text where possible")
    parser.add_argument("--verbose", action="store_true", help="include more detail in text output")

    subparsers = parser.add_subparsers(dest="command", required=True)

    serve_parser = subparsers.add_parser("serve", help="run the long-lived orchestrator HTTP service")
    serve_parser.add_argument(
        "--host",
        default=os.getenv("ORCHESTRATOR_HOST", "0.0.0.0"),
        help="bind host for the orchestrator service",
    )
    serve_parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("ORCHESTRATOR_PORT", str(DEFAULT_ORCHESTRATOR_PORT))),
        help="bind port for the orchestrator service; defaults to ORCHESTRATOR_PORT or 58080",
    )

    run_parser = subparsers.add_parser(
        "run",
        help="run a script or command in a temporary container",
    )
    run_parser.add_argument(
        "script",
        nargs="?",
        help="script file to run; omit it and put an inline command after --",
    )
    run_parser.add_argument(
        "--image",
        required=True,
        help="container image",
    )
    run_parser.add_argument(
        "--access",
        action="append",
        default=[],
        help=(
            "grant datasets, output, pipelines, model-cache, database, or all; "
            "repeat or use commas"
        ),
    )
    run_parser.add_argument(
        "--env",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="set a container environment variable; repeat as needed",
    )
    run_parser.add_argument(
        "--mount",
        action="append",
        default=[],
        metavar="SOURCE:TARGET[:ro|rw]",
        help="mount another orchestrator-visible path; defaults to read-only",
    )
    run_parser.add_argument(
        "--workdir",
        default="/workspace",
        help="container working directory; defaults to /workspace",
    )

    job_parser = subparsers.add_parser("job", help="create, inspect, update, and cancel jobs")
    job_subparsers = job_parser.add_subparsers(dest="job_command", required=True)
    job_add = job_subparsers.add_parser("add", help="create planned jobs from dataset or output targets")
    job_add.add_argument(
        "--dataset",
        help="dataset or output target for inputs.data; optional when --candidate supplies the primary samples",
    )
    job_add.add_argument(
        "--candidate",
        help="dataset or output target for inputs.candidate",
    )
    job_add.add_argument("--runner", help="runner name or exact runner selector such as name@version")
    job_add.add_argument(
        "--reference",
        action="append",
        dest="references",
        default=[],
        help="dataset or output path to send as optional samples in inputs.references; repeat as needed",
    )
    job_add.add_argument(
        "--set",
        action="append",
        dest="settings",
        default=[],
        metavar="KEY=VALUE",
        help="override a runner job parameter; repeat for multiple values",
    )
    job_add.add_argument("--timeout-minutes", type=float, help="override job timeout in minutes")
    job_add.add_argument("--source-job", dest="source_job_id", help="optional upstream job reference")
    job_add.add_argument("--allow-outside-window", action="store_true", help="allow this job to start outside active windows")

    job_list = job_subparsers.add_parser("list", help="list jobs or grouped job summaries")
    job_list.add_argument("--job", action="append", dest="job_ids", default=[], help="exact job id or batch_id/job_id")
    job_list.add_argument("--dataset", help="dataset path such as testset1, testset1/Gascola/P000, or testset1/.../sample.png")
    job_list.add_argument("--runner", help="runner name or exact runner selector such as name@version")
    job_list.add_argument("--state", action="append", dest="states", default=[], help="repeatable state filter")
    job_list.add_argument("--view", choices=["groups", "jobs"], default="groups", help="grouped or raw job rows")
    job_list.add_argument("--sort", default="updated_at", choices=["updated_at", "created_at", "completed_at", "dataset", "runner"], help="sort field")
    job_list.set_defaults(desc=True)
    job_list_direction = job_list.add_mutually_exclusive_group()
    job_list_direction.add_argument("--desc", action="store_true", dest="desc", help="sort descending")
    job_list_direction.add_argument("--asc", action="store_false", dest="desc", help="sort ascending")
    add_list_controls(job_list)
    job_list.add_argument("--created-since", help="duration like 1h or timestamp")
    job_list.add_argument("--created-until", help="duration like 1h or timestamp")
    job_list.add_argument("--updated-since", help="duration like 1h or timestamp")
    job_list.add_argument("--updated-until", help="duration like 1h or timestamp")
    job_list.add_argument("--finished-since", help="duration like 1h or timestamp")
    job_list.add_argument("--finished-until", help="duration like 1h or timestamp")
    job_list.add_argument("--failed", action="store_true", help="show only failed jobs")
    job_list.add_argument("--active", action="store_true", help="show only pending jobs")
    job_list.add_argument("--completed", action="store_true", help="show only completed jobs")

    job_show = job_subparsers.add_parser("show", help="show one exact job")
    job_show.add_argument("job_id", help="job id")

    job_update = job_subparsers.add_parser("update", help="update mutable job fields for jobs matched by filters")
    job_update.add_argument("--job", action="append", dest="job_ids", default=[], help="exact job id")
    job_update.add_argument("--dataset", help="dataset path such as testset1, testset1/Gascola/P000, or testset1/.../sample.png")
    job_update.add_argument("--runner", help="runner name or exact runner selector such as name@version")
    job_update.add_argument("--allow-outside-window", action="store_true", help="allow this job to start outside active windows")
    job_update.add_argument("--disallow-outside-window", action="store_true", help="require this job to wait for an active window")

    job_cancel = job_subparsers.add_parser("cancel", help="cancel pending jobs matched by filters")
    job_cancel.add_argument("--job", action="append", dest="job_ids", default=[], help="exact job id")
    job_cancel.add_argument("--dataset", help="dataset path such as testset1, testset1/Gascola/P000, or testset1/.../sample.png")
    job_cancel.add_argument("--runner", help="runner name or exact runner selector such as name@version")

    runner_parser = subparsers.add_parser("runner", help="inspect configured runners")
    runner_subparsers = runner_parser.add_subparsers(dest="runner_command", required=True)
    runner_list = runner_subparsers.add_parser("list", help="list configured runners")
    add_list_controls(runner_list)
    runner_show = runner_subparsers.add_parser("show", help="show one runner definition")
    runner_show.add_argument("runner_selector", help="runner selector, bare name, or name@latest")
    runner_status = runner_subparsers.add_parser("status", help="query live runner status when possible")
    runner_status.add_argument("runner_selector", help="runner selector, bare name, or name@latest")

    dataset_parser = subparsers.add_parser("dataset", help="inspect datasets and samples")
    dataset_subparsers = dataset_parser.add_subparsers(dest="dataset_command", required=True)
    dataset_list = dataset_subparsers.add_parser("list", help="list dataset roots or direct children under a path")
    dataset_list.add_argument("target", nargs="?", help="optional dataset path such as testset1 or testset1/subset")
    add_list_controls(dataset_list)
    dataset_show = dataset_subparsers.add_parser("show", help="show one dataset or subset")
    dataset_show.add_argument("target", help="dataset name or dataset/subset path")
    dataset_show.add_argument("--sample", action="store_true", help="include sample rows under the target path")
    add_list_controls(dataset_show, limit_help="maximum number of subset or sample rows to show")
    dataset_download = dataset_subparsers.add_parser("download", help="create a dataset download job")
    dataset_download.add_argument("dataset_name", help="dataset name to create or update")
    dataset_download.add_argument("--runner", help="dataset_downloader runner name or exact selector")
    dataset_download.add_argument(
        "--set",
        action="append",
        dest="settings",
        default=[],
        metavar="KEY=VALUE",
        help="pass a downloader parameter; repeat for multiple values",
    )
    dataset_download.add_argument("--timeout-minutes", type=float, help="override job timeout in minutes")
    dataset_download.add_argument(
        "--rescan",
        type=parse_boolean,
        metavar="{true,false}",
        default=None,
        help="override whether to rescan after download; omit to use the runner setting",
    )
    dataset_download.add_argument(
        "--allow-outside-window",
        action="store_true",
        help="allow this job to start outside active windows",
    )
    dataset_rescan = dataset_subparsers.add_parser(
        "rescan",
        help="refresh the durable sample index from dataset manifests",
    )
    dataset_rescan.add_argument(
        "dataset_name",
        nargs="?",
        help="dataset name to rescan; omit to rescan every dataset",
    )

    batch_parser = subparsers.add_parser("batch", help="inspect durable batches")
    batch_subparsers = batch_parser.add_subparsers(dest="batch_command", required=True)
    batch_list = batch_subparsers.add_parser("list", help="list durable batches")
    batch_list.add_argument("--runner", help="runner name or exact runner selector such as name@version")
    batch_list.add_argument("--open", action="store_true", help="show only open batches")
    batch_list.add_argument("--closed", action="store_true", help="show only closed batches")
    add_list_controls(batch_list)
    batch_show = batch_subparsers.add_parser("show", help="show one durable batch")
    batch_show.add_argument("batch_id", help="exact batch id")

    output_parser = subparsers.add_parser("output", help="inspect indexed generated outputs")
    output_subparsers = output_parser.add_subparsers(dest="output_command", required=True)
    output_list = output_subparsers.add_parser("list", help="list indexed generated outputs")
    output_list.add_argument("--dataset", help="dataset path such as testset1 or testset1/subset")
    output_list.add_argument("--runner", help="source runner name or exact runner selector such as name@version")
    add_list_controls(output_list)
    output_show = output_subparsers.add_parser("show", help="show indexed generated output samples")
    output_show.add_argument("target", help="output path such as output/runner/dataset/subset")
    add_list_controls(output_show, limit_help="maximum number of output samples to show")

    config_parser = subparsers.add_parser("config", help="inspect effective configuration")
    config_subparsers = config_parser.add_subparsers(dest="config_command", required=True)
    config_subparsers.add_parser("show", help="show resolved config")
    config_subparsers.add_parser("validate", help="validate resolved config")
    config_sources = config_subparsers.add_parser("sources", help="show where effective config values came from")
    add_list_controls(config_sources)

    pipeline_parser = subparsers.add_parser(
        "pipeline", help="validate, add, inspect, and cancel pipelines"
    )
    pipeline_subparsers = pipeline_parser.add_subparsers(
        dest="pipeline_command", required=True
    )
    pipeline_subparsers.add_parser("list", help="list pipeline definitions")
    pipeline_validate = pipeline_subparsers.add_parser(
        "validate", help="validate a pipeline definition"
    )
    pipeline_validate.add_argument("pipeline_name", nargs="?")
    pipeline_validate.add_argument("-f", "--file", dest="pipeline_file")
    pipeline_add = pipeline_subparsers.add_parser("add", help="add a pipeline")
    pipeline_add.add_argument("pipeline_name", nargs="?")
    pipeline_add.add_argument("-f", "--file", dest="pipeline_file")
    pipeline_add.add_argument("--dataset", help="override the pipeline dataset")
    pipeline_add.add_argument("--runner", help="override the pipeline runner")
    pipeline_add.add_argument(
        "--matrix",
        action="append",
        default=[],
        metavar="KEY=VALUE1,VALUE2",
        help="override one declared matrix axis",
    )
    pipeline_add.add_argument(
        "--allow-outside-window",
        action="store_true",
        help="allow jobs created by this pipeline to start outside active windows",
    )
    pipeline_runs = pipeline_subparsers.add_parser(
        "runs", help="list durable pipeline runs"
    )
    add_list_controls(pipeline_runs)
    pipeline_show = pipeline_subparsers.add_parser(
        "show", help="show one pipeline run"
    )
    pipeline_show.add_argument("pipeline_run_id")
    pipeline_cancel = pipeline_subparsers.add_parser(
        "cancel", help="cancel one active pipeline run"
    )
    pipeline_cancel.add_argument("pipeline_run_id")

    return parser


def handle_config_show(args: argparse.Namespace) -> int:
    payload = config_show_sections(args.config)
    if output_format(args, "text") == "json":
        print_json(payload)
        return 0
    scheduling = payload["scheduling"] or {}
    print(render_key_value([
        ("Dataset Root", payload["storage"]["dataset_root"]),
        ("Output Root", payload["storage"]["output_root"]),
        ("Pipeline Root", payload["storage"]["pipeline_root"]),
        ("Runner Catalog", payload["catalogs"]["runners"]),
        ("Pipeline Catalog", payload["catalogs"]["pipelines"]),
        ("DB Host", payload["database"]["host"]),
        ("DB Port", payload["database"]["port"]),
        ("DB Name", payload["database"]["name"]),
        ("DB User", payload["database"]["user"]),
        ("Poll Startup", payload["polling"]["startup_seconds"]),
        ("Poll Post Submit", payload["polling"]["post_submit_seconds"]),
        ("Poll Running", payload["polling"]["running_seconds"]),
        ("Scheduling Timezone", scheduling.get("timezone") or "-"),
        ("Max Batch Size", scheduling.get("max_batch_size") or "-"),
        ("Max Attempts", scheduling.get("max_attempts") or "-"),
        ("Job Timeout Minutes", scheduling.get("job_timeout_minutes") or "-"),
        ("Startup Timeout Minutes", scheduling.get("startup_timeout_minutes") or "-"),
    ]))
    window_rows = schedule_window_rows(scheduling)
    if window_rows:
        print()
        print("WINDOWS")
        print(render_table(["DAYS", "START", "END", "START POLICY", "END POLICY"], window_rows))
    return 0


def handle_run(args: argparse.Namespace) -> int:
    return run_script(
        args.config,
        image=args.image,
        script_path=args.script,
        command=list(args.run_command),
        access=args.access,
        environment=args.env,
        mounts=args.mount,
        workdir=args.workdir,
    )


def handle_config_validate(args: argparse.Namespace) -> int:
    payload = config_validate_payload(args.config)
    if output_format(args, "text") == "json":
        print_json(payload)
        return 0
    print(render_key_value([
        ("Valid", "yes" if payload["valid"] else "no"),
        ("Config Version", payload["config_version"]),
        ("Runner Count", payload["runner_count"]),
        ("Dataset Root", payload["dataset_root"]),
        ("Output Root", payload["output_root"]),
        ("Pipeline Root", payload["pipeline_root"]),
        ("DB Host", payload["db_host"]),
        ("DB Name", payload["db_name"]),
    ]))
    return 0


def handle_config_sources(args: argparse.Namespace) -> int:
    rows = config_sources_payload(args.config)
    rows, total_rows = limit_rows(rows, args)
    if output_format(args, "table") == "json":
        print_json(rows)
        return 0
    table_rows = [
        {
            "KEY": row["key"],
            "VALUE": row["value"],
            "SOURCE": row["source"],
        }
        for row in rows
    ]
    print(render_table(["KEY", "VALUE", "SOURCE"], table_rows))
    print_truncation_notice(len(rows), total_rows)
    return 0


def handle_runner_list(args: argparse.Namespace) -> int:
    rows = list_runners(args.config)
    rows, total_rows = limit_rows(rows, args)
    if output_format(args, "table") == "json":
        print_json(rows)
        return 0
    table_rows = [
        {
            "RUNNER": row["selector"],
            "LATEST": "yes" if row["latest"] else "-",
            "MISSING": "yes" if row.get("missing") else "-",
            "TYPE": row["type"],
            "VERSION": row["version"],
            "DRIVER": row["launcher_driver"] or "-",
            "IMAGE": row["image"] or "-",
            "LAST SEEN": format_relative_time(row["last_seen"]),
        }
        for row in rows
    ]
    print(render_table(["RUNNER", "LATEST", "MISSING", "TYPE", "VERSION", "DRIVER", "IMAGE", "LAST SEEN"], table_rows))
    print_truncation_notice(len(rows), total_rows)
    return 0


def handle_runner_show(args: argparse.Namespace) -> int:
    payload = show_runner(args.config, args.runner_selector)
    if output_format(args, "text") == "json":
        print_json(payload)
        return 0
    scheduling = payload.get("scheduling") or {}
    inputs = payload["inputs"]
    print(render_key_value([
        ("Requested Runner", payload["requested_runner"]),
        ("Runner", payload["selector"]),
        ("Name", payload["name"]),
        ("Display Name", payload["display_name"]),
        ("Type", payload["type"]),
        ("Version", payload["version"]),
        ("Latest", "yes" if payload["latest"] else "no"),
        ("Missing", "yes" if payload.get("missing") else "no"),
        ("Contract Version", payload["contract_version"]),
        ("Inputs", inputs),
        ("Job Parameter Defaults", payload["job_parameters"] or "-"),
        ("Scheduling Timezone", scheduling.get("timezone") or "-"),
        ("Max Batch Size", scheduling.get("max_batch_size") or "-"),
        ("Max Attempts", scheduling.get("max_attempts") or "-"),
        ("Launcher", payload["launcher"]),
        ("Jobs Total", payload["job_counts"]["total"]),
        ("Jobs Completed", payload["job_counts"]["completed"]),
        ("Jobs Cancelled", payload["job_counts"]["cancelled"]),
        ("Jobs Pending", payload["job_counts"]["pending"]),
        ("Jobs Failed", payload["job_counts"]["failed"]),
    ]))
    window_rows = schedule_window_rows(scheduling)
    if window_rows:
        print()
        print("WINDOWS")
        print(render_table(["DAYS", "START", "END", "START POLICY", "END POLICY"], window_rows))
    return 0


def handle_runner_status(args: argparse.Namespace) -> int:
    payload = get_runner_status(args.config, args.runner_selector)
    if output_format(args, "text") == "json":
        print_json(payload)
        return 0
    pairs = [(key.replace("_", " ").title(), value) for key, value in payload.items()]
    print(render_key_value(pairs))
    return 0


def handle_dataset_list(args: argparse.Namespace) -> int:
    rows = list_datasets(args.config, target=args.target)
    rows, total_rows = limit_rows(rows, args)
    if output_format(args, "table") == "json":
        print_json(rows)
        return 0
    if args.target:
        table_rows = [
            {
                "TYPE": row["kind"],
                "PATH": row["path"],
                "SAMPLES": row["samples"],
                "DATA TYPES": ",".join(row["data_types"]) or "-",
                "LAST JOB": row["last_job"] or "-",
                "LAST STATE": row["last_state"] or "-",
                "UPDATED": format_relative_time(row["updated_at"]),
            }
            for row in rows
        ]
        print(render_table(["TYPE", "PATH", "SAMPLES", "DATA TYPES", "LAST JOB", "LAST STATE", "UPDATED"], table_rows))
    else:
        table_rows = [
            {
                "DATASET": row["dataset"],
                "VERSION": row["version"],
                "SAMPLES": row["samples"],
                "DATA TYPES": ",".join(row["data_types"]) or "-",
                "LAST JOB": format_relative_time(row["last_job"]),
            }
            for row in rows
        ]
        print(render_table(["DATASET", "VERSION", "SAMPLES", "DATA TYPES", "LAST JOB"], table_rows))
    print_truncation_notice(len(rows), total_rows)
    return 0


def handle_dataset_show(args: argparse.Namespace) -> int:
    payload = show_dataset(args.config, args.target)
    sample_rows = list_dataset_samples(args.config, args.target) if args.sample else None
    subset_rows = [
        {"DATASET": dataset_path, "SAMPLES": count}
        for dataset_path, count in sorted(payload["subset_counts"].items())
    ]
    subset_rows, total_subset_rows = limit_rows(subset_rows, args)
    if sample_rows is not None:
        sample_rows, total_sample_rows = limit_rows(sample_rows, args)
    else:
        total_sample_rows = 0
    if output_format(args, "text") == "json":
        payload = {
            **payload,
            "subset_counts": {row["DATASET"]: row["SAMPLES"] for row in subset_rows},
        }
        if sample_rows is not None:
            payload = {**payload, "samples": sample_rows}
        print_json(payload)
        return 0
    print(render_key_value([
        ("Target", payload["target"]),
        ("Dataset", payload["dataset"]),
        ("Version", payload["version"]),
        ("Sample Count", payload["sample_count"]),
        ("Data Types", ", ".join(payload["data_types"]) or "-"),
        ("Jobs Total", payload["job_counts"]["total"]),
        ("Jobs Completed", payload["job_counts"]["completed"]),
        ("Jobs Cancelled", payload["job_counts"]["cancelled"]),
        ("Jobs Pending", payload["job_counts"]["pending"]),
        ("Jobs Failed", payload["job_counts"]["failed"]),
    ]))
    if subset_rows:
        print()
        print(render_table(["DATASET", "SAMPLES"], subset_rows))
        print_truncation_notice(len(subset_rows), total_subset_rows, label="subset rows")
    if sample_rows is not None:
        print()
        table_rows = [
            {
                "SAMPLE": row["sample"],
                "DATA TYPES": ",".join(row["data_types"]) or "-",
                "LAST JOB": row["last_job"] or "-",
                "LAST STATE": row["last_state"] or "-",
                "UPDATED": format_relative_time(row["updated_at"]),
            }
            for row in sample_rows
        ]
        print(render_table(["SAMPLE", "DATA TYPES", "LAST JOB", "LAST STATE", "UPDATED"], table_rows))
        print_truncation_notice(len(sample_rows), total_sample_rows, label="sample rows")
    return 0


def handle_dataset_download(args: argparse.Namespace) -> int:
    payload = download_dataset(
        args.config,
        dataset_name=args.dataset_name,
        runner=args.runner,
        settings=args.settings,
        timeout_minutes=args.timeout_minutes,
        rescan_after_download=args.rescan,
        allow_start_outside_window=args.allow_outside_window,
    )
    if output_format(args, "text") == "json":
        print_json(payload)
        return 0
    print(render_key_value([
        ("Job Count", payload["job_count"]),
        ("Created", format_timestamp(payload["created_at"])),
        ("Dataset", payload["dataset"]),
        ("Runner", payload["runner"]),
        ("Job Type", payload["job_type"]),
        ("Timeout Minutes", round(payload["timeout_seconds"] / 60, 3)),
        ("Rescan After Download", "yes" if payload["rescan_after_download"] else "no"),
        ("Allow Outside Window", "yes" if payload["allow_start_outside_window"] else "no"),
    ]))
    if payload["parameters"]:
        print()
        parameter_rows = [
            {"KEY": key, "VALUE": value}
            for key, value in sorted(payload["parameters"].items())
        ]
        print(render_table(["KEY", "VALUE"], parameter_rows))
    if payload["job_rows"]:
        print()
        table_rows = [
            {
                "JOB": row["job_ref"],
                "DATASET": row["dataset"],
                "RUNNER": row["runner"],
                "STATE": row["state"],
                "ATTEMPT": row["attempt"],
                "UPDATED": format_relative_time(row["updated_at"]),
            }
            for row in payload["job_rows"]
        ]
        print(render_table(["JOB", "DATASET", "RUNNER", "STATE", "ATTEMPT", "UPDATED"], table_rows))
    return 0


def handle_dataset_rescan(args: argparse.Namespace) -> int:
    payload = rescan_datasets(
        args.config,
        dataset_name=args.dataset_name,
    )
    if output_format(args, "text") == "json":
        print_json(payload)
        return 0
    print(render_key_value([
        ("Dataset", payload["dataset"] or "all"),
        ("Datasets Scanned", payload["dataset_count"]),
        ("Samples Indexed", payload["sample_count"]),
    ]))
    return 0


def handle_output_list(args: argparse.Namespace) -> int:
    rows = list_outputs(args.config, dataset=args.dataset, runner=args.runner)
    limited_rows, total_rows = limit_rows(rows, args)
    if output_format(args, "table") == "json":
        print_json({"output_count": len(rows), "rows": limited_rows})
        return 0
    if not args.quiet:
        print(f"{len(rows)} output groups matched.")
        if limited_rows:
            print()
    if not limited_rows:
        print("No outputs matched.")
        return 0
    table_rows = [
        {
            "TARGET": row["target"],
            "RUNNER": row["runner"],
            "SAMPLES": row["samples"],
            "DATA TYPES": ",".join(row["data_types"]) or "-",
            "UPDATED": format_relative_time(row["updated_at"]),
        }
        for row in limited_rows
    ]
    print(render_table(["TARGET", "RUNNER", "SAMPLES", "DATA TYPES", "UPDATED"], table_rows))
    print_truncation_notice(len(limited_rows), total_rows, label="output groups")
    return 0


def handle_output_show(args: argparse.Namespace) -> int:
    payload = show_output(args.config, args.target)
    samples = payload["samples"]
    limited_samples, total_samples = limit_rows(samples, args)
    if output_format(args, "text") == "json":
        print_json({**payload, "samples": limited_samples})
        return 0
    print(render_key_value([
        ("Target", payload["target"]),
        ("Sample Count", payload["sample_count"]),
        ("Data Types", ", ".join(payload["data_types"]) or "-"),
    ]))
    if not limited_samples:
        print()
        print("No output samples matched.")
        return 0
    print()
    table_rows = [
        {
            "SAMPLE": row["sample"],
            "SOURCE JOB": row["source_job_id"],
            "DATA TYPES": ",".join(row["data_types"]) or "-",
            "UPDATED": format_relative_time(row["updated_at"]),
        }
        for row in limited_samples
    ]
    print(render_table(["SAMPLE", "SOURCE JOB", "DATA TYPES", "UPDATED"], table_rows))
    print_truncation_notice(len(limited_samples), total_samples, label="output samples")
    return 0


def handle_job_list(args: argparse.Namespace) -> int:
    payload = list_jobs(
        args.config,
        JobListOptions(
            job_ids=args.job_ids,
            dataset=args.dataset,
            runner=args.runner,
            states=args.states,
            view=args.view,
            sort=args.sort,
            desc=args.desc,
            limit=effective_row_limit(args),
            created_since=args.created_since,
            created_until=args.created_until,
            updated_since=args.updated_since,
            updated_until=args.updated_until,
            finished_since=args.finished_since,
            finished_until=args.finished_until,
            failed=args.failed,
            active=args.active,
            completed=args.completed,
        ),
    )
    if output_format(args, "table") == "json":
        print_json(payload)
        return 0
    summary = payload["summary"]
    matched_rows = summary.get("group_count", summary["job_count"]) if payload["view"] == "groups" else summary["job_count"]
    if not args.quiet:
        prefix = f"{summary.get('group_count', summary['job_count'])} {'groups' if payload['view'] == 'groups' else 'jobs'} matched."
        print(
            f"{prefix} {summary['job_count']} jobs total. "
            f"{summary['completed']} completed, {summary['pending']} pending, "
            f"{summary['failed']} failed, {summary['cancelled']} cancelled."
        )
        if payload["rows"]:
            print()
    if not payload["rows"]:
        print("No jobs matched.")
        return 0
    if payload["view"] == "groups":
        table_rows = [
            {
                "DATASET": row["dataset"],
                "RUNNER": row["runner"],
                "TOTAL": row["total"],
                "COMPLETED": row["completed"],
                "PENDING": row["pending"],
                "FAILED": row["failed"],
                "CANCELLED": row["cancelled"],
                "LAST UPDATE": format_relative_time(row["last_update"]),
            }
            for row in payload["rows"]
        ]
        print(render_table(["DATASET", "RUNNER", "TOTAL", "COMPLETED", "PENDING", "FAILED", "CANCELLED", "LAST UPDATE"], table_rows))
        print_truncation_notice(len(payload["rows"]), matched_rows, label="group rows")
        return 0
    table_rows = [
        {
            "JOB": row["job_ref"],
            "DATASET": row["dataset"],
            "SAMPLE": row["sample"],
            "RUNNER": row["runner"],
            "STATE": row["state"],
            "ATTEMPT": row["attempt"],
            "UPDATED": format_relative_time(row["updated_at"]),
        }
        for row in payload["rows"]
    ]
    print(render_table(["JOB", "DATASET", "SAMPLE", "RUNNER", "STATE", "ATTEMPT", "UPDATED"], table_rows))
    print_truncation_notice(len(payload["rows"]), matched_rows, label="job rows")
    return 0


def handle_job_add(args: argparse.Namespace) -> int:
    payload = add_job(
        args.config,
        dataset=args.dataset,
        candidate=args.candidate,
        runner=args.runner,
        references=args.references,
        settings=args.settings,
        timeout_minutes=args.timeout_minutes,
        source_job_id=args.source_job_id,
        allow_start_outside_window=args.allow_outside_window,
        batch_id=None,
        job_id=None,
    )
    if output_format(args, "text") == "json":
        print_json(payload)
        return 0
    print(render_key_value([
        ("Job Count", payload["job_count"]),
        ("Created", format_timestamp(payload["created_at"])),
        ("Dataset", payload["dataset"]),
        ("Dataset Version", payload["dataset_version"]),
        ("Runner", payload["runner"]),
        ("Job Type", payload["job_type"]),
        ("Timeout Minutes", round(payload["timeout_seconds"] / 60, 3)),
        ("Allow Outside Window", "yes" if payload["allow_start_outside_window"] else "no"),
    ]))
    if payload["parameters"]:
        print()
        parameter_rows = [
            {"KEY": key, "VALUE": value}
            for key, value in sorted(payload["parameters"].items())
        ]
        print(render_table(["KEY", "VALUE"], parameter_rows))
    if payload["groups"]:
        print()
        table_rows = [
            {
                "DATASET": row["dataset"],
                "RUNNER": row["runner"],
                "TOTAL": row["total"],
                "PENDING": row["pending"],
                "FAILED": row["failed"],
                "CANCELLED": row["cancelled"],
                "LAST UPDATE": format_relative_time(row["last_update"]),
            }
            for row in payload["groups"]
        ]
        print(render_table(["DATASET", "RUNNER", "TOTAL", "PENDING", "FAILED", "CANCELLED", "LAST UPDATE"], table_rows))
    if args.verbose and payload["job_rows"]:
        print()
        table_rows = [
            {
                "JOB": row["job_ref"],
                "SAMPLE": row["sample"],
                "STATE": row["state"],
            }
            for row in payload["job_rows"]
        ]
        print(render_table(["JOB", "SAMPLE", "STATE"], table_rows))
    return 0


def handle_job_update(args: argparse.Namespace) -> int:
    if args.allow_outside_window == args.disallow_outside_window:
        raise ValueError("choose exactly one of --allow-outside-window or --disallow-outside-window")
    payload = update_jobs_window_flag(
        args.config,
        job_ids=args.job_ids,
        dataset=args.dataset,
        runner=args.runner,
        allow=bool(args.allow_outside_window),
    )
    if output_format(args, "text") == "json":
        print_json(payload)
        return 0
    print(render_key_value([
        ("Matched Jobs", payload["matched"]),
        ("Updated Jobs", payload["updated"]),
        ("Allow Outside Window", "yes" if payload["allow_start_outside_window"] else "no"),
    ]))
    if payload["groups"]:
        print()
        table_rows = [
            {
                "DATASET": row["dataset"],
                "RUNNER": row["runner"],
                "TOTAL": row["total"],
                "COMPLETED": row["completed"],
                "PENDING": row["pending"],
                "FAILED": row["failed"],
                "CANCELLED": row["cancelled"],
                "LAST UPDATE": format_relative_time(row["last_update"]),
            }
            for row in payload["groups"]
        ]
        print(render_table(["DATASET", "RUNNER", "TOTAL", "COMPLETED", "PENDING", "FAILED", "CANCELLED", "LAST UPDATE"], table_rows))
    if args.verbose and payload["job_rows"]:
        print()
        print(render_table(["JOB"], [{"JOB": row["job_ref"]} for row in payload["job_rows"]]))
    return 0


def handle_job_cancel(args: argparse.Namespace) -> int:
    payload = cancel_jobs_matching_filters(
        args.config,
        job_ids=args.job_ids,
        dataset=args.dataset,
        runner=args.runner,
    )
    if output_format(args, "text") == "json":
        print_json(payload)
        return 0
    print(render_key_value([
        ("Matched Jobs", payload["matched"]),
        ("Cancelled Jobs", payload["cancelled"]),
        ("Skipped Jobs", payload["skipped"]),
    ]))
    if payload["groups"]:
        print()
        table_rows = [
            {
                "DATASET": row["dataset"],
                "RUNNER": row["runner"],
                "TOTAL": row["total"],
                "CANCELLED": row["cancelled"],
                "LAST UPDATE": format_relative_time(row["last_update"]),
            }
            for row in payload["groups"]
        ]
        print(render_table(["DATASET", "RUNNER", "TOTAL", "CANCELLED", "LAST UPDATE"], table_rows))
    if args.verbose and payload["job_rows"]:
        print()
        print(render_table(["JOB"], [{"JOB": row["job_ref"]} for row in payload["job_rows"]]))
    return 0


def handle_job_show(args: argparse.Namespace) -> int:
    payload = show_job(args.config, args.job_id)
    if output_format(args, "text") == "json":
        print_json(payload)
        return 0
    print(render_key_value([
        ("Job", payload["job_ref"]),
        ("State", payload["state"]),
        ("Job Type", payload["job_type"]),
        ("Runner", payload["runner"]),
        ("Dataset", payload["dataset"]),
        ("Dataset Version", payload["dataset_version"]),
        ("Sample", payload["sample"]),
        ("Attempt", payload["attempt"]),
        ("Source Job", payload["source_job_id"] or "-"),
        ("Created", format_timestamp(payload["created_at"])),
        ("Updated", format_timestamp(payload["updated_at"])),
        ("Completed", format_timestamp(payload["completed_at"])),
        ("Duration", format_duration(payload["created_at"], payload["completed_at"])),
        ("Output Dir", payload["output_dir"] or "-"),
        ("Failure Code", payload["failure_code"] or "-"),
        ("Failure Message", payload["failure_message"] or "-"),
        ("Allow Outside Window", "yes" if payload["allow_start_outside_window"] else "no"),
        ("Artifact Count", payload["artifact_count"]),
        ("Metric Count", payload["metric_count"]),
    ]))
    return 0


def handle_batch_list(args: argparse.Namespace) -> int:
    rows = list_batches(
        args.config,
        runner=args.runner,
        open_only=bool(args.open),
        closed_only=bool(args.closed),
    )
    limited_rows, total_rows = limit_rows(rows, args)
    if output_format(args, "table") == "json":
        print_json({"batch_count": len(rows), "rows": limited_rows})
        return 0
    if not args.quiet:
        print(f"{len(rows)} batches matched.")
        if limited_rows:
            print()
    if not limited_rows:
        print("No batches matched.")
        return 0
    table_rows = [
        {
            "BATCH": row["batch_id"],
            "RUNNER": row["runner"],
            "STATE": row["state"],
            "JOBS": row["job_count"],
            "PENDING": row["pending"],
            "COMPLETED": row["completed"],
            "FAILED": row["failed"],
            "CANCELLED": row["cancelled"],
            "UPDATED": format_relative_time(row["updated_at"]),
        }
        for row in limited_rows
    ]
    print(render_table(["BATCH", "RUNNER", "STATE", "JOBS", "PENDING", "COMPLETED", "FAILED", "CANCELLED", "UPDATED"], table_rows))
    print_truncation_notice(len(limited_rows), total_rows, label="batch rows")
    return 0


def handle_batch_show(args: argparse.Namespace) -> int:
    payload = show_batch(args.config, args.batch_id)
    if output_format(args, "text") == "json":
        print_json(payload)
        return 0
    print(render_key_value([
        ("Batch", payload["batch_id"]),
        ("State", payload["state"]),
        ("Runner", payload["runner"]),
        ("Runner Name", payload["runner_name"]),
        ("Runner Type", payload["runner_type"]),
        ("Runner Version", payload["runner_version"]),
        ("Runner Endpoint", payload["runner_endpoint"] or "-"),
        ("Job Count", payload["job_count"]),
        ("Pending", payload["pending"]),
        ("Completed", payload["completed"]),
        ("Failed", payload["failed"]),
        ("Cancelled", payload["cancelled"]),
        ("Created", format_timestamp(payload["created_at"])),
        ("Updated", format_timestamp(payload["updated_at"])),
        ("Closed", format_timestamp(payload["closed_at"])),
    ]))
    if payload["jobs"]:
        print()
        table_rows = [
            {
                "JOB": row["job_ref"],
                "DATASET": row["dataset"],
                "SAMPLE": row["sample"],
                "STATE": row["state"],
                "ATTEMPT": row["attempt"],
                "UPDATED": format_relative_time(row["updated_at"]),
            }
            for row in payload["jobs"]
        ]
        print(render_table(["JOB", "DATASET", "SAMPLE", "STATE", "ATTEMPT", "UPDATED"], table_rows))
    return 0


def handle_pipeline_list(args: argparse.Namespace) -> int:
    rows = list_pipelines(args.config)
    if output_format(args, "table") == "json":
        print_json(rows)
        return 0
    print(
        render_table(
            ["PIPELINE", "DATASET", "RUNNER", "LANES", "STAGES", "FILE"],
            [
                {
                    "PIPELINE": row["name"],
                    "DATASET": row["dataset"] or "-",
                    "RUNNER": row["runner"] or "-",
                    "LANES": row["matrix_lane_count"],
                    "STAGES": row["stage_count"],
                    "FILE": row["path"],
                }
                for row in rows
            ],
        )
    )
    return 0


def handle_pipeline_validate(args: argparse.Namespace) -> int:
    payload = validate_pipeline(
        args.config,
        name=args.pipeline_name,
        file_path=args.pipeline_file,
    )
    if output_format(args, "text") == "json":
        print_json(payload)
        return 0
    print(
        render_key_value(
            [
                ("Valid", "yes"),
                ("Pipeline", payload["name"]),
                ("File", payload["path"]),
                ("Dataset", payload["dataset"] or "-"),
                ("Runner", payload["runner"] or "-"),
                ("Matrix Lanes", payload["matrix_lane_count"]),
                ("Stages", ", ".join(payload["stages"])),
            ]
        )
    )
    return 0


def handle_pipeline_add(args: argparse.Namespace) -> int:
    payload = add_pipeline(
        args.config,
        name=args.pipeline_name,
        file_path=args.pipeline_file,
        input_overrides={"dataset": args.dataset, "runner": args.runner},
        matrix_values=args.matrix,
        allow_start_outside_window=args.allow_outside_window,
    )
    if output_format(args, "text") == "json":
        print_json(payload)
        return 0
    print(
        render_key_value(
            [
                ("Pipeline Run", payload["pipeline_run_id"]),
                ("Pipeline", payload["pipeline_name"]),
                ("State", payload["status"]),
                ("Dataset", payload["dataset_target"]),
                ("Runner", payload["runner"] or "-"),
                ("Matrix Lanes", len(payload.get("lanes_json") or [])),
                ("Created", format_timestamp(payload["created_at_utc"])),
            ]
        )
    )
    return 0


def handle_pipeline_runs(args: argparse.Namespace) -> int:
    rows = list_pipeline_runs(args.config)
    rows, total_rows = limit_rows(rows, args)
    if output_format(args, "table") == "json":
        print_json(rows)
        return 0
    print(
        render_table(
            ["RUN", "PIPELINE", "DATASET", "RUNNER", "STATE", "LANES", "UPDATED"],
            [
                {
                    "RUN": row["pipeline_run_id"],
                    "PIPELINE": row["pipeline_name"],
                    "DATASET": row["dataset_target"],
                    "RUNNER": row["runner"] or "-",
                    "STATE": row["status"],
                    "LANES": len(row.get("lanes_json") or []),
                    "UPDATED": format_relative_time(row["updated_at_utc"]),
                }
                for row in rows
            ],
        )
    )
    print_truncation_notice(len(rows), total_rows, label="pipeline runs")
    return 0


def handle_pipeline_show(args: argparse.Namespace) -> int:
    payload = show_pipeline_run(args.config, args.pipeline_run_id)
    if output_format(args, "text") == "json":
        print_json(payload)
        return 0
    print(
        render_key_value(
            [
                ("Pipeline Run", payload["pipeline_run_id"]),
                ("Pipeline", payload["pipeline_name"]),
                ("State", payload["status"]),
                ("Dataset", payload["dataset_target"]),
                ("Runner", payload["runner"] or "-"),
                ("Created", format_timestamp(payload["created_at_utc"])),
                ("Completed", format_timestamp(payload["completed_at_utc"])),
                ("Failure", payload.get("failure_message") or "-"),
            ]
        )
    )
    if payload["stages"]:
        print()
        print(
            render_table(
                ["STAGE", "LANE", "SAMPLE", "STATE", "JOB", "CHILD PIPELINE"],
                [
                    {
                        "STAGE": row["stage_id"],
                        "LANE": row["lane_index"],
                        "SAMPLE": (
                            "-"
                            if str(row.get("external_key") or "").startswith("__")
                            else row.get("external_key") or "-"
                        ),
                        "STATE": row["status"],
                        "JOB": row.get("job_id") or "-",
                        "CHILD PIPELINE": row.get("child_pipeline_run_id") or "-",
                    }
                    for row in payload["stages"]
                ],
            )
        )
    return 0


def handle_pipeline_cancel(args: argparse.Namespace) -> int:
    payload = cancel_pipeline(args.config, args.pipeline_run_id)
    if output_format(args, "text") == "json":
        print_json(payload)
        return 0
    print(
        render_key_value(
            [
                ("Pipeline Run", payload["pipeline_run_id"]),
                ("State", payload["status"]),
                ("Cancelled Jobs", payload["cancelled_jobs"]),
                (
                    "Cancelled Stage Executions",
                    payload["cancelled_stage_executions"],
                ),
            ]
        )
    )
    return 0


def dispatch_command(parser: argparse.ArgumentParser, args: argparse.Namespace) -> int:
    if args.command == "serve":
        run_service(args.host, args.port, args.config)
        return 0
    if args.command == "run":
        return handle_run(args)
    if args.command == "config":
        if args.config_command == "show":
            return handle_config_show(args)
        if args.config_command == "validate":
            return handle_config_validate(args)
        return handle_config_sources(args)
    if args.command == "runner":
        if args.runner_command == "list":
            return handle_runner_list(args)
        if args.runner_command == "show":
            return handle_runner_show(args)
        return handle_runner_status(args)
    if args.command == "dataset":
        if args.dataset_command == "list":
            return handle_dataset_list(args)
        if args.dataset_command == "download":
            return handle_dataset_download(args)
        if args.dataset_command == "rescan":
            return handle_dataset_rescan(args)
        return handle_dataset_show(args)
    if args.command == "batch":
        if args.batch_command == "list":
            return handle_batch_list(args)
        return handle_batch_show(args)
    if args.command == "output":
        if args.output_command == "list":
            return handle_output_list(args)
        return handle_output_show(args)
    if args.command == "pipeline":
        if args.pipeline_command == "list":
            return handle_pipeline_list(args)
        if args.pipeline_command == "validate":
            return handle_pipeline_validate(args)
        if args.pipeline_command == "add":
            return handle_pipeline_add(args)
        if args.pipeline_command == "runs":
            return handle_pipeline_runs(args)
        if args.pipeline_command == "show":
            return handle_pipeline_show(args)
        return handle_pipeline_cancel(args)
    if args.command == "job":
        if args.job_command == "add":
            return handle_job_add(args)
        if args.job_command == "list":
            return handle_job_list(args)
        if args.job_command == "cancel":
            return handle_job_cancel(args)
        if args.job_command == "update":
            return handle_job_update(args)
        return handle_job_show(args)
    parser.error(f"unsupported command {args.command!r}")
    return 2


def main(argv: list[str] | None = None) -> int:
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    run_command: list[str] = []
    if "run" in raw_argv:
        run_index = raw_argv.index("run")
        try:
            separator_index = raw_argv.index("--", run_index + 1)
        except ValueError:
            separator_index = -1
        if separator_index >= 0:
            run_command = raw_argv[separator_index + 1 :]
            del raw_argv[separator_index:]
    parser = build_parser()
    args = parser.parse_args(raw_argv)
    if args.command == "run":
        args.run_command = run_command
    configure_logging(service_mode=args.command == "serve")
    logger.info(
        event_message(
            "orchestrator_command_start",
            command=args.command,
            config_path=args.config or os.getenv("PATH_CONFIG_SYSTEM"),
            dispatch=bool(getattr(args, "dispatch", False)),
        )
    )
    try:
        exit_code = dispatch_command(parser, args)
        logger.info(event_message("orchestrator_command_finished", command=args.command, exit_code=exit_code))
        return exit_code
    except (FileNotFoundError, NotADirectoryError, RuntimeError, ValueError, TimeoutError) as exc:
        logger.error(event_message("command_failed", command=args.command, error=str(exc)))
        parser.error(str(exc))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
