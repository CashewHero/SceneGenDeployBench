# Pipelines

A pipeline connects existing DeployBench runners and direct script containers. Runner stages create ordinary durable jobs; script stages use the same container launcher as `deploybench run`.

## Definition

Catalog definitions live in `config/pipelines/*.yaml`:

```yaml
pipeline_version: 1
name: test_pipeline
dataset: example_set1/city

stages:
  copy:
    runner: test_runner
    inputs:
      data: ${{ dataset }}

  evaluate:
    needs: copy
    runner: test_evaluator
    inputs:
      data: ${{ dataset }}
      candidate: ${{ stages.copy.outputs }}

  report:
    needs: evaluate
    scope: pipeline
    image: python:3.12-slim
    run: python -c 'import json; print(json.load(open("/workspace/pipeline.json")))'
```

Each stage uses exactly one execution form:

- `runner` selects a registered runner. `inputs` contains the `data`, `candidate`, and `references` roles; `with` contains job parameters.
- `image` plus `run` executes a direct script container. `run` can be a shell string or an exact argument list.

One stage can produce several stage executions across matrix lanes and samples. A runner-backed execution links to one ordinary job; a script execution runs directly without creating a job. `scope: matrix` is the default and runs the stage once per matrix lane; `scope: pipeline` runs it once for the pipeline. A pipeline-scoped dependency feeds every matrix lane, while a pipeline-scoped stage depending on a matrix stage waits for all lanes.

`needs` accepts one stage id or a list. A stage starts after all its dependencies finish. The default `if: success()` skips it after a failed dependency; `if: always()` still runs it. `timeout-minutes` defaults to 60.

## Matrix

Matrix axes apply to the whole pipeline and form a Cartesian product:

```yaml
matrix:
  sigma_px: [0, 1, 2, 4]
  seed: [0, 1]

stages:
  degrade:
    runner: image_degradation
    inputs:
      data: ${{ dataset }}
    with:
      degradation: gaussian_blur
      sigma_px: ${{ matrix.sigma_px }}
      seed: ${{ matrix.seed }}
```

Override declared axes from the CLI:

```bash
deploybench pipeline add experiment \
  --dataset tartan-test-15 \
  --matrix sigma_px=0,2,4 \
  --matrix seed=0,1
```

Supported expressions are:

```text
${{ dataset }}
${{ matrix.<name> }}
${{ stages.<stage>.outputs }}
```

A stage used through `stages.<stage>.outputs` must also appear in `needs`.

## Scripts

A script receives `/workspace/pipeline.json`:

```json
{
  "pipeline": {"run_id": "...", "name": "...", "dataset": "..."},
  "stage": {"id": "report", "scope": "pipeline", "lane_index": null, "matrix": {}},
  "needs": {"evaluate": [{"sample": "...", "lane_index": 0, "matrix": {"sigma_px": 2}, "status": "completed", "result": {}}]}
}
```

Script stages also accept `access`, `env`, `mounts`, and `workdir`, with the same meanings as `deploybench run`.

A script may return no result. Every new file it creates under `/workspace` is copied into its execution directory; `pipeline.json`, `result.json`, and files placed in the workspace by the orchestrator are excluded. Pipeline-scoped scripts and single-lane matrix scripts use `/data/pipelines/<pipeline>/<timestamp>/<stage>/`; matrix-scoped scripts with multiple lanes add `/<lane>/`. Empty directories are removed.

To expose structured metrics or make selected files addressable through `${{ stages.<stage>.outputs }}`, the script may also write a JSON object to the path in `$DEPLOYBENCH_RESULT`:

```json
{
  "output_files": {
    "summary": {
      "text": "report.txt"
    }
  },
  "metrics": []
}
```

`output_files` has the same `sample_id -> data_type -> path` shape as a runner result. Paths are relative to `/workspace` and matching paths in the stored result are rewritten to their persistent locations. The response does not control file persistence: missing, malformed, and incomplete responses do not prevent workspace files from being kept.

Every runner and script stage accepts `retention: keep`, `pipeline`, `matrix`, or `none`; the default is `keep`. `pipeline` deletes output when the pipeline finishes, `matrix` deletes it when the matrix lane finishes, and `none` deletes it as soon as the stage execution finishes. On a pipeline-scoped stage, `matrix` behaves as `pipeline`; output from a `none` stage cannot be used as a later stage input. Cleanup physically deletes runner files and their searchable output records, while script cleanup removes the complete execution directory without relying on the script response.

## Run

Use `deploybench pipeline add <name>` for a catalog pipeline or `deploybench pipeline add --file <path>` for another definition. See [CLI Pipelines](cli.md#pipelines) for the complete commands and options.

The scheduler checks ready stages during its normal poll. Pipeline state and runner jobs survive orchestrator restarts. Cancelling a pipeline cancels its unfinished jobs and active script container.
