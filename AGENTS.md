# Repository Guidelines

## Project Structure & Module Organization
`src/index.ts` is the entrypoint (discovery plus `runFull`/`runIncremental` orchestration). Reusable logic lives under `src/lib/`:
`config.ts` parses and validates CLI arguments, `csv.ts` maps swap records to rows, `csvSink.ts` handles streaming CSV output, `storageTarget.ts` abstracts the storage backend (local or S3: state, sinks, manifest, run lock), `logger.ts` emits structured JSON logs, `retry.ts` provides bounded backoff, and `state.ts` manages resume state. Canister interfaces live in `src/idl/`. Tests live in `tests/`. Infrastructure is split into `terraform/aws-storage/` (persistent S3 bucket with lifecycle rules) and `terraform/aws-compute/` (ECR, ECS/Fargate, IAM, CloudWatch, scheduler, failure alerting). CI lives in `.github/workflows/ci.yml`. The operational runbook (local runs, deployment, day-2 operations) is `docs/RUNBOOK.md`. Local output is written to `out/` at runtime and must stay untracked.

## Build, Test, and Development Commands
Install dependencies with `npm install`.

- `npm run dev -- --mode full --page-size 1000 --concurrency 5`: run the ETL directly with `tsx`.
- `npm run build`: compile TypeScript from `src/` to `dist/`.
- `npm start -- --mode incremental --overlap 50`: run the compiled build.
- `npm test`: run the `node:test` suites in `tests/`.
- `terraform fmt -check` must pass for `terraform/aws-storage` and `terraform/aws-compute` (CI enforces it).
- `scripts/aws_build_push_and_run.sh --mode incremental`: build, push to ECR, and trigger a one-off Fargate run from Terraform outputs.
- `cd terraform/aws-storage && terraform init && terraform apply`: provision the persistent S3 bucket and bucket policies.
- `cd terraform/aws-compute && terraform init && terraform apply`: provision ECR, IAM roles, ECS/Fargate, and the optional scheduler.

## Coding Style & Naming Conventions
The codebase is strict TypeScript with ES modules and NodeNext resolution. Follow the existing style: 2-space indentation, semicolons, single quotes, and explicit types for exported helpers. Prefer `camelCase` for variables/functions, `PascalCase` for types/classes, and short descriptive filenames like `csvSink.ts`. Keep CLI argument names kebab-cased, for example `--state-file`.

## Testing Guidelines
Tests use Node's built-in `node:test` runner via `tsx` (no extra dependencies) and live in `tests/`, named after the target module (`state.test.ts`). For changes, run `npm test` and `npm run build` at minimum, and exercise the affected mode with `npm run dev -- --mode ...`. Pure logic (parsing, mapping, state) should be covered by unit tests; network and S3 interaction is verified by running the pipeline.

## Commit & Pull Request Guidelines
Use short imperative commit subjects, with a conventional type/scope when it improves clarity (`refactor: ...`, `chore: ...`). PRs should describe the ETL behavior changed, list the validation performed, and include sample commands or output paths when the change affects runtime behavior, storage layout, or Terraform.

## Security & Configuration Tips
Do not commit `out/`, Terraform state, credentials, or filled-in `terraform.tfvars`. Use the `terraform.tfvars.example` files as starting points, and prefer environment variables (`S3_BUCKET`, `S3_PREFIX`) for deployment-specific configuration. The Fargate task role is scoped to its S3 prefix — keep new IAM grants equally narrow.
