# Fuzz infrastructure

GCS buckets and Workload Identity for the fuzz CI jobs. Only the daily `fuzz-deep` schedule on `main` can write the corpus. CI can only create crash objects, and only project owners can read them.

```shell
terraform -chdir=scripts/ci/fuzz/infra init
terraform -chdir=scripts/ci/fuzz/infra apply -var fuzz_corpus_bucket=<name> -var fuzz_crashes_bucket=<name>
```

CI requires `FUZZ_CORPUS_BUCKET`, `FUZZ_CRASHES_BUCKET`, and `FUZZ_CORPUS_WIF_PROVIDER`.
