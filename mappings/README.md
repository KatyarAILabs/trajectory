# Mappings

Two different things get called a mapping, and they live in different places.

**Convention mappings** — OpenInference, OTel GenAI — map a tracing
convention's span attributes onto the canonical step record. They are embedded
in the binary so the collector works with no external files, and they live with
the normaliser that reads them:

    collector/normalize/conventions/*.yaml

That directory is the single copy. An operator overrides one without waiting
for a release by pointing `sources[].mappings_dir` at a directory holding a file
with the same `name:`; it replaces the builtin wholesale.

**Gateway mappings** — LiteLLM, Portkey, Helicone — map a gateway's webhook
callback payload onto the same record. They belong in this directory, and are
Phase 4 work (F-1.3).

Both are data, not code (F-2.4): convention churn upstream is handled by editing
a file and pinning a conformance fixture against the version, not by
recompiling a parser.
