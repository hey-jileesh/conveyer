"""The identity exemplar's violations-variant pipeline (LLD §12.2, R-04).

Same `apply` as `pipelines.identity` (imported, not duplicated); `post_check`
flags candidate rows instead of admitting everything, so R-04's fixture can
exercise the post_check quarantine path end-to-end. A separate `pipelines.`
module rather than a fixture-driven branch inside `pipelines.identity`
itself: `transforms_module` is a `PipelineSpecModel` field (spec-driven), so
two modules bound by two specs is the natural shape — no runtime branching
on fixture content inside one pipeline's transforms.
"""
