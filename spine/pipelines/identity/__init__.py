"""The identity exemplar pipeline (LLD §12.2, architect D-2).

Ships as a real `pipelines.identity` module in the spine wheel (not under
`tests/exemplar/`) so I-10's `^pipelines\\.` importlib-namespace grammar holds
identically in tests and once deployed. `tests/exemplar/identity/` carries
only the deployed-shape `pipeline.yaml`, fixture CSV objects, and the
scenario tests that bind against this module.
"""
