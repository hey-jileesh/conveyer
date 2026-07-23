"""Local-stack fixtures — LLD §12.5.

TODO(M2): provide the central `local_effects -> Effects` fixture: the same
record shape production uses, assembled from moto clients, a SqlCatalog ledger
(bootstrap-created via create_ledger.py), an in-memory sftp dict, a
controllable now(), and a seeded deterministic delivery-id sequence.
No mocking framework anywhere (linter-banned, LLD §12.2).
"""
