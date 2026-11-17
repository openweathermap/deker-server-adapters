# Changelog

All notable changes to deker-server-adapters, the HTTP client adapters of [DEKER™](https://github.com/openweathermap/deker)
for deker-server, are listed here, release by release, newest first. Only production releases are
listed. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), the versions
follow [Semantic Versioning](https://semver.org/).

## [2.0.0]

Released together with DEKER™ 2.0.0; requires `deker>=2,<3` and `deker-tools>=2,<3`.

### Breaking changes

- `create()` of the array adapters takes an `Array` or a `VArray` and returns it. The dict form
  moved to `create_from_dict()`, which warns about its deprecation; `create(dict)` still delegates
  to it, so the old code keeps working until 3.0.0.
- `get_id_by_primary_attributes(primary_attributes, schema)` of the array adapters, required by the
  adapter interface of DEKER™ 2.0.0: the id of an array by the same request as the whole array.
- `get_by_primary_attributes` and `get_id_by_primary_attributes` raise `DekerFilterError` before any request
  unless the attributes are exactly the primary attributes of the schema.
- The data of an update are validated and converted before they are sent, by the rules of the
  local write in DEKER™ 2.0.0: Python numbers, lists and tuples are converted to the dtype of the
  collection when every value fits, NumPy arrays and scalars must match the dtype exactly, a
  number fills the whole region. A wrong value raises `DekerValidationError` on the client instead
  of a server error or a silent write.
- The hash ring raises `HashRingError` when a key finds no node, instead of returning `None`; the
  ring is generic over the node type.
- The adapter interfaces are generic over the path and the array type (`deker` 2.0.0); the
  methods of a closed context raise `DekerAdapterClosedError`.

### Changed

- Reading an array streams the response into a NumPy array of the expected shape, chunk by
  chunk: the peak memory is the array plus one chunk, not two copies of the data. The length of
  the response is checked against the shape of the slice. A failed response is still read at once,
  so the errors and the switch between the nodes work as before.
- The slice of a request is serialised by `deker_tools.slices.slices_to_str` instead of a private
  helper.
- The hash of a key is computed from the bytes directly, with the same result; a failed request is
  logged once instead of being printed to stderr and logged.
- Clear error messages for a server response that is a dict and for an unread stream; an unused
  node traversal method is removed; the test of the client iteration that was marked as expected
  to fail runs again.

### Internals and tooling

- The messages of the log are formatted only when they are written.
- The build moved from poetry to uv, ruff and pydoclint replace black, flake8, isort and darglint;
  `make ci`, `make test-matrix`, `make install-matrix`; `py.typed`, no type-check suppressions;
  100% test coverage required; tests in random order.

## [1.2.0] - 2026-11-11

### Added

- Python 3.13 and 3.14 are supported.
- NumPy 2 is supported (`numpy>=1.26,<3`).

### Changed

- `httpx>=0.28.1,<1` (was `^0.27`), which brings `h11` 0.16 with the fix of CVE-2025-43859; the
  other dependency ranges are widened to the current majors; the dev and docs tooling is updated.
- `pytest-httpx` is a dev dependency, not a runtime one.
- CI runs the tests on every supported Python.

### Removed

- Python 3.9. Pin `deker-server-adapters<1.2` to stay on it.

## [1.0.9] - 2025-12-11

### Added

- The cluster config carries the storage of a node and the cluster status.
- The `Last-Modified` header of the cluster config is kept and sent with the requests, so a node
  can tell a stale client.

### Changed

- The data are sent as `application/octet-stream`.

### Fixed

- The plugins of the httpx client; a circular import in the typing.

## [1.0.8] - 2024-07-05

### Fixed

- The array read from a response was not writable: the buffer is copied into a `bytearray`.

## [1.0.7] - 2024-07-03

### Added

- The cluster configuration is read from the server at start: the nodes with their ids, hosts and
  ports, the leader and the mode. The leader is chosen by the config, not by the first node.
- A single-server entrypoint: the same adapter works with one server and with a cluster.

### Changed

- The status of a response is checked on create, update and delete, and `DekerServerError` names
  the failed operation.
- `httpx` updated and its deprecations fixed.

### Fixed

- `get()` of a missing array returns `None` instead of failing.

## [1.0.6] - 2024-05-17

### Fixed

- `create()` failed on a response without an id; it raises `DekerServerError` with a clear
  message now.

## [1.0.5] - 2024-04-16

### Added

- `HashRingError`; the hash ring is refactored, and the hash key of an array is computed from its
  primary attributes in one place.
- The cluster mode is detected by the healthcheck of the server and configures the context.

## [1.0.4] - 2024-03-06

### Added

- A hash ring chooses the node for an array in the cluster mode; the node for a request is picked
  at random among the nodes.

### Fixed

- Two different arrays could get the same id: the ids come from `deker.tools.array.get_id`.
- Listing the collections of a single server asked the cluster nodes.

## [1.0.3] - 2023-11-21

### Fixed

- Reading a single number from a response failed on the truth value of a NumPy array.

## [1.0.2] - 2023-11-20

### Added

- The cluster mode: a URI with several servers, requests are retried on the other nodes.

## [1.0.1] - 2023-08-31

### Changed

- HTTP/1 is the default protocol version.
- `deker` version requirement updated.

## [1.0.0] - 2023-08-18

The pilot release of deker-server-adapters.

[2.0.0]: https://github.com/openweathermap/deker-server-adapters/compare/v1.2.0...v2.0.0
[1.2.0]: https://github.com/openweathermap/deker-server-adapters/compare/v1.0.9...v1.2.0
[1.0.9]: https://github.com/openweathermap/deker-server-adapters/compare/v1.0.8...v1.0.9
[1.0.8]: https://github.com/openweathermap/deker-server-adapters/compare/v1.0.7...v1.0.8
[1.0.7]: https://github.com/openweathermap/deker-server-adapters/compare/v1.0.6...v1.0.7
[1.0.6]: https://github.com/openweathermap/deker-server-adapters/compare/v1.0.5...v1.0.6
[1.0.5]: https://github.com/openweathermap/deker-server-adapters/compare/v1.0.4...v1.0.5
[1.0.4]: https://github.com/openweathermap/deker-server-adapters/compare/v1.0.3...v1.0.4
[1.0.3]: https://github.com/openweathermap/deker-server-adapters/compare/v1.0.2...v1.0.3
[1.0.2]: https://github.com/openweathermap/deker-server-adapters/compare/v1.0.1...v1.0.2
[1.0.1]: https://github.com/openweathermap/deker-server-adapters/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/openweathermap/deker-server-adapters/releases/tag/v1.0.0
