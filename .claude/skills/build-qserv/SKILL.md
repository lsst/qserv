---
name: build-qserv
description: Build Qserv C++/Python in its containerized build environment, run unit tests, clang-format, mypy, and build docs. Use for any compile/test/format task in the qserv repo.
---

# Building and unit-testing Qserv

All compilation happens inside Docker containers driven by the host-side `./bin/qserv`
CLI (Python 3 + `click pyyaml requests`, Docker required). Never attempt a bare-metal
cmake build; system deps only exist in the build image. Image names derive from
`git describe`, so a dirty tree changes tags — that is normal.

## One-time setup

```sh
git submodule update --init          # extern/sphgeom, extern/log, extern/hyrise-sql-parser
./bin/qserv env                      # show image names + env var overrides (QSERV_*)
./bin/qserv build-user-build-image   # make the per-user build image (fast; wraps GHCR build image)
```

If the base/build images are missing locally they are pulled from GHCR
(`ghcr.io/lsst/qserv-build-base:...`); only run `./bin/qserv build-images` if the
Dockerfiles under `deploy/docker/` changed.

## The standard build + unit test cycle

```sh
./bin/qserv build -j8 --no-build-image        # cmake (if needed) + make install + unit tests
./bin/qserv build -j8                         # same, then also packages the qserv run image
```

Useful toggles: `--no-unit-test`, `--no-mypy`, `--cmake/--no-cmake` (force/skip cmake),
`--clang-format CHECK|REFORMAT|OFF` (CI uses CHECK; use REFORMAT to fix formatting),
`--dry` to print the docker commands without running.

Build artifacts land in `build/` at the repo root; `rm -rf build/` is the fix for
mysterious cmake-state errors.

## Fast iteration

```sh
./bin/qserv run-build        # interactive shell in the build container, repo mounted
# then inside:
make -j8 install             # incremental
make -j8 install test ARGS=-j8   # with unit tests (CTest)
ctest -R testQueryAna -j4    # run a single test suite from the build dir
```

Unit tests are Boost.Test binaries registered with CTest (`test*.cc` in each
`src/<module>/`). To add one, follow the `add_executable`/`add_test` pattern in the
module's `CMakeLists.txt`.

## Lint / format only

```sh
./bin/qserv build --no-make --no-mypy --clang-format REFORMAT   # just reformat C++
ruff check python/ bin/       # if ruff available on host; config in pyproject.toml (line length 110)
```

## Docs

```sh
tox -e docs                  # sphinx via documenteer; warnings are errors
# or: ./bin/qserv build-docs
```

Output: `build/doc/html`. Note `doc/architecture/*.md` is intentionally excluded from
the sphinx build (see `doc/conf.py` exclude_patterns).
