# hipscat, hipscat-import, lsdb now packaged for conda-forge

## End-user demo

LSDB + hipscat modules/tools are now packaged for conda-forge. Here's how
a user would install `lsdb` into a new environment:

  `conda create -n lsdb -c conda-forge lsdb`

, with the [video available here](https://asciinema.org/a/9XT0tbyMEJDrDsvsIJSJEQalH). As 
all of them are pure python, they should work across all platforms/architectures
for which their prerequisites are available.

**NOTE:** This seems to work well, except that `mamba install` takes about 2 minutes to resolve
the dependencies; that seems slow and user unfriendly. Recommend to investigate at
some point.

## Developer info

The "recipes" for how to build these packages are in:

  * https://github.com/conda-forge/hipscat-feedstock
  * https://github.com/conda-forge/hipscat-import-feedstock
  * https://github.com/conda-forge/lsdb-feedstock

The recipes run all unit tests once they're built, so we have some confidence
that the packages work.

These recipes get automatic pull requests every time a new version of the underlying
package is tagged. Example:

  * https://github.com/conda-forge/hipscat-feedstock/pull/2

`conda-forge` release happens once this PR is merged.

**NOTE:** Right now I'm the only one with permissions to these repositories; who else
should be added as maintainer?

**NOTE:** As the three recipes are built/released independently, it's possible to
(say) break LSDB by releasing a version of hipscat which inadvertantly breaks API
compatibility. Our conda CI wouldn't detect this right now. Recommend to look into
how to automatically prevent such breakage.

## Resolves

* [issue 105](https://github.com/astronomy-commons/lsdb/issues/105)
* [issue 188](https://github.com/astronomy-commons/hipscat/issues/188)
* [issue 196](https://github.com/astronomy-commons/hipscat-import/issues/196)
