# HATS/LSDB releases and changes to the RSP

All package releases should happen via a release tracker ticket.

This helps to associate all work with a single release, and also provides
a checklist that should be followed (after many previous instances of forgetting something).

Just added another section, copied below here:

### tie it together

once releases have been confirmed on conda-forge, confirm that the RSP
environment and notebooks will not be impacted:

- [ ] run the [smoke-test-conda workflow](https://github.com/astronomy-commons/lsdb-rubin/actions/workflows/smoke_test_conda.yml)
  in the `lsdb-rubin` repo.
- [ ] wait until it passes
- [ ] Inspect the `Get dependency changes from installing LSDB` stage and make sure there aren't
  "too many" new packages installed

### What? Why?

The test is very Rubin-specific, and so has been placed in the `lsdb-rubin` package.
This workflow does a conda install of the RSP environment, then installs `lsdb` on top.

Unfortunately, this step runs *after* the release, so if we're breaking RSP, there's a 
risk that the changes will be picked up / noticed by RSP. But it gives us some warning.

We can look at the results of this workflow together:

https://github.com/astronomy-commons/lsdb-rubin/actions/runs/17999546487/job/51205527996

