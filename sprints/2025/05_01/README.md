# Reminders

- Do you need to record this session?
- Keep an eye on the time
- Hold discussion topics for later slots

# View-only Demos

## Olivia [5 minutes]

## Kostya [5 minutes]

## Wilson [5 minutes]

## Sean [5 minutes]

## Melissa [5 minutes]

## Derek [5 minutes]

Originally intended to show off left-join, but it won the battle! So
instead I'm showing some data thumbnail work and recommendations.

[Generating data thumbnail local to
catalog](local_data_thumbnail.ipynb): ~5m

[Generating data thumbnail over HTTPS](remote_data_thumbnail.ipynb):
~40m (LSST to epyc), but took all afternoon because of the maddening
resource tuning, which is a low-level crisis. On the upside, having
the data thumbnail already would save at least 40m!

I think it should be a stage in the HATS import pipeline, next, but
certainly for sanity's sake, it ought to be done while the partitions
are being created.

Would love to have a progress bar, and could, if this could be managed
with `.map_partitions`. Maybe by using `.to_delayed`? Ideas?

## Sandro [5 minutes]

[Customizing row-groups](./row_group_splitting/row_group_splitting.ipynb) according to two splitting strategies (_num_rows_ and _subtile_order_delta_).

## Doug [5 minutes] - [Toy Catalog Generation](./catalog_generation.ipynb)

# Input Requested
