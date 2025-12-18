Remember to record!!

## Derek

LSDB in TOPCAT via TAP server. How to use it:

1. Launch topcat
2. From the application menu, choose VO > Table Access Protocol (TAP)
3. This brings up a dialog with a list of TAP services. At the bottom
   is a section labeld, "Selected TAP Service", and in there is a
   place to enter a "TAP URL".
4. Into that "TAP URL" enter "http://epyc.astro.washington.edu:43213"
   and press the "Use Service" button. This will change the tab
   of the window in use so that "Use Service" is selected.
   At the bottom of this window is a place to enter ADQL text.
5. Into that window, enter an ADQL query, like the one below.
6. Make sure that the Query Mode is "Synchronous".
7. Press the "Run Query" button at the bottom. This pops up another
   window titled "TOPCAT" and which appears to have no results in it,
   other than "TAP_1_gaia_dr3.gaia". But the results are available;
   you just have to choose an icon on from the ribbon at the top
   (or its menu equivalent) to display them.
   a. View > Table Data will show your query in a spreadsheet-like format.
   b. Graphics > Sky Plot will show your query in a 2-D format.
   c. Graphics > Cube Plot will show a 3-D representation.

```sql
SELECT TOP 15
    source_id, ra, dec, phot_g_mean_mag, phot_variable_flag
FROM gaia_dr3.gaia
WHERE 1 = CONTAINS(
    POINT('ICRS', ra, dec),
    CIRCLE('ICRS', 270.0, 23.0, 0.25)
)
AND phot_g_mean_mag < 16
AND phot_variable_flag = 'VARIABLE'
```

## Wilson

## Olivia

## Doug

## Melissa

nested light curve import

## Sean

## Kostya

- [Epoch propagation NB](https://docs.lsdb.io/en/latest/tutorials/pre_executed/dp1-gaia-epoch-prop.html)

# Seeking feedback

## Sandro

- [Estimate catalog size](estimate_size.ipynb)

## Kostya

- Generate `README.md` for catalogs? https://github.com/astronomy-commons/hats/issues/615
