# How I want the pipeline built

  I've thought about the structure and I'd like it built the way
  I've laid out below.
  Please stick to this rather than reorganising it into
  something "cleaner" — I want it to
  match my own style of work, but shape may not as elegant as others.

  ## Layers and schemas

  Three layers, and I want each layer split into two schemas so
  every stage is explicit
  and separately queryable:

  - Bronze → `raw` and `live`
  - Silver → `core` and `shape`
  - Gold   → `data_mart` and `curated`

  Create all six schemas up front.

  ## Bronze

  - In `raw`, load the transactional sheets **broken out by
  month** — a separate table per
    month (July, August, September, October, November, December
  as their own tables). Each month may huge amount of data, and 
  it will be easier to query small table, rather than huge for a specific month.
  If there is an isse in one of the months, I can fix an issue in just one small table, and 
  it will push through changes to consolidated table
  - In `live`, bring it all back together: consolidate
  everything into one `deposits` table and one `withdrawals` table.
  In most cases we still need all data in one place.

  ## Silver

  - `core` - here most of heavy lifting is done, such as first layer of unpicking the json for groups 
  and clients, and applying FX rates are added to transactional data
  - `shape` this is where remaining clean up happens, some remaining json unpicked, and FX rates applied to 
  actual values to get GBP normalisations

  ## Gold

  - `data_mart` holds the modelled, aggregated network data - so we can combine some client and group tables, 
  but not yet combine with transactional data. I want to add a source column for data, so its very clear how row is put together.
  It should combine all sources used to create the row, so its very clear where data came from
  - `curated` is the final product reporting reads from —
  `data_mart` feeds `curated`,
    not the other way round.
  - The end result has to drive the relationship-network view:
  directed money flow between
    a focal group and its counterparts, in GBP, sliceable by
  month and year, and able to
    drill up and down the group/company hierarchy.
  Counterparties that belong to a group
    should sit under that group; ones that don't stand on their
  own.

  