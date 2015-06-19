How To Reindex Seamlessly and with Zero Downtime
========

Preface
--------
In the before time, long long ago, we had an index and it was good.
Sometimes we wanted to reindex and so spun up a whole new spandex stack, reindexed all the documents, and cutover.
This has some real monetary cost estimated at $1200, and a ton of engineering effort.

Now we have the spandex-http read (search) path pointing at this alias `spandex-r`,
and the secondary-watcher-spandex write (index) path pointing at this other alias `spandex-w`.

Start Reindexing
--------
Add a new index and mappings, with the appropriate settings from `reference.conf`.
```
PUT /spandex2
{...}

PUT /spandex2/dataset_copy/_mapping
{...}

PUT /spandex2/column_map/_mapping
{...}

PUT /spandex2/field_value/_mapping
{...}
```

Add the new index to the write (index) alias.
```
POST _aliases
{
  "actions": [
    {
      "add": {
        "index": "spandex2",
        "alias": "spandex-w"
      }
    }
  ]
}
```

Now secondary-watcher-spandex will transparently write any new documents in both indices.

Cutover
--------

See Also
--------
https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html
