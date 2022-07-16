# alblogs

Scans AWS ALB Access Logs, filtering entries for specific criteria. `-f, --filter` and `-s, --sink` options may be added multiple times. For `-f, --filter` an OR expression can be created by using a JSON array containing each filter that should be OR-ed together. AND expressions can be created by specifying two or more `-f, --filter` options.

ALB Log sources can remain in their gzipped state. Files can be globbed or a specific folder can be read in it's entirely.

```console
python3 -m cli "{SOURCE OF FILES}" --filter "{\"column_name\": \"user_agent\", \"equals\": \"{USER AGENT}\"}" --sink "{\"columns\": [], \"target_csv_file\": \"{OUTPUT FILE LOCATION}"}"
```

## Filters

Filters are specifed using JSON strings. The JSON object should match the Python __init__ method signature for the filter. A `column_name` key can be used instead of a `column_number`. At run time, `column_name` will be resolved to a `column_number`.

### FilterBetween

Include entries falling between two points, inclusive or exclusive.

### FilterEquals

Include entries that equal exactly the comparison value. Case folding is optional.

### FilterNotEquals

Include entries that do not equal the comparison value. Case folding is optional.

### FilterStartsWith

Include entries that start with the comparison value. Case folding is optional.
