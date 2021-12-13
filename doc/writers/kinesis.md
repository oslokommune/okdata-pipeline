# writers.kinesis

Pipeline component for writing to kinesis streams with prefix `dataplatform.dataset`.

## Input event

The input event contains the input location and output destination. Input can
either be of type "S3" and contain an S3 key or it can be of type "INLINE" and
contain the JSON object directly.

Example Lambda event input:

```json
{
  "input": {
    "type": "S3",
    "value": "input_s3_key.json"
  },
  "output": "dataplatform.dataset.green.some-dataset-id.some-version.json",
}
```

The input event object takes the following key/values:

| Key               | Type      | Description                                       | Default value                     |
| ----------------- | --------- | ------------------------------------------------- | --------------------------------- |
| input             | Object    | Type('type') and location('value') for input json | <no default, must be supplied>    |
| output            | Object    | Destination stream name                           | <no default, must be supplied>    |
