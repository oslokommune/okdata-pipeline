# lambda-invoker

Pipeline component for invoking other Lambda functions. Typically used in edge cases.

## Input event format

Example Lambda event input for this function:

```json
{
    "input": { "befolkning-zxy": "s3/key/to/file.csv" },
    "output": "s3/key/or/prefix",
    "config": {
      "arn" : "arn:some:arn:to:a:function",
      "lambda_config": { "type": "status" }
    }
}
```

Will invoke another function with the following payload:

```json
{
    "input": { "befolkning-zxy": "s3/key/to/file.csv" },
    "output": "s3/key/or/prefix",
    "config": { "type": "status" }
}
```
