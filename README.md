# func_adl_servicex

 Send func_adl expressions to a ServiceX endpoint

## Introduction

This package contains the single object `ServiceXDataset` which can be used as a root of a `func_adl` expression to query large LHC datasets from an active `ServiceX` instance located on the net.

For example, get get all the jet pt's above 30 GeV from an ATLAS xaod:

```python
example
```

And to fetch the same from a root tuple file:

```python
example
```

### Further Information

- `servicex` documentation
- `func_adl` documentation

## Development

PR's are welcome! Feel free to add an issue for new features or questions.

The `master` branch is the most recent commits that both pass all tests and are slated for the next release. Releases are tagged. Modifications to any released versions are made off those tags.

## Qastle

This is for people working with the back-ends that run in `servicex`.

This is the `qastle` produced for an xAOD dataset:

```text
(call EventDataset 'ServiceXDatasetSource')
```

(the actual dataset name is passed in the `servicex` web API call.)

This is the `qastle` produced for a ROOT flat file:

```text
(call EventDataset 'ServiceXDatasetSource' 'tree_name')
```
