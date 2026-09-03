# Phase 2 deterministic parity fixture

`src/test/visual-parity-v1.json` is the reproducible visual checkpoint. It binds the Rust V1 golden manifest/layout hash to exact sparse-overlay coordinates at two viewport sizes and to the manifest-driven hybrid-axis ticks. Tests also lock the generic raster/overlay/label/table composition and the opt-in raster filter.

No checked-in AxAoU Manhattan raster or screenshot golden exists at the mapped reference revision, so pixel-diff screenshot parity cannot be honestly claimed. The fixture instead verifies the geometry that must line up with the server raster and the reusable visual behaviors extracted from AxAoU. Product-specific AxAoU recoloring remains opt-in through `--association-raster-filter`.
