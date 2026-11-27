# Models

Trained models go here, one directory per model or straight in this one
(model.onnx, model.json, model.txt). They are not committed. To make one:

```bash
cargo run --release -p detektive-graph -- data/raw/HI-Small_Trans.csv --batch 4096 --features graph.csv
cd ml && python -m detektive_ml.models.export ../data/processed/hi_small.parquet ../graph.csv ../models
```

See [docs/scoring.md](../docs/scoring.md).
