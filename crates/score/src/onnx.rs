//! Reads the trees out of an ONNX file.
//!
//! Only the parts of the ONNX format a tree ensemble needs are declared here,
//! with the field numbers from `onnx.proto`, so no protobuf compiler is
//! needed. The model must hold one `TreeEnsembleClassifier` node (from the
//! `ai.onnx.ml` domain) for a binary label. That is what `onnxmltools` writes
//! for a LightGBM or XGBoost classifier. Anything else is refused rather than
//! half understood.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result, anyhow, bail, ensure};
use prost::Message;

use crate::forest::{Forest, Mode, Node, PostTransform};

#[derive(Clone, PartialEq, Message)]
struct ModelProto {
    #[prost(int64, tag = "1")]
    ir_version: i64,
    #[prost(string, tag = "2")]
    producer_name: String,
    #[prost(message, optional, tag = "7")]
    graph: Option<GraphProto>,
    #[prost(message, repeated, tag = "8")]
    opset_import: Vec<OperatorSetIdProto>,
}

#[derive(Clone, PartialEq, Message)]
struct OperatorSetIdProto {
    #[prost(string, tag = "1")]
    domain: String,
    #[prost(int64, tag = "2")]
    version: i64,
}

#[derive(Clone, PartialEq, Message)]
struct GraphProto {
    #[prost(message, repeated, tag = "1")]
    node: Vec<NodeProto>,
    #[prost(message, repeated, tag = "5")]
    initializer: Vec<TensorProto>,
}

#[derive(Clone, PartialEq, Message)]
struct NodeProto {
    #[prost(string, repeated, tag = "1")]
    input: Vec<String>,
    #[prost(string, repeated, tag = "2")]
    output: Vec<String>,
    #[prost(string, tag = "4")]
    op_type: String,
    #[prost(message, repeated, tag = "5")]
    attribute: Vec<AttributeProto>,
    #[prost(string, tag = "7")]
    domain: String,
}

#[derive(Clone, PartialEq, Message)]
struct AttributeProto {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(bytes = "vec", tag = "4")]
    s: Vec<u8>,
    #[prost(message, optional, tag = "5")]
    t: Option<TensorProto>,
    #[prost(float, repeated, tag = "7")]
    floats: Vec<f32>,
    #[prost(int64, repeated, tag = "8")]
    ints: Vec<i64>,
    #[prost(bytes = "vec", repeated, tag = "9")]
    strings: Vec<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
struct TensorProto {
    #[prost(int32, tag = "2")]
    data_type: i32,
    #[prost(float, repeated, tag = "4")]
    float_data: Vec<f32>,
    #[prost(bytes = "vec", tag = "9")]
    raw_data: Vec<u8>,
    #[prost(string, tag = "8")]
    name: String,
    #[prost(double, repeated, tag = "10")]
    double_data: Vec<f64>,
}

// TensorProto.DataType values
const FLOAT: i32 = 1;
const DOUBLE: i32 = 11;

impl TensorProto {
    fn values(&self) -> Result<Vec<f64>> {
        Ok(match self.data_type {
            FLOAT if !self.raw_data.is_empty() => self
                .raw_data
                .chunks_exact(4)
                .map(|b| f64::from(f32::from_le_bytes([b[0], b[1], b[2], b[3]])))
                .collect(),
            FLOAT => self.float_data.iter().map(|&v| f64::from(v)).collect(),
            DOUBLE if !self.raw_data.is_empty() => self
                .raw_data
                .chunks_exact(8)
                .map(|b| f64::from_le_bytes(b.try_into().expect("8 bytes")))
                .collect(),
            DOUBLE => self.double_data.clone(),
            other => bail!("tensor of data type {other} where numbers were expected"),
        })
    }
}

pub fn read(path: &Path) -> Result<Forest> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    decode(&bytes).with_context(|| format!("reading the trees in {}", path.display()))
}

pub fn decode(bytes: &[u8]) -> Result<Forest> {
    let model = ModelProto::decode(bytes).context("not an ONNX model")?;
    let graph = model.graph.ok_or_else(|| anyhow!("model has no graph"))?;
    let mut ensembles = graph
        .node
        .iter()
        .filter(|n| n.op_type == "TreeEnsembleClassifier" && n.domain == "ai.onnx.ml");
    let node = ensembles
        .next()
        .ok_or_else(|| anyhow!("no TreeEnsembleClassifier in the model"))?;
    ensure!(
        ensembles.next().is_none(),
        "more than one tree ensemble in the model"
    );
    check_wrapping(&graph)?;
    Attributes::of(node).forest()
}

/// onnxmltools wraps the ensemble in a few bookkeeping nodes: Identity, a Cast
/// of the label and a Mul of the probabilities by a constant 1. Anything else
/// would change the scores, so it is refused.
fn check_wrapping(graph: &GraphProto) -> Result<()> {
    for node in &graph.node {
        match node.op_type.as_str() {
            "TreeEnsembleClassifier" | "Identity" | "Cast" => {}
            "Mul" => {
                for input in &node.input {
                    if let Some(t) = graph.initializer.iter().find(|t| &t.name == input) {
                        ensure!(
                            t.values()?.iter().all(|&v| v == 1.0),
                            "the model multiplies its output by {input}, which is not 1"
                        );
                    }
                }
            }
            other => bail!("the model has a {other} node; only a plain tree ensemble is supported"),
        }
    }
    Ok(())
}

/// The classifier's attributes by name.
struct Attributes<'a>(HashMap<&'a str, &'a AttributeProto>);

impl<'a> Attributes<'a> {
    fn of(node: &'a NodeProto) -> Self {
        Self(
            node.attribute
                .iter()
                .map(|a| (a.name.as_str(), a))
                .collect(),
        )
    }

    fn ints(&self, name: &str) -> Result<&'a [i64]> {
        self.0
            .get(name)
            .map(|a| a.ints.as_slice())
            .ok_or_else(|| anyhow!("missing attribute {name}"))
    }

    /// Ints that may be left out, meaning all zero.
    fn ints_or_empty(&self, name: &str) -> &'a [i64] {
        self.0.get(name).map_or(&[], |a| a.ints.as_slice())
    }

    /// A float list, or its `_as_tensor` form (ai.onnx.ml opset 3).
    fn numbers(&self, name: &str) -> Result<Vec<f64>> {
        if let Some(a) = self.0.get(format!("{name}_as_tensor").as_str())
            && let Some(t) = &a.t
        {
            return t.values();
        }
        self.0
            .get(name)
            .map(|a| a.floats.iter().map(|&v| f64::from(v)).collect())
            .ok_or_else(|| anyhow!("missing attribute {name}"))
    }

    fn strings(&self, name: &str) -> Result<Vec<String>> {
        let a = self
            .0
            .get(name)
            .ok_or_else(|| anyhow!("missing attribute {name}"))?;
        Ok(a.strings
            .iter()
            .map(|s| String::from_utf8_lossy(s).into_owned())
            .collect())
    }

    fn string(&self, name: &str) -> Option<String> {
        self.0
            .get(name)
            .map(|a| String::from_utf8_lossy(&a.s).into_owned())
    }

    fn forest(&self) -> Result<Forest> {
        let tree_ids = self.ints("nodes_treeids")?;
        let node_ids = self.ints("nodes_nodeids")?;
        let feature_ids = self.ints("nodes_featureids")?;
        let values = self.numbers("nodes_values")?;
        let modes = self.strings("nodes_modes")?;
        let yes = self.ints("nodes_truenodeids")?;
        let no = self.ints("nodes_falsenodeids")?;
        let missing = self.ints_or_empty("nodes_missing_value_tracks_true");
        let n = tree_ids.len();
        for (name, len) in [
            ("nodes_nodeids", node_ids.len()),
            ("nodes_featureids", feature_ids.len()),
            ("nodes_values", values.len()),
            ("nodes_modes", modes.len()),
            ("nodes_truenodeids", yes.len()),
            ("nodes_falsenodeids", no.len()),
        ] {
            ensure!(len == n, "{name} has {len} entries, nodes_treeids has {n}");
        }
        ensure!(
            missing.is_empty() || missing.len() == n,
            "nodes_missing_value_tracks_true has the wrong length"
        );

        let labels = self.ints_or_empty("classlabels_int64s");
        ensure!(
            labels == [0, 1],
            "only binary classifiers with labels 0 and 1 are supported, found {labels:?}"
        );
        let post = match self.string("post_transform").as_deref() {
            None | Some("NONE") => PostTransform::None,
            Some("LOGISTIC") => PostTransform::Logistic,
            Some(other) => bail!("post_transform {other} is not supported"),
        };

        // (tree, node id) -> position in the flat node list
        let index: HashMap<(i64, i64), u32> = tree_ids
            .iter()
            .zip(node_ids)
            .enumerate()
            .map(|(i, (&t, &id))| ((t, id), i as u32))
            .collect();
        ensure!(index.len() == n, "two nodes share a tree and node id");
        let at = |tree: i64, id: i64| {
            index
                .get(&(tree, id))
                .copied()
                .ok_or_else(|| anyhow!("tree {tree} points at node {id}, which does not exist"))
        };

        let mut nodes = Vec::with_capacity(n);
        for i in 0..n {
            let mode = Mode::parse(&modes[i]).ok_or_else(|| anyhow!("node mode {:?}", modes[i]))?;
            let (yes, no) = if mode == Mode::Leaf {
                (0, 0)
            } else {
                (at(tree_ids[i], yes[i])?, at(tree_ids[i], no[i])?)
            };
            nodes.push(Node {
                feature: u32::try_from(feature_ids[i]).context("negative feature id")?,
                value: values[i] as f32,
                mode,
                yes,
                no,
                missing_yes: missing.get(i).is_some_and(|&m| m != 0),
                weight: 0.0,
            });
        }

        // a leaf's output: the weights given to it, for the positive class
        let class_trees = self.ints("class_treeids")?;
        let class_nodes = self.ints("class_nodeids")?;
        let class_ids = self.ints("class_ids")?;
        let weights = self.numbers("class_weights")?;
        ensure!(
            class_nodes.len() == class_trees.len()
                && class_ids.len() == class_trees.len()
                && weights.len() == class_trees.len(),
            "class_* attributes differ in length"
        );
        // A binary classifier stores one weight per leaf, for class 0 only.
        // The runtime then reads it as the score of class 1.
        ensure!(
            class_ids.iter().all(|&c| c == 0),
            "expected one weight per leaf (a binary classifier), found class ids {:?}",
            class_ids.iter().collect::<std::collections::BTreeSet<_>>()
        );
        for j in 0..class_trees.len() {
            let i = at(class_trees[j], class_nodes[j])? as usize;
            ensure!(
                nodes[i].mode == Mode::Leaf,
                "a weight on a node that is not a leaf"
            );
            nodes[i].weight += weights[j];
        }

        // each tree's root is the one node no other node in it points to
        let mut child = vec![false; n];
        for node in nodes.iter().filter(|node| node.mode != Mode::Leaf) {
            child[node.yes as usize] = true;
            child[node.no as usize] = true;
        }
        let mut roots: Vec<(i64, u32)> = (0..n)
            .filter(|&i| !child[i])
            .map(|i| (tree_ids[i], i as u32))
            .collect();
        roots.sort_unstable();
        ensure!(
            roots.windows(2).all(|w| w[0].0 != w[1].0),
            "a tree with more than one root"
        );
        let base = match self.numbers("base_values") {
            Ok(v) if v.len() == 1 => v[0],
            Ok(v) if v.is_empty() => 0.0,
            Ok(v) => bail!("expected one base value, found {}", v.len()),
            Err(_) => 0.0,
        };

        Forest::new(
            nodes,
            roots.into_iter().map(|(_, i)| i).collect(),
            base,
            post,
        )
    }
}
