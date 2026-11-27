//! A tree ensemble, flattened for fast scoring.
//!
//! Every node of every tree sits in one array of 16-byte entries, laid out
//! tree by tree in depth-first order. Walking a tree is then a loop over
//! indices that mostly moves forward through memory, and the whole model
//! (about 47,000 nodes for the HI-Small model) fits in a core's L2 cache. Leaf
//! outputs are added up in f64, the way LightGBM adds them, so scores do not
//! drift from LightGBM's as the number of trees grows.

use anyhow::{Result, ensure};

/// How a node compares the feature with its value (ONNX `nodes_modes`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Leq,
    Lt,
    Gte,
    Gt,
    Eq,
    Neq,
    Leaf,
}

impl Mode {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "BRANCH_LEQ" => Self::Leq,
            "BRANCH_LT" => Self::Lt,
            "BRANCH_GTE" => Self::Gte,
            "BRANCH_GT" => Self::Gt,
            "BRANCH_EQ" => Self::Eq,
            "BRANCH_NEQ" => Self::Neq,
            "LEAF" => Self::Leaf,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostTransform {
    None,
    Logistic,
}

#[derive(Debug, Clone)]
pub struct Node {
    pub feature: u32,
    pub value: f32,
    pub mode: Mode,
    /// Where to go when the comparison holds, and when it does not.
    pub yes: u32,
    pub no: u32,
    /// Where a missing value (NaN) goes.
    pub missing_yes: bool,
    /// Output of a leaf; unused on branches.
    pub weight: f64,
}

/// A split as it is stored: the comparison value, the feature index with the
/// mode and missing-value flag packed above it, and the two children. A child
/// that is a leaf is stored as the bitwise complement of its index in
/// `leaves`, so it is negative, and a walk ends without loading a node for it.
#[derive(Debug, Clone, Copy)]
struct Packed {
    value: f32,
    meta: u32,
    yes: i32,
    no: i32,
}

const FEATURE_BITS: u32 = 24;
const MISSING_YES: u32 = 1 << 31;

// mode numbers as packed
const LEQ: u32 = 0;
const LT: u32 = 1;
const GTE: u32 = 2;
const GT: u32 = 3;
const EQ: u32 = 4;

fn code(mode: Mode) -> u32 {
    match mode {
        Mode::Leq => LEQ,
        Mode::Lt => LT,
        Mode::Gte => GTE,
        Mode::Gt => GT,
        Mode::Eq => EQ,
        Mode::Neq => 5,
        Mode::Leaf => unreachable!("leaves are not stored as splits"),
    }
}

#[derive(Debug, Clone)]
pub struct Forest {
    splits: Vec<Packed>,
    leaves: Vec<f64>,
    /// Each tree's first split, or a complemented leaf index for a tree that
    /// is a single leaf.
    roots: Vec<i32>,
    base: f64,
    post: PostTransform,
    features: usize,
    /// Every split is "less than or equal", as LightGBM writes them, so the
    /// walk can skip looking at the mode.
    all_leq: bool,
}

impl Forest {
    pub fn new(nodes: Vec<Node>, roots: Vec<u32>, base: f64, post: PostTransform) -> Result<Self> {
        ensure!(!roots.is_empty(), "the model has no trees");
        let n = nodes.len() as u32;
        for node in nodes.iter().filter(|n| n.mode != Mode::Leaf) {
            ensure!(
                node.yes < n && node.no < n,
                "a node points outside the model"
            );
            ensure!(
                node.feature < 1 << FEATURE_BITS,
                "feature index {} is too large",
                node.feature
            );
        }
        ensure!(nodes.len() < i32::MAX as usize, "too many nodes");
        check_acyclic(&nodes, &roots)?;
        let features = nodes
            .iter()
            .filter(|n| n.mode != Mode::Leaf)
            .map(|n| n.feature as usize + 1)
            .max()
            .unwrap_or(0);
        let all_leq = nodes
            .iter()
            .all(|n| matches!(n.mode, Mode::Leq | Mode::Leaf));

        // Lay the splits out again, depth first, the "yes" child right after
        // its parent; the "no" child is filled in once it is placed.
        let mut splits: Vec<Packed> = Vec::with_capacity(nodes.len() / 2 + 1);
        let mut leaves = Vec::with_capacity(nodes.len() / 2 + 1);
        let mut new_roots = Vec::with_capacity(roots.len());
        enum Slot {
            Root,
            Yes(usize),
            No(usize),
        }
        for &root in &roots {
            let mut stack = vec![(root, Slot::Root)];
            while let Some((old, slot)) = stack.pop() {
                let node = &nodes[old as usize];
                let at = if node.mode == Mode::Leaf {
                    leaves.push(node.weight);
                    !(leaves.len() as i32 - 1)
                } else {
                    let missing = if node.missing_yes { MISSING_YES } else { 0 };
                    splits.push(Packed {
                        value: node.value,
                        meta: node.feature | code(node.mode) << FEATURE_BITS | missing,
                        yes: 0,
                        no: 0,
                    });
                    let here = splits.len() - 1;
                    // pushed in reverse so the yes subtree is placed first
                    stack.push((node.no, Slot::No(here)));
                    stack.push((node.yes, Slot::Yes(here)));
                    here as i32
                };
                match slot {
                    Slot::Root => new_roots.push(at),
                    Slot::Yes(p) => splits[p].yes = at,
                    Slot::No(p) => splits[p].no = at,
                }
            }
        }
        Ok(Self {
            splits,
            leaves,
            roots: new_roots,
            base,
            post,
            features,
            all_leq,
        })
    }

    /// Sum of the leaves the input lands in, plus the base value.
    pub fn raw(&self, x: &[f32]) -> f64 {
        assert!(
            x.len() >= self.features,
            "{} features given, the trees read {}",
            x.len(),
            self.features
        );
        let mut sum = self.base;
        if !self.all_leq {
            for &root in &self.roots {
                sum += self.leaves[!self.walk(root, x) as usize];
            }
            return sum;
        }
        // Each step of a walk waits on the node load before it, so one walk
        // at a time leaves the core mostly idle. Walking several trees
        // together keeps that many loads in flight.
        const LANES: usize = 8;
        let mut chunks = self.roots.chunks_exact(LANES);
        for chunk in &mut chunks {
            let mut at: [i32; LANES] = chunk.try_into().expect("chunk of LANES");
            while at.iter().any(|&i| i >= 0) {
                for i in &mut at {
                    if *i >= 0 {
                        *i = self.step_leq(*i, x);
                    }
                }
            }
            // added in tree order, as one walk at a time would
            for i in at {
                sum += self.leaves[!i as usize];
            }
        }
        for &root in chunks.remainder() {
            sum += self.leaves[!self.walk(root, x) as usize];
        }
        sum
    }

    #[inline(always)]
    fn step_leq(&self, i: i32, x: &[f32]) -> i32 {
        let n = self.splits[i as usize];
        let v = x[(n.meta & ((1 << FEATURE_BITS) - 1)) as usize];
        // NaN compares false, so it needs its own test
        if v <= n.value || (v.is_nan() && n.meta & MISSING_YES != 0) {
            n.yes
        } else {
            n.no
        }
    }

    /// One tree, any comparison mode: where the input lands (a complemented
    /// leaf index).
    fn walk(&self, root: i32, x: &[f32]) -> i32 {
        let mut i = root;
        while i >= 0 {
            let n = self.splits[i as usize];
            let v = x[(n.meta & ((1 << FEATURE_BITS) - 1)) as usize];
            let go_yes = if v.is_nan() {
                n.meta & MISSING_YES != 0
            } else {
                match (n.meta >> FEATURE_BITS) & 0x7 {
                    LEQ => v <= n.value,
                    LT => v < n.value,
                    GTE => v >= n.value,
                    GT => v > n.value,
                    EQ => v == n.value,
                    _ => v != n.value,
                }
            };
            i = if go_yes { n.yes } else { n.no };
        }
        i
    }

    pub fn trees(&self) -> usize {
        self.roots.len()
    }

    pub fn nodes(&self) -> usize {
        self.splits.len() + self.leaves.len()
    }

    /// Features the trees read; the input must have at least this many.
    pub fn features(&self) -> usize {
        self.features
    }

    /// Probability of the positive class.
    pub fn score(&self, x: &[f32]) -> f64 {
        let raw = self.raw(x);
        match self.post {
            PostTransform::None => raw,
            PostTransform::Logistic => 1.0 / (1.0 + (-raw).exp()),
        }
    }
}

/// Every walk from a root reaches a leaf: no node is visited twice.
fn check_acyclic(nodes: &[Node], roots: &[u32]) -> Result<()> {
    let mut seen = vec![false; nodes.len()];
    for &root in roots {
        let mut stack = vec![root];
        while let Some(i) = stack.pop() {
            ensure!(
                !seen[i as usize],
                "node {i} is reached twice; the trees are not trees"
            );
            seen[i as usize] = true;
            let node = &nodes[i as usize];
            if node.mode != Mode::Leaf {
                stack.push(node.yes);
                stack.push(node.no);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leaf(weight: f64) -> Node {
        Node {
            feature: 0,
            value: 0.0,
            mode: Mode::Leaf,
            yes: 0,
            no: 0,
            missing_yes: false,
            weight,
        }
    }

    fn split(feature: u32, value: f32, mode: Mode, yes: u32, no: u32, missing_yes: bool) -> Node {
        Node {
            feature,
            value,
            mode,
            yes,
            no,
            missing_yes,
            weight: 0.0,
        }
    }

    /// two stumps: x0 <= 1 -> 1.0 else -1.0; x1 < 5 -> 0.5 else 0.0 (NaN -> yes)
    fn stumps() -> Forest {
        let nodes = vec![
            split(0, 1.0, Mode::Leq, 1, 2, false),
            leaf(1.0),
            leaf(-1.0),
            split(1, 5.0, Mode::Lt, 4, 5, true),
            leaf(0.5),
            leaf(0.0),
        ];
        Forest::new(nodes, vec![0, 3], 0.25, PostTransform::None).unwrap()
    }

    #[test]
    fn walks_each_tree_and_adds_the_leaves() {
        let f = stumps();
        assert_eq!(f.trees(), 2);
        assert_eq!(f.features(), 2);
        assert_eq!(
            f.raw(&[1.0, 4.0]),
            0.25 + 1.0 + 0.5,
            "equal to the value goes left on LEQ"
        );
        assert_eq!(
            f.raw(&[1.5, 5.0]),
            0.25 - 1.0 + 0.0,
            "equal to the value goes right on LT"
        );
        assert_eq!(
            f.raw(&[f32::NAN, f32::NAN]),
            0.25 - 1.0 + 0.5,
            "missing follows the flag"
        );
    }

    #[test]
    fn logistic_turns_the_sum_into_a_probability() {
        let nodes = vec![leaf(0.0)];
        let f = Forest::new(nodes, vec![0], 0.0, PostTransform::Logistic).unwrap();
        assert_eq!(f.score(&[]), 0.5);
    }

    #[test]
    fn refuses_a_loop() {
        let nodes = vec![split(0, 1.0, Mode::Leq, 1, 0, false), leaf(1.0)];
        assert!(Forest::new(nodes, vec![0], 0.0, PostTransform::None).is_err());
    }

    #[test]
    fn refuses_a_pointer_outside_the_model() {
        let nodes = vec![split(0, 1.0, Mode::Leq, 1, 7, false), leaf(1.0)];
        assert!(Forest::new(nodes, vec![0], 0.0, PostTransform::None).is_err());
    }
}
