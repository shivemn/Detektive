//! Why a payment scored what it did: each feature's share of the score.
//!
//! This is path-dependent TreeSHAP (Lundberg, Erion and Lee, "Consistent
//! individualized feature attribution for tree ensembles", 2018), written to
//! match LightGBM's own `pred_contrib`. For each tree, it shares the leaf's
//! value among the features that were split on along the way. A split counts
//! for a feature as much as the payment's branch differs from what the
//! training data did at that node on average. The shares are additive: the
//! base value plus every feature's share is exactly the model's raw score (the
//! log-odds before the logistic), so an explanation never leaves part of a
//! score unaccounted for.
//!
//! The tree walk needs how many training rows reached each node. ONNX has no
//! place for that, so the export writes it next to the model (`cover.json`).
//!
//! Cost grows with leaves × depth², so this is far slower than scoring: a few
//! milliseconds a payment on the HI-Small model. The consumer runs it only for
//! payments that alert.

use anyhow::{Result, ensure};

use crate::forest::{Mode, Node};

#[derive(Debug, Clone, Copy)]
struct Split {
    feature: u32,
    value: f32,
    mode: Mode,
    missing_yes: bool,
    /// Children in this tree's own node list.
    yes: u32,
    no: u32,
}

#[derive(Debug, Clone)]
struct Tree {
    /// `None` for a leaf.
    splits: Vec<Option<Split>>,
    /// Leaf outputs; 0 on splits.
    values: Vec<f64>,
    cover: Vec<f64>,
    max_depth: usize,
    /// What the tree outputs on average over the training data.
    expected: f64,
}

/// One feature's share of a score, in log-odds.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Contribution {
    pub feature: usize,
    pub value: f64,
}

#[derive(Debug, Clone)]
pub struct Shap {
    trees: Vec<Tree>,
    features: usize,
    base: f64,
}

/// A node still to copy: its index in the file, its depth, and the slot in
/// its parent to point at it (parent, whether it is the "yes" child).
type Pending = (u32, usize, Option<(usize, bool)>);

#[derive(Debug, Clone, Copy, Default)]
struct PathElement {
    feature: i64,
    zero: f64,
    one: f64,
    weight: f64,
}

impl Shap {
    /// `nodes` and `roots` as read from the ONNX file, `cover` the training
    /// rows through each of those nodes, in the same order. `features` is the
    /// width of the input; `base` the ensemble's base value.
    pub fn new(
        nodes: &[Node],
        roots: &[u32],
        cover: &[f64],
        features: usize,
        base: f64,
    ) -> Result<Self> {
        ensure!(
            cover.len() == nodes.len(),
            "cover has {} entries, the model has {} nodes",
            cover.len(),
            nodes.len()
        );
        let mut trees = Vec::with_capacity(roots.len());
        for &root in roots {
            // copy the tree out, depth first, with local indices
            let mut local = Tree {
                splits: Vec::new(),
                values: Vec::new(),
                cover: Vec::new(),
                max_depth: 0,
                expected: 0.0,
            };
            let mut stack: Vec<Pending> = vec![(root, 0, None)];
            while let Some((g, depth, parent)) = stack.pop() {
                let here = local.splits.len();
                if let Some((p, is_yes)) = parent {
                    let s = local.splits[p].as_mut().expect("parent is a split");
                    if is_yes {
                        s.yes = here as u32;
                    } else {
                        s.no = here as u32;
                    }
                }
                let n = &nodes[g as usize];
                let c = cover[g as usize];
                ensure!(
                    c > 0.0 || n.mode == Mode::Leaf,
                    "a split no training row reached"
                );
                local.cover.push(c);
                local.max_depth = local.max_depth.max(depth);
                if n.mode == Mode::Leaf {
                    local.splits.push(None);
                    local.values.push(n.weight);
                } else {
                    ensure!(
                        (n.feature as usize) < features,
                        "a split on feature {} of {features}",
                        n.feature
                    );
                    local.splits.push(Some(Split {
                        feature: n.feature,
                        value: n.value,
                        mode: n.mode,
                        missing_yes: n.missing_yes,
                        yes: 0,
                        no: 0,
                    }));
                    local.values.push(0.0);
                    stack.push((n.no, depth + 1, Some((here, false))));
                    stack.push((n.yes, depth + 1, Some((here, true))));
                }
            }
            local.expected = if local.splits.len() == 1 {
                local.values[0]
            } else {
                let total = local.cover[0];
                (0..local.splits.len())
                    .filter(|&i| local.splits[i].is_none())
                    .map(|i| local.cover[i] / total * local.values[i])
                    .sum()
            };
            trees.push(local);
        }
        Ok(Self {
            trees,
            features,
            base,
        })
    }

    /// The score's starting point before any feature is known: the base value
    /// plus each tree's average output. Every explanation starts here.
    pub fn expected(&self) -> f64 {
        self.base + self.trees.iter().map(|t| t.expected).sum::<f64>()
    }

    /// Each feature's share of the raw score (log-odds), in feature order.
    /// `expected()` plus the sum of these is the raw score.
    pub fn contributions(&self, x: &[f32]) -> Vec<f64> {
        assert!(
            x.len() >= self.features,
            "{} features given, the model has {}",
            x.len(),
            self.features
        );
        let mut phi = vec![0.0; self.features];
        let mut path = Vec::new();
        for tree in &self.trees {
            if tree.splits.len() == 1 {
                continue; // a single leaf: all of it is in `expected`
            }
            let d = tree.max_depth + 2;
            path.clear();
            path.resize(d * (d + 1) / 2, PathElement::default());
            recurse(tree, x, &mut phi, &mut path, 0, 0, 0, 1.0, 1.0, -1);
        }
        phi
    }

    /// The features that pushed the score furthest, either way, largest
    /// first.
    pub fn top(&self, x: &[f32], n: usize) -> Vec<Contribution> {
        let mut all: Vec<Contribution> = self
            .contributions(x)
            .into_iter()
            .enumerate()
            .map(|(feature, value)| Contribution { feature, value })
            .collect();
        all.sort_by(|a, b| b.value.abs().total_cmp(&a.value.abs()));
        all.truncate(n);
        all
    }
}

fn goes_yes(s: &Split, v: f32) -> bool {
    if v.is_nan() {
        return s.missing_yes;
    }
    match s.mode {
        Mode::Leq => v <= s.value,
        Mode::Lt => v < s.value,
        Mode::Gte => v >= s.value,
        Mode::Gt => v > s.value,
        Mode::Eq => v == s.value,
        Mode::Neq => v != s.value,
        Mode::Leaf => unreachable!(),
    }
}

/// One step of TreeSHAP. The path for this call lives at
/// `path[offset..=offset + depth]`, right after its parent's.
#[allow(clippy::too_many_arguments)]
fn recurse(
    tree: &Tree,
    x: &[f32],
    phi: &mut [f64],
    path: &mut [PathElement],
    parent_offset: usize,
    node: usize,
    depth: usize,
    zero: f64,
    one: f64,
    feature: i64,
) {
    let offset = parent_offset + depth;
    if depth > 0 {
        path.copy_within(parent_offset..parent_offset + depth, offset);
    }
    let here = &mut path[offset..];
    extend(here, depth, zero, one, feature);

    let Some(split) = tree.splits[node] else {
        let leaf = tree.values[node];
        for i in 1..=depth {
            let w = unwound_sum(here, depth, i);
            let e = here[i];
            phi[e.feature as usize] += w * (e.one - e.zero) * leaf;
        }
        return;
    };

    let (hot, cold) = if goes_yes(&split, x[split.feature as usize]) {
        (split.yes as usize, split.no as usize)
    } else {
        (split.no as usize, split.yes as usize)
    };
    let w = tree.cover[node];
    let hot_zero = tree.cover[hot] / w;
    let cold_zero = tree.cover[cold] / w;
    let (mut incoming_zero, mut incoming_one) = (1.0, 1.0);
    let mut depth = depth;

    // if this feature was split on higher up, undo that split and redo it here
    let f = i64::from(split.feature);
    if let Some(k) = (0..=depth).find(|&k| here[k].feature == f) {
        incoming_zero = here[k].zero;
        incoming_one = here[k].one;
        unwind(here, depth, k);
        depth -= 1;
    }

    recurse(
        tree,
        x,
        phi,
        path,
        offset,
        hot,
        depth + 1,
        hot_zero * incoming_zero,
        incoming_one,
        f,
    );
    recurse(
        tree,
        x,
        phi,
        path,
        offset,
        cold,
        depth + 1,
        cold_zero * incoming_zero,
        0.0,
        f,
    );
}

fn extend(path: &mut [PathElement], depth: usize, zero: f64, one: f64, feature: i64) {
    path[depth] = PathElement {
        feature,
        zero,
        one,
        weight: if depth == 0 { 1.0 } else { 0.0 },
    };
    let d1 = (depth + 1) as f64;
    for i in (0..depth).rev() {
        path[i + 1].weight += one * path[i].weight * (i + 1) as f64 / d1;
        path[i].weight = zero * path[i].weight * (depth - i) as f64 / d1;
    }
}

fn unwind(path: &mut [PathElement], depth: usize, k: usize) {
    let one = path[k].one;
    let zero = path[k].zero;
    let d1 = (depth + 1) as f64;
    let mut next = path[depth].weight;
    for i in (0..depth).rev() {
        if one != 0.0 {
            let tmp = path[i].weight;
            path[i].weight = next * d1 / ((i + 1) as f64 * one);
            next = tmp - path[i].weight * zero * (depth - i) as f64 / d1;
        } else {
            path[i].weight = path[i].weight * d1 / (zero * (depth - i) as f64);
        }
    }
    for i in k..depth {
        path[i].feature = path[i + 1].feature;
        path[i].zero = path[i + 1].zero;
        path[i].one = path[i + 1].one;
    }
}

fn unwound_sum(path: &[PathElement], depth: usize, k: usize) -> f64 {
    let one = path[k].one;
    let zero = path[k].zero;
    let d1 = (depth + 1) as f64;
    let mut next = path[depth].weight;
    let mut total = 0.0;
    for i in (0..depth).rev() {
        if one != 0.0 {
            let tmp = next * d1 / ((i + 1) as f64 * one);
            total += tmp;
            next = path[i].weight - tmp * zero * ((depth - i) as f64 / d1);
        } else {
            total += (path[i].weight / zero) / ((depth - i) as f64 / d1);
        }
    }
    total
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

    fn split(feature: u32, value: f32, yes: u32, no: u32) -> Node {
        Node {
            feature,
            value,
            mode: Mode::Leq,
            yes,
            no,
            missing_yes: false,
            weight: 0.0,
        }
    }

    /// Brute-force Shapley values for one tree, by the definition: average the
    /// change from adding each feature over every order of the features. A
    /// feature left out follows both branches, weighted by training cover.
    fn brute(nodes: &[Node], cover: &[f64], root: usize, x: &[f32], features: usize) -> Vec<f64> {
        fn value(nodes: &[Node], cover: &[f64], i: usize, x: &[f32], known: &[bool]) -> f64 {
            let n = &nodes[i];
            if n.mode == Mode::Leaf {
                return n.weight;
            }
            if known[n.feature as usize] {
                let next = if x[n.feature as usize] <= n.value {
                    n.yes
                } else {
                    n.no
                };
                value(nodes, cover, next as usize, x, known)
            } else {
                let (y, o) = (n.yes as usize, n.no as usize);
                (cover[y] * value(nodes, cover, y, x, known)
                    + cover[o] * value(nodes, cover, o, x, known))
                    / cover[i]
            }
        }
        let mut phi = vec![0.0; features];
        let mut perm: Vec<usize> = (0..features).collect();
        let mut count = 0.0;
        // every permutation (features is tiny in the tests)
        fn permutations(k: usize, perm: &mut Vec<usize>, out: &mut Vec<Vec<usize>>) {
            if k == perm.len() {
                out.push(perm.clone());
                return;
            }
            for i in k..perm.len() {
                perm.swap(k, i);
                permutations(k + 1, perm, out);
                perm.swap(k, i);
            }
        }
        let mut all = Vec::new();
        permutations(0, &mut perm, &mut all);
        for order in all {
            let mut known = vec![false; features];
            let mut before = value(nodes, cover, root, x, &known);
            for &f in &order {
                known[f] = true;
                let after = value(nodes, cover, root, x, &known);
                phi[f] += after - before;
                before = after;
            }
            count += 1.0;
        }
        phi.iter().map(|p| p / count).collect()
    }

    /// x0 <= 1 ? (x1 <= 5 ? 3 : (x0 <= 0 ? -2 : 1)) : (x2 <= 0 ? 0.5 : -1)
    /// x0 is split on twice along one path, which exercises the unwind.
    fn tree() -> (Vec<Node>, Vec<f64>) {
        let nodes = vec![
            split(0, 1.0, 1, 2),
            split(1, 5.0, 3, 4),
            split(2, 0.0, 7, 8),
            leaf(3.0),
            split(0, 0.0, 5, 6),
            leaf(-2.0),
            leaf(1.0),
            leaf(0.5),
            leaf(-1.0),
        ];
        let cover = vec![100.0, 60.0, 40.0, 25.0, 35.0, 20.0, 15.0, 10.0, 30.0];
        (nodes, cover)
    }

    #[test]
    fn matches_shapley_values_by_brute_force() {
        let (nodes, cover) = tree();
        let shap = Shap::new(&nodes, &[0], &cover, 3, 0.0).unwrap();
        for x in [
            [0.5, 7.0, 1.0],
            [-1.0, 7.0, -1.0],
            [2.0, 0.0, -1.0],
            [1.0, 5.0, 0.0],
            [0.0, 9.0, 9.0],
        ] {
            let fast = shap.contributions(&x);
            let slow = brute(&nodes, &cover, 0, &x, 3);
            for (a, b) in fast.iter().zip(&slow) {
                assert!((a - b).abs() < 1e-12, "x {x:?}: {fast:?} vs {slow:?}");
            }
        }
    }

    #[test]
    fn shares_add_up_to_the_score() {
        let (nodes, cover) = tree();
        let shap = Shap::new(&nodes, &[0], &cover, 3, 0.25).unwrap();
        let forest = crate::forest::Forest::new(
            nodes.clone(),
            vec![0],
            0.25,
            crate::forest::PostTransform::None,
        )
        .unwrap();
        for x in [[0.5, 7.0, 1.0], [-1.0, 7.0, -1.0], [2.0, 0.0, -1.0]] {
            let total = shap.expected() + shap.contributions(&x).iter().sum::<f64>();
            assert!((total - forest.raw(&x)).abs() < 1e-12);
        }
        // expected value: cover-weighted mean of the leaves, plus the base
        let mean = (25.0 * 3.0 + 20.0 * -2.0 + 15.0 * 1.0 + 10.0 * 0.5 - 30.0) / 100.0;
        assert!((shap.expected() - (0.25 + mean)).abs() < 1e-12);
    }

    #[test]
    fn top_orders_by_size_either_way() {
        let (nodes, cover) = tree();
        let shap = Shap::new(&nodes, &[0], &cover, 3, 0.0).unwrap();
        let top = shap.top(&[2.0, 0.0, -1.0], 2);
        assert_eq!(top.len(), 2);
        assert!(top[0].value.abs() >= top[1].value.abs());
    }

    #[test]
    fn refuses_cover_of_the_wrong_length() {
        let (nodes, cover) = tree();
        assert!(Shap::new(&nodes, &[0], &cover[..3], 3, 0.0).is_err());
    }
}
