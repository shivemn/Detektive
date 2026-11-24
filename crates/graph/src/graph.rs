//! A time-windowed directed multigraph of payments.
//!
//! Accounts are nodes and every payment is its own edge, so two accounts can
//! be joined by many edges (a multigraph). Only payments inside a sliding
//! window of event time are kept: when the watermark passes `t`, every edge
//! older than `t - window` is evicted. Memory is therefore bounded by how many
//! payments fit in one window, not by how long the stream has run.
//!
//! Layout. Each account keeps two adjacency lists, outgoing and incoming,
//! with the payment's time, the other account and the amount stored inline:
//! walking a neighbourhood never chases a pointer into a shared edge table.
//! A separate expiry index, ordered by time, says which edges leave the
//! window next.
//!
//! Each account also keeps a running count of payments per counterparty, in
//! each direction, updated as payments enter and leave the window. Fan-in and
//! fan-out (how many distinct accounts) and the number of payments between a
//! pair are then single lookups instead of scans of the neighbourhood: the
//! features in `features.rs` cost the same for a hub as for anyone else.
//!
//! Eviction pops from the front of each adjacency list. Payments mostly
//! arrive in time order, so the oldest edge is almost always at the front.
//! A late payment can sit behind a newer one; it then lingers in the list
//! after it has left the window, until the edges in front of it go too. The
//! live counts are exact regardless (they are updated from the expiry index),
//! and every read filters by the window, so a lingering edge is never seen.

use std::collections::{BTreeMap, VecDeque};

use rustc_hash::FxHashMap;

/// A payment as the graph needs it.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Payment {
    pub tx: u32,
    pub ts: i64,
    pub src: u32,
    pub dst: u32,
    /// In one common unit for every currency (the callers use rupees at the
    /// replayer's fixed rates), so amounts can be compared with each other.
    pub amount: f64,
    /// Payment format (ACH, cheque, ...) as a small code; only compared
    /// for equality.
    pub format: u8,
}

/// One end of an edge, as stored in an account's adjacency list: `peer` is
/// the account at the other end.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Half {
    pub ts: i64,
    pub peer: u32,
    pub tx: u32,
    pub amount: f64,
    pub format: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Insert {
    Added,
    /// Already older than the window when it arrived; not added.
    TooLate,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    /// Edges inside the window.
    pub edges: usize,
    /// Accounts with at least one edge inside the window.
    pub accounts: usize,
    pub evicted: u64,
    pub too_late: u64,
    /// Adjacency entries held, including late edges still lingering after
    /// leaving the window. Twice `edges` when nothing lingers.
    pub stored: usize,
    /// Distinct (account, counterparty) pairs counted, both directions.
    pub pairs: usize,
}

impl Stats {
    /// Rough memory held by the edges, in bytes.
    pub fn approx_bytes(&self) -> usize {
        // a hash map entry: key, count and about one control byte, at the
        // map's typical 7/8 load
        const PAIR: usize = (4 + 4 + 1) * 8 / 7;
        self.stored * std::mem::size_of::<Half>()
            + self.edges * std::mem::size_of::<(u32, u32)>()
            + self.pairs * PAIR
    }
}

#[derive(Debug, Default)]
struct Node {
    out: VecDeque<Half>,
    inc: VecDeque<Half>,
    out_live: u32,
    in_live: u32,
    /// Payments inside the window, per account paid / account paid by.
    out_peers: FxHashMap<u32, u32>,
    in_peers: FxHashMap<u32, u32>,
}

impl Node {
    fn is_live(&self) -> bool {
        self.out_live + self.in_live > 0
    }
}

#[derive(Debug)]
pub struct WindowGraph {
    window: i64,
    /// Edges older than this have left the window. `None` until the first
    /// watermark.
    cutoff: Option<i64>,
    /// Indexed by account id. Ids are small and dense: the replayer gives
    /// every account one when it reads the file.
    nodes: Vec<Node>,
    /// Edges still in the window, by time: (src, dst) of each.
    expiry: BTreeMap<i64, Vec<(u32, u32)>>,
    stats: Stats,
}

impl WindowGraph {
    /// A graph keeping `window` seconds of event time.
    pub fn new(window: i64) -> Self {
        assert!(window > 0, "the window must be positive");
        Self {
            window,
            cutoff: None,
            nodes: Vec::new(),
            expiry: BTreeMap::new(),
            stats: Stats::default(),
        }
    }

    pub fn window(&self) -> i64 {
        self.window
    }

    /// Edges older than this are out of the window.
    pub fn cutoff(&self) -> Option<i64> {
        self.cutoff
    }

    pub fn stats(&self) -> Stats {
        self.stats
    }

    /// Move the window to end at `watermark` and evict what falls out of it.
    /// Returns how many edges were evicted. A watermark that does not move
    /// the window forward is ignored.
    pub fn advance(&mut self, watermark: i64) -> usize {
        let cutoff = watermark.saturating_sub(self.window);
        if self.cutoff.is_some_and(|c| cutoff <= c) {
            return 0;
        }
        self.cutoff = Some(cutoff);

        let mut evicted = 0;
        while let Some(entry) = self.expiry.first_entry() {
            if *entry.key() >= cutoff {
                break;
            }
            for (src, dst) in entry.remove() {
                self.evict(src, dst, cutoff);
                evicted += 1;
            }
        }
        self.stats.edges -= evicted;
        self.stats.evicted += evicted as u64;
        evicted
    }

    fn evict(&mut self, src: u32, dst: u32, cutoff: i64) {
        let node = &mut self.nodes[src as usize];
        node.out_live -= 1;
        self.stats.stored -= pop_expired(&mut node.out, cutoff);
        self.stats.pairs -= uncount(&mut node.out_peers, dst);
        self.release_if_empty(src);

        let node = &mut self.nodes[dst as usize];
        node.in_live -= 1;
        self.stats.stored -= pop_expired(&mut node.inc, cutoff);
        self.stats.pairs -= uncount(&mut node.in_peers, src);
        self.release_if_empty(dst);
    }

    /// An account with nothing left in the window gives its memory back.
    fn release_if_empty(&mut self, id: u32) {
        let node = &mut self.nodes[id as usize];
        if node.is_live() {
            return;
        }
        // with no live edge left, everything it held was older than the
        // cutoff, so the lists are already empty
        debug_assert!(node.out.is_empty() && node.inc.is_empty());
        debug_assert!(node.out_peers.is_empty() && node.in_peers.is_empty());
        if node.out.capacity() + node.inc.capacity() > 0
            || node.out_peers.capacity() + node.in_peers.capacity() > 0
        {
            *node = Node::default();
        }
        self.stats.accounts -= 1;
    }

    /// Add a payment. One already older than the window is refused: it
    /// would be evicted at once, and counting it would make the window lie.
    pub fn insert(&mut self, p: Payment) -> Insert {
        if self.cutoff.is_some_and(|c| p.ts < c) {
            self.stats.too_late += 1;
            return Insert::TooLate;
        }
        let needed = p.src.max(p.dst) as usize + 1;
        if needed > self.nodes.len() {
            self.nodes.resize_with(needed, Node::default);
        }

        let src = &mut self.nodes[p.src as usize];
        let was_live = src.is_live();
        src.out.push_back(Half {
            ts: p.ts,
            peer: p.dst,
            tx: p.tx,
            amount: p.amount,
            format: p.format,
        });
        src.out_live += 1;
        self.stats.pairs += count(&mut src.out_peers, p.dst);
        if !was_live {
            self.stats.accounts += 1;
        }

        // for a payment to oneself this is the same node, already live
        let dst = &mut self.nodes[p.dst as usize];
        let was_live = dst.is_live();
        dst.inc.push_back(Half {
            ts: p.ts,
            peer: p.src,
            tx: p.tx,
            amount: p.amount,
            format: p.format,
        });
        dst.in_live += 1;
        self.stats.pairs += count(&mut dst.in_peers, p.src);
        if !was_live {
            self.stats.accounts += 1;
        }

        self.expiry.entry(p.ts).or_default().push((p.src, p.dst));
        self.stats.edges += 1;
        self.stats.stored += 2;
        Insert::Added
    }

    /// Payments `account` sent inside the window.
    pub fn out_edges(&self, account: u32) -> impl Iterator<Item = &Half> {
        let cutoff = self.cutoff.unwrap_or(i64::MIN);
        self.nodes
            .get(account as usize)
            .into_iter()
            .flat_map(|n| n.out.iter())
            .filter(move |h| h.ts >= cutoff)
    }

    /// Payments `account` received inside the window.
    pub fn in_edges(&self, account: u32) -> impl Iterator<Item = &Half> {
        let cutoff = self.cutoff.unwrap_or(i64::MIN);
        self.nodes
            .get(account as usize)
            .into_iter()
            .flat_map(|n| n.inc.iter())
            .filter(move |h| h.ts >= cutoff)
    }

    /// Payments sent inside the window, counting repeats to the same peer.
    pub fn out_degree(&self, account: u32) -> u32 {
        self.nodes.get(account as usize).map_or(0, |n| n.out_live)
    }

    /// Payments received inside the window, counting repeats from the same
    /// peer.
    pub fn in_degree(&self, account: u32) -> u32 {
        self.nodes.get(account as usize).map_or(0, |n| n.in_live)
    }

    /// Distinct accounts `account` paid inside the window.
    pub fn fan_out(&self, account: u32) -> u32 {
        self.nodes
            .get(account as usize)
            .map_or(0, |n| n.out_peers.len() as u32)
    }

    /// Distinct accounts that paid `account` inside the window.
    pub fn fan_in(&self, account: u32) -> u32 {
        self.nodes
            .get(account as usize)
            .map_or(0, |n| n.in_peers.len() as u32)
    }

    /// Payments from `src` to `dst` inside the window.
    pub fn pair_count(&self, src: u32, dst: u32) -> u32 {
        self.nodes
            .get(src as usize)
            .and_then(|n| n.out_peers.get(&dst))
            .copied()
            .unwrap_or(0)
    }

    /// Accounts with at least one payment inside the window.
    pub fn accounts(&self) -> impl Iterator<Item = u32> + '_ {
        self.nodes
            .iter()
            .enumerate()
            .filter(|(_, n)| n.is_live())
            .map(|(i, _)| i as u32)
    }
}

/// Count one more payment with `peer`; returns 1 if the pair is new.
fn count(peers: &mut FxHashMap<u32, u32>, peer: u32) -> usize {
    let n = peers.entry(peer).or_insert(0);
    *n += 1;
    usize::from(*n == 1)
}

/// Count one payment with `peer` less; returns 1 if the pair is now gone.
fn uncount(peers: &mut FxHashMap<u32, u32>, peer: u32) -> usize {
    let n = peers
        .get_mut(&peer)
        .expect("evicting a payment that was counted");
    *n -= 1;
    if *n == 0 {
        peers.remove(&peer);
        1
    } else {
        0
    }
}

/// Drop entries older than `cutoff` from the front; returns how many.
fn pop_expired(list: &mut VecDeque<Half>, cutoff: i64) -> usize {
    let mut popped = 0;
    while list.front().is_some_and(|h| h.ts < cutoff) {
        list.pop_front();
        popped += 1;
    }
    popped
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pay(tx: u32, ts: i64, src: u32, dst: u32) -> Payment {
        Payment {
            tx,
            ts,
            src,
            dst,
            amount: 1.0,
            format: 0,
        }
    }

    fn txs<'a>(edges: impl Iterator<Item = &'a Half>) -> Vec<u32> {
        let mut v: Vec<u32> = edges.map(|h| h.tx).collect();
        v.sort_unstable();
        v
    }

    #[test]
    fn keeps_parallel_edges_apart() {
        let mut g = WindowGraph::new(100);
        g.insert(pay(0, 10, 1, 2));
        g.insert(pay(1, 20, 1, 2));
        g.insert(pay(2, 30, 2, 1));
        assert_eq!(g.out_degree(1), 2);
        assert_eq!(g.in_degree(2), 2);
        assert_eq!(txs(g.out_edges(1)), vec![0, 1]);
        assert_eq!(txs(g.in_edges(1)), vec![2]);
        assert_eq!(g.stats().edges, 3);
        assert_eq!(g.stats().accounts, 2);
    }

    #[test]
    fn evicts_edges_that_leave_the_window() {
        let mut g = WindowGraph::new(100);
        g.insert(pay(0, 10, 1, 2));
        g.insert(pay(1, 50, 1, 3));
        g.insert(pay(2, 120, 1, 2));
        assert_eq!(g.advance(120), 1, "cutoff 20: only t=10 goes");
        assert_eq!(txs(g.out_edges(1)), vec![1, 2]);
        assert_eq!(g.out_degree(1), 2);
        assert_eq!(g.in_degree(2), 1);
        assert_eq!(g.advance(151), 1, "cutoff 51: t=50 goes");
        assert_eq!(g.stats().accounts, 2, "account 3 has nothing left");
        assert_eq!(g.stats().evicted, 2);
    }

    #[test]
    fn an_edge_exactly_at_the_cutoff_stays() {
        let mut g = WindowGraph::new(100);
        g.insert(pay(0, 20, 1, 2));
        g.advance(120);
        assert_eq!(g.out_degree(1), 1);
        g.advance(121);
        assert_eq!(g.out_degree(1), 0);
    }

    #[test]
    fn refuses_payments_already_out_of_the_window() {
        let mut g = WindowGraph::new(100);
        g.advance(500);
        assert_eq!(g.insert(pay(0, 399, 1, 2)), Insert::TooLate);
        assert_eq!(g.insert(pay(1, 400, 1, 2)), Insert::Added);
        assert_eq!(g.stats().too_late, 1);
        assert_eq!(g.stats().edges, 1);
    }

    #[test]
    fn a_late_edge_behind_a_newer_one_is_hidden_then_dropped() {
        let mut g = WindowGraph::new(100);
        g.insert(pay(0, 60, 1, 2));
        g.insert(pay(1, 30, 1, 2)); // late, but inside the window
        g.advance(140); // cutoff 40: tx 1 leaves, but sits behind tx 0
        assert_eq!(txs(g.out_edges(1)), vec![0]);
        assert_eq!(g.out_degree(1), 1);
        assert_eq!(g.stats().stored, 4, "tx 1 still lingers in both lists");
        g.advance(170);
        assert_eq!(g.stats().stored, 0);
        assert_eq!(g.stats().accounts, 0);
    }

    #[test]
    fn payments_to_oneself_count_once_as_an_account() {
        let mut g = WindowGraph::new(100);
        g.insert(pay(0, 10, 4, 4));
        assert_eq!(g.stats().accounts, 1);
        assert_eq!((g.out_degree(4), g.in_degree(4)), (1, 1));
        g.advance(200);
        assert_eq!(g.stats().accounts, 0);
        assert_eq!(g.stats().edges, 0);
    }

    #[test]
    fn the_window_never_moves_back() {
        let mut g = WindowGraph::new(100);
        g.advance(500);
        g.advance(300);
        assert_eq!(g.cutoff(), Some(400));
    }

    #[test]
    fn counts_distinct_counterparties_and_repeats() {
        let mut g = WindowGraph::new(100);
        g.insert(pay(0, 10, 1, 2));
        g.insert(pay(1, 20, 1, 2));
        g.insert(pay(2, 30, 1, 3));
        g.insert(pay(3, 40, 4, 2));
        assert_eq!((g.out_degree(1), g.fan_out(1)), (3, 2));
        assert_eq!((g.in_degree(2), g.fan_in(2)), (3, 2));
        assert_eq!(g.pair_count(1, 2), 2);
        assert_eq!(g.pair_count(2, 1), 0);
        assert_eq!(
            g.stats().pairs,
            6,
            "1-2, 1-3, 4-2, each counted at both ends"
        );

        g.advance(115); // cutoff 15: tx 0 goes, 1->2 is still paid once
        assert_eq!((g.fan_out(1), g.pair_count(1, 2)), (2, 1));
        g.advance(125); // tx 1 goes too: 1 no longer pays 2
        assert_eq!((g.fan_out(1), g.pair_count(1, 2)), (1, 0));
        assert_eq!(g.fan_in(2), 1);
        assert_eq!(g.stats().pairs, 4);
    }

    #[test]
    fn lists_the_accounts_in_the_window() {
        let mut g = WindowGraph::new(100);
        g.insert(pay(0, 10, 5, 2));
        g.insert(pay(1, 90, 7, 7));
        g.advance(150);
        assert_eq!(g.accounts().collect::<Vec<_>>(), vec![7]);
    }

    #[test]
    fn unknown_accounts_have_no_edges() {
        let g = WindowGraph::new(100);
        assert_eq!(g.out_degree(42), 0);
        assert_eq!(g.out_edges(42).count(), 0);
    }
}
