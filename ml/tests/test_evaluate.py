import numpy as np
import pytest

from detektive_ml.evaluate import best_threshold, score, summarise


def test_best_threshold_finds_the_clean_cut():
    y = np.array([0, 0, 0, 1, 1])
    p = np.array([0.1, 0.2, 0.3, 0.8, 0.9])
    threshold, f1 = best_threshold(y, p)
    assert f1 == 1.0
    assert 0.3 < threshold <= 0.8


def test_threshold_comes_from_validation_not_test():
    y_val, p_val = np.array([0, 0, 1, 1]), np.array([0.1, 0.2, 0.6, 0.7])
    # on test the positives score 0.5, below the validation threshold of 0.6
    y_test, p_test = np.array([0, 1, 1]), np.array([0.1, 0.5, 0.5])
    s = score(y_val, p_val, y_test, p_test)
    assert s.threshold == pytest.approx(0.6)
    assert s.tp == 0 and s.fn == 2
    assert s.f1 == 0.0
    # PR-AUC ignores the threshold, so it still sees a perfect ranking
    assert s.pr_auc == pytest.approx(1.0)


def test_confusion_counts_add_up():
    rng = np.random.default_rng(0)
    y = (rng.random(500) < 0.1).astype(int)
    p = rng.random(500) * 0.5 + y * 0.4
    s = score(y, p, y, p)
    assert s.tp + s.fp + s.fn + s.tn == 500


def test_needs_positives():
    with pytest.raises(ValueError):
        score(np.zeros(5), np.zeros(5), np.ones(5), np.ones(5))


def test_summary_is_in_percent():
    y = np.array([0, 1, 1, 0])
    p = np.array([0.1, 0.9, 0.8, 0.2])
    s = score(y, p, y, p)
    assert summarise([s, s])["f1"] == {"mean": 100.0, "std": 0.0}


def test_tail_is_scored_separately_with_the_same_threshold():
    from detektive_ml.evaluate import score_with_tail

    y_val, p_val = np.array([0, 0, 1, 1]), np.array([0.1, 0.2, 0.6, 0.7])
    y_test = np.array([0, 1, 0, 1, 1])
    p_test = np.array([0.1, 0.4, 0.2, 0.9, 0.9])
    tail = np.array([False, False, False, True, True])
    full, trimmed = score_with_tail(y_val, p_val, y_test, p_test, tail)
    assert full.threshold == trimmed.threshold
    assert full.tp == 2 and trimmed.tp == 0 and trimmed.fn == 1


def test_no_tail_means_no_second_score():
    from detektive_ml.evaluate import score_with_tail

    y, p = np.array([0, 1]), np.array([0.2, 0.8])
    assert score_with_tail(y, p, y, p, None)[1] is None
