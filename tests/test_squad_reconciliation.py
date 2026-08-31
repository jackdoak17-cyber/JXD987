from scripts.sync_sparse_squads import select_team_batch


def test_select_team_batch_is_bounded_and_resumable():
    team_ids = [11, 22, 33, 44, 55]

    assert select_team_batch(team_ids, offset=0, max_teams=2) == [11, 22]
    assert select_team_batch(team_ids, offset=2, max_teams=2) == [33, 44]
    assert select_team_batch(team_ids, offset=4, max_teams=2) == [55]


def test_select_team_batch_zero_max_means_all_remaining():
    assert select_team_batch([11, 22, 33], offset=1, max_teams=0) == [22, 33]


def test_select_team_batch_rejects_invalid_bounds():
    for offset, max_teams in [(-1, 1), (0, -1)]:
        try:
            select_team_batch([11], offset=offset, max_teams=max_teams)
        except ValueError:
            pass
        else:
            raise AssertionError("invalid batch bounds must raise ValueError")
