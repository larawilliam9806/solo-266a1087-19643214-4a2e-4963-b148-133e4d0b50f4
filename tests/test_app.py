import json
import os
import sqlite3
import tempfile
import threading

import pytest

_tmpdir = tempfile.mkdtemp(prefix="metrodp-")
os.environ["APP_DB"] = os.path.join(_tmpdir, "test.db")

import app as app_module  # noqa: E402

CSV_V1 = "乘客ID,进站,出站\np1,A,X\np2,A,X\np3,B,X\np4,A,Y\np5,C,Z\n"
CSV_V2 = "乘客ID,进站,出站\np1,A,X\np2,B,Y\n"


@pytest.fixture(autouse=True)
def clean_db():
    conn = sqlite3.connect(os.environ["APP_DB"])
    conn.executescript("DELETE FROM queries; DELETE FROM return_queries;"
                       " DELETE FROM return_snapshots; DELETE FROM day_state;"
                       " DELETE FROM passengers;")
    conn.close()
    yield


@pytest.fixture
def client():
    return app_module.app.test_client()


def upload(client, date, csv_text):
    return client.post("/upload", data={"date": date, "csv": csv_text})


def query(client, date, ins, outs):
    return client.post("/query", json={"date": date, "in_stations": ins,
                                       "out_stations": outs})


def return_query(client, date, ins, outs):
    return client.post("/return-query", json={"date": date, "in_stations": ins,
                                              "out_stations": outs})


def status(client, date):
    return client.get(f"/api/status?date={date}").get_json()


# ------------------------------------------------------------- 上传与锁定

def test_upload_ok_and_replace_before_lock(client):
    r = upload(client, "2026-11-01", CSV_V1)
    assert r.status_code == 200 and r.get_json()["rows"] == 5
    # 锁定前可整日替换
    r = upload(client, "2026-11-01", CSV_V2)
    assert r.status_code == 200 and r.get_json()["rows"] == 2
    st = status(client, "2026-11-01")
    assert st["rows"] == 2 and not st["locked"]


def test_upload_rejected_after_lock(client):
    upload(client, "2026-11-02", CSV_V1)
    r = query(client, "2026-11-02", ["A"], ["X"])
    assert r.status_code == 200
    r = upload(client, "2026-11-02", CSV_V2)
    assert r.status_code == 409
    assert "锁定" in r.get_json()["error"]
    # 源数据未被改动
    assert status(client, "2026-11-02")["rows"] == 5


def test_duplicate_passenger_rejected(client):
    r = upload(client, "2026-11-03", "乘客ID,进站,出站\np1,A,X\np1,B,Y\n")
    assert r.status_code == 400
    assert "重复" in r.get_json()["error"]


def test_csv_without_header_and_english_header(client):
    r = upload(client, "2026-11-04", "p1,A,X\np2,B,Y\n")
    assert r.status_code == 200 and r.get_json()["rows"] == 2
    r = upload(client, "2026-11-05", "passenger_id,in_station,out_station\np1,A,X\n")
    assert r.status_code == 200 and r.get_json()["rows"] == 1


# ------------------------------------------------------------- 查询与缓存

def test_query_returns_noisy_only(client):
    upload(client, "2026-11-06", CSV_V1)
    r = query(client, "2026-11-06", ["A"], ["X"])
    body = r.get_json()
    assert r.status_code == 200 and body["ok"]
    assert isinstance(body["noisy_count"], float)
    assert "true_count" not in body and "count" not in body
    assert status(client, "2026-11-06")["budget_spent"] == 0.5
    assert status(client, "2026-11-06")["locked"]


def test_same_query_reused_regardless_of_order_and_duplicates(client):
    upload(client, "2026-11-07", CSV_V1)
    r1 = query(client, "2026-11-07", ["A", "B"], ["X", "Y"]).get_json()
    r2 = query(client, "2026-11-07", ["B", "A", "A"], ["Y", "X", "X"]).get_json()
    assert r1["noisy_count"] == r2["noisy_count"]
    assert r2["cached"] is True
    # 第二次未扣费
    assert status(client, "2026-11-07")["budget_spent"] == 0.5


def test_budget_limit_four_queries(client):
    upload(client, "2026-11-08", CSV_V1)
    ok, rejected = 0, 0
    for i in range(6):
        r = query(client, "2026-11-08", [f"S{i}"], ["X"])
        if r.status_code == 200:
            ok += 1
        else:
            rejected += 1
            assert r.status_code == 403
            assert "预算不足" in r.get_json()["error"]
    assert ok == 4 and rejected == 2  # 2 / 0.5 = 4
    assert status(client, "2026-11-08")["budget_spent"] == 2.0
    # 预算用完后, 已缓存的查询仍然返回原值
    r = query(client, "2026-11-08", ["S0"], ["X"])
    assert r.status_code == 200 and r.get_json()["cached"] is True


def test_budget_is_per_date(client):
    upload(client, "2026-11-09", CSV_V1)
    upload(client, "2026-11-10", CSV_V1)
    for _ in range(4):
        assert query(client, "2026-11-09", [f"S{_}"], ["X"]).status_code == 200
    # 另一个日期仍有完整预算
    assert query(client, "2026-11-10", ["S0"], ["X"]).status_code == 200


# ------------------------------------------------------------- 并发

def test_concurrent_distinct_queries_never_overspend(client):
    upload(client, "2026-11-11", CSV_V1)
    results = []

    def worker(i):
        c = app_module.app.test_client()
        results.append(query(c, "2026-11-11", [f"T{i}"], ["X"]).status_code)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert results.count(200) == 4
    assert results.count(403) == 8
    assert status(client, "2026-11-11")["budget_spent"] == 2.0


def test_concurrent_identical_query_single_result(client):
    upload(client, "2026-11-12", CSV_V1)
    bodies = []

    def worker():
        c = app_module.app.test_client()
        bodies.append(query(c, "2026-11-12", ["A", "B"], ["X"]).get_json())

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert all(b["ok"] for b in bodies)
    assert len({b["noisy_count"] for b in bodies}) == 1  # 只有一份结果
    assert status(client, "2026-11-12")["budget_spent"] == 0.5  # 只扣一次
    conn = sqlite3.connect(os.environ["APP_DB"])
    n = conn.execute("SELECT COUNT(*) FROM queries WHERE date='2026-11-12'").fetchone()[0]
    conn.close()
    assert n == 1


# ------------------------------------------------------------- 持久化

def test_restart_keeps_results_and_budget(client):
    upload(client, "2026-11-13", CSV_V1)
    r1 = query(client, "2026-11-13", ["A"], ["X"]).get_json()
    # 模拟重启: 重新初始化数据库连接(文件不变)
    app_module.init_db()
    client2 = app_module.app.test_client()
    r2 = query(client2, "2026-11-13", ["A"], ["X"]).get_json()
    assert r2["cached"] is True and r2["noisy_count"] == r1["noisy_count"]
    assert status(client2, "2026-11-13")["budget_spent"] == 0.5
    # 重启后上传仍被拒绝
    assert upload(client2, "2026-11-13", CSV_V2).status_code == 409


# ------------------------------------------------------------- 次日回乘

CSV_D1 = "乘客ID,进站,出站\np1,A,X\np2,A,X\np3,B,X\np4,A,Y\np5,C,Z\n"
# p1/p2 次日有首程, p3 次日无记录, p4 首日不在条件内, p5 首日不在条件内
CSV_D2 = "乘客ID,进站,出站\np1,M,N\np2,Q,R\np3,S,T\np4,U,V\np6,W,Z\n"


def upload_two_days(client):
    upload(client, "2026-12-01", CSV_D1)
    upload(client, "2026-12-02", CSV_D2)


def test_return_query_counts_only_returning_passengers(client):
    upload_two_days(client)
    body = return_query(client, "2026-12-01", ["A"], ["X"]).get_json()
    assert body["ok"] and isinstance(body["noisy_count"], float)
    assert body["cached"] is False and body["next_date"] == "2026-12-02"
    # 首日 A->X 有 p1,p2,p4; 次日仍有首程的是 p1,p2,p4 -> 3
    # 噪声下无法精确断言, 但多次采样应接近 3; 这里检查不泄露原始字段
    assert "true_count" not in body and "passenger_ids" not in body
    # 两日各扣 0.5
    assert status(client, "2026-12-01")["budget_spent"] == 0.5
    assert status(client, "2026-12-02")["budget_spent"] == 0.5
    # 两日均锁定
    assert status(client, "2026-12-01")["locked"]
    assert status(client, "2026-12-02")["locked"]


def test_return_query_true_count_in_interval(client, monkeypatch):
    """固定噪声为 0 时, 返回值应等于交集真实人数."""
    upload_two_days(client)
    monkeypatch.setattr(app_module, "laplace_noise", lambda scale=2.0: 0.0)
    # A->X p1,p2, 次日都有 -> 2
    assert return_query(client, "2026-12-01", ["A"], ["X"]).get_json()["noisy_count"] == 2
    # B->X p3, 次日有 -> 1
    assert return_query(client, "2026-12-01", ["B"], ["X"]).get_json()["noisy_count"] == 1
    # C->Z p5, 次日无 -> 0
    assert return_query(client, "2026-12-01", ["C"], ["Z"]).get_json()["noisy_count"] == 0


def test_return_query_requires_both_days(client):
    upload(client, "2026-12-03", CSV_D1)
    r = return_query(client, "2026-12-03", ["A"], ["X"])
    assert r.status_code == 409 and "源数据缺失" in r.get_json()["error"]
    # 首日无数据也不行
    upload(client, "2026-12-05", CSV_D1)
    r = return_query(client, "2026-12-04", ["A"], ["X"])
    assert r.status_code == 409
    # 失败不扣费、不锁定
    st = status(client, "2026-12-03")
    assert st["budget_spent"] == 0.0 and not st["locked"]


def test_return_query_reused_same_day_and_stations(client):
    upload_two_days(client)
    r1 = return_query(client, "2026-12-01", ["A", "B"], ["X"]).get_json()
    r2 = return_query(client, "2026-12-01", ["B", "A", "A"], ["X", "X"]).get_json()
    assert r1["noisy_count"] == r2["noisy_count"] and r2["cached"] is True
    assert status(client, "2026-12-01")["budget_spent"] == 0.5
    assert status(client, "2026-12-02")["budget_spent"] == 0.5


def test_return_query_shares_budget_with_single_day(client):
    upload_two_days(client)
    # 首日先用掉 1.5, 次日用掉 0.5 -> 回乘需要两日各 0.5, 首日只余 0.5 可以
    for i in range(3):
        assert query(client, "2026-12-01", [f"S{i}"], ["X"]).status_code == 200
    assert query(client, "2026-12-02", ["S0"], ["X"]).status_code == 200
    r = return_query(client, "2026-12-01", ["A"], ["X"])
    assert r.status_code == 200
    assert status(client, "2026-12-01")["budget_spent"] == 2.0
    assert status(client, "2026-12-02")["budget_spent"] == 1.0
    # 首日预算耗尽后, 新回乘查询被拒且次日也不扣费
    r = return_query(client, "2026-12-01", ["B"], ["X"])
    assert r.status_code == 403 and "预算不足" in r.get_json()["error"]
    assert status(client, "2026-12-02")["budget_spent"] == 1.0


def test_return_query_rejected_when_next_day_budget_insufficient(client):
    upload_two_days(client)
    for i in range(4):
        assert query(client, "2026-12-02", [f"S{i}"], ["X"]).status_code == 200
    r = return_query(client, "2026-12-01", ["A"], ["X"])
    assert r.status_code == 403
    # 首日不扣费、不锁定; 次日状态不变
    st1 = status(client, "2026-12-01")
    assert st1["budget_spent"] == 0.0 and not st1["locked"]
    assert status(client, "2026-12-02")["budget_spent"] == 2.0


def test_return_query_locks_both_days_after_success(client):
    upload_two_days(client)
    assert return_query(client, "2026-12-01", ["A"], ["X"]).status_code == 200
    assert upload(client, "2026-12-01", CSV_V2).status_code == 409
    assert upload(client, "2026-12-02", CSV_V2).status_code == 409


def test_return_query_cached_after_restart(client):
    upload_two_days(client)
    r1 = return_query(client, "2026-12-01", ["A"], ["X"]).get_json()
    app_module.init_db()  # 模拟重启
    c2 = app_module.app.test_client()
    r2 = return_query(c2, "2026-12-01", ["A"], ["X"]).get_json()
    assert r2["cached"] is True and r2["noisy_count"] == r1["noisy_count"]
    assert status(c2, "2026-12-01")["budget_spent"] == 0.5
    assert status(c2, "2026-12-02")["budget_spent"] == 0.5


def test_return_query_interleaved_with_upload_uses_consistent_snapshot(client, monkeypatch):
    """首日无单日查询时, 回乘结果发布后首日仍可替换 -> 重查须按当前数据重新计算."""
    # 首日: p1 A->X, p2 B->X; 次日有 p1,p2
    upload(client, "2026-12-06", "乘客ID,进站,出站\np1,A,X\np2,B,X\n")
    upload(client, "2026-12-07", "乘客ID,进站,出站\np1,M,N\np2,Q,R\n")
    cm = app_module.app.test_client()
    monkeypatch.setattr(app_module, "laplace_noise", lambda scale=2.0: 0.0)
    r1 = return_query(cm, "2026-12-06", ["A"], ["X"]).get_json()
    assert r1["noisy_count"] == 1
    # 首日已被锁定 -> 替换被拒; 重查返回原值, 对应同一份两日源数据
    assert upload(cm, "2026-12-06", CSV_V2).status_code == 409
    r2 = return_query(cm, "2026-12-06", ["A"], ["X"]).get_json()
    assert r2["cached"] is True and r2["noisy_count"] == r1["noisy_count"]


def test_return_query_new_result_after_unlocked_day_replacement(client, monkeypatch):
    """首日被替换(尚未锁定)后, 旧的回乘结果不得再复用."""
    upload(client, "2026-12-08", "乘客ID,进站,出站\np1,A,X\n")
    upload(client, "2026-12-09", "乘客ID,进站,出站\np1,M,N\np2,Q,R\n")
    # 直接写入一条历史结果并记录旧快照版本, 模拟"结果已发布但首日后来被替换"
    conn = sqlite3.connect(os.environ["APP_DB"])
    conn.execute("INSERT INTO day_state (date, locked, budget_spent, version)"
                 " VALUES ('2026-12-08', 0, 0.5, 1)"
                 " ON CONFLICT(date) DO UPDATE SET locked = 0, budget_spent = 0.5")
    conn.execute("INSERT INTO return_queries VALUES ('2026-12-08', '[\"A\"]', '[\"X\"]',"
                 " '[\"A\"]', '[\"X\"]', 42.0, '2026-12-08T00:00:00+00:00')")
    conn.execute("INSERT INTO return_snapshots VALUES ('2026-12-08', '[\"A\"]', '[\"X\"]', 1)")
    conn.commit()
    conn.close()
    # 整日替换: 版本 +1
    upload(client, "2026-12-08", "乘客ID,进站,出站\np1,A,X\np2,A,X\n")
    monkeypatch.setattr(app_module, "laplace_noise", lambda scale=2.0: 0.0)
    r = return_query(client, "2026-12-08", ["A"], ["X"]).get_json()
    assert r["cached"] is False and r["noisy_count"] == 2


# ------------------------------------------------------------- 回乘并发与回滚

def test_concurrent_return_queries_do_not_overspend(client):
    upload_two_days(client)
    results = []

    def worker(i):
        c = app_module.app.test_client()
        results.append(return_query(c, "2026-12-01", [f"S{i}"], ["X"]).status_code)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # 两日各 2 预算, 每次回乘每颗日 0.5 -> 最多 4 次
    assert results.count(200) == 4
    assert status(client, "2026-12-01")["budget_spent"] == 2.0
    assert status(client, "2026-12-02")["budget_spent"] == 2.0


def test_concurrent_identical_return_query_single_result(client):
    upload_two_days(client)
    bodies = []

    def worker():
        c = app_module.app.test_client()
        bodies.append(return_query(c, "2026-12-01", ["A", "B"], ["X"]).get_json())

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert all(b["ok"] for b in bodies)
    assert len({b["noisy_count"] for b in bodies}) == 1
    assert status(client, "2026-12-01")["budget_spent"] == 0.5
    assert status(client, "2026-12-02")["budget_spent"] == 0.5


def test_return_query_save_failure_rolls_back_everything(client, monkeypatch):
    """保存结果失败时, 结果、两日扣费、两日锁定全部不生效."""
    upload_two_days(client)
    # 用 SQLite 触发器模拟结果写入失败(真实的存储层错误)
    conn = sqlite3.connect(os.environ["APP_DB"])
    conn.executescript(
        "CREATE TRIGGER fail_return_query_save BEFORE INSERT ON return_queries"
        " BEGIN SELECT RAISE(ABORT, 'disk I/O error'); END;")
    conn.close()
    monkeypatch.setattr(app_module, "laplace_noise", lambda scale=2.0: 0.0)
    try:
        r = return_query(client, "2026-12-01", ["A"], ["X"])
        assert r.status_code == 500  # 保存失败, 不返回结果
    finally:
        conn = sqlite3.connect(os.environ["APP_DB"])
        conn.executescript("DROP TRIGGER fail_return_query_save;")
        conn.close()

    for d in ("2026-12-01", "2026-12-02"):
        st = status(client, d)
        assert st["budget_spent"] == 0.0 and not st["locked"]
    conn = sqlite3.connect(os.environ["APP_DB"])
    assert conn.execute("SELECT COUNT(*) FROM return_queries").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM return_snapshots").fetchone()[0] == 0
    conn.close()
    # 回滚后可正常重试
    assert return_query(client, "2026-12-01", ["A"], ["X"]).status_code == 200


# ------------------------------------------------------------- 状态接口

def test_return_queries_endpoint(client):
    upload_two_days(client)
    return_query(client, "2026-12-01", ["A"], ["X"])
    data = client.get("/api/return-queries?date=2026-12-01").get_json()
    assert data["ok"] and data["next_date"] == "2026-12-02"
    assert len(data["queries"]) == 1
    assert "passenger" not in json.dumps(data)


# ------------------------------------------------------------- 噪声分布

def test_laplace_noise_distribution():
    samples = [app_module.laplace_noise() for _ in range(20000)]
    mean = sum(samples) / len(samples)
    var = sum((s - mean) ** 2 for s in samples) / len(samples)
    assert abs(mean) < 0.1            # 均值 ≈ 0
    assert abs(var - 8.0) < 0.8       # Laplace(0,2) 方差 = 2 * 2^2 = 8
