import argparse
import math
import random
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Set, Tuple

import pandas as pd


DEFAULT_LANGUAGES = ["繁体中文", "阿语", "越南语", "西班牙语", "英文", "印尼语", "葡萄牙语", "菲律宾语"]
WEEK_DAYS = [1, 2, 3, 4, 5, 6, 7]
DRAMABOX_SOURCES = {"Dramabox最新剧单", "七星Dramabox收益榜单"}
CHANNEL_LANG_COL = "频道语种(可多填)"
CHANNEL_LANG_OUTPUT_COL = "频道语言(可多填)"
DRAMA_KEY_COLS = ["内容一级分类", "内容二级分类", "剧名", "一级版权方", "二级版权方", "语种"]
LANG_ALIASES = {
    "英语": "英文",
    "英文": "英文",
    "english": "英文",
    "en": "英文",
    "西班牙语": "西班牙语",
    "西语": "西班牙语",
    "spanish": "西班牙语",
    "es": "西班牙语",
    "繁体中文": "繁体中文",
    "繁中": "繁体中文",
    "繁体": "繁体中文",
    "阿语": "阿语",
    "阿拉伯语": "阿语",
    "越南语": "越南语",
    "印尼语": "印尼语",
    "葡萄牙语": "葡萄牙语",
    "葡语": "葡萄牙语",
    "portuguese": "葡萄牙语",
    "pt": "葡萄牙语",
    "菲律宾语": "菲律宾语",
    "菲律宾": "菲律宾语",
    "filipino": "菲律宾语",
    "tagalog": "菲律宾语",
    "tl": "菲律宾语",
}


def _norm_str(v) -> str:
    if pd.isna(v):
        return ""
    return str(v).strip()


def _split_languages(v: str) -> Set[str]:
    s = _norm_str(v)
    if not s:
        return set()
    vals = set()
    for x in re.split(r"[，,、/|;\s]+", s):
        x = x.strip()
        if not x:
            continue
        vals.add(LANG_ALIASES.get(x, x))
    return vals


def _contains_keyword(v: str, keyword: str) -> bool:
    return keyword in _norm_str(v)


def _norm_lang(v: str) -> str:
    s = _norm_str(v)
    return LANG_ALIASES.get(s, s)


def _drama_key_from_row(row: pd.Series, include_channel: bool = False) -> Tuple[str, ...]:
    vals = []
    for c in DRAMA_KEY_COLS:
        v = row.get(c, "")
        if c == "语种":
            v = _norm_lang(v)
        vals.append(_norm_str(v))
    if include_channel:
        vals.append(_norm_str(row.get("频道链接", "")))
    return tuple(vals)


def _extract_required_num(req_text: str, keyword: str) -> int:
    """
    Parse quantity from text like:
      "完整版短剧*3，分销*2"
      "完整版短剧 3 部"
    """
    s = _norm_str(req_text)
    if not s or keyword not in s:
        return 0
    p1 = re.search(rf"{re.escape(keyword)}\s*[*xX×]?\s*(\d+)", s)
    if p1:
        return int(p1.group(1))
    p2 = re.search(rf"(\d+)\s*部?\s*{re.escape(keyword)}", s)
    if p2:
        return int(p2.group(1))
    return 0


def _random_sort(df: pd.DataFrame, seed: int, by_cols: Sequence[str], start_rank: int = 1, rank_col: str = "sort_rank") -> pd.DataFrame:
    if df.empty:
        out = df.copy()
        out[rank_col] = []
        return out
    out = df.copy()
    out = out.sample(frac=1, random_state=seed).reset_index(drop=True)
    out = out.sort_values(list(by_cols), kind="mergesort").reset_index(drop=True)
    out[rank_col] = range(start_rank, start_rank + len(out))
    return out


def _rebuild_channel_priority(channels: pd.DataFrame, seed: int) -> pd.DataFrame:
    group_cols = [
        "频道一级分类(短剧/动漫/电视剧)",
        "频道二级分类(完整版短剧/解说/分销)",
        "频道一级品类(男频/女频)",
        "频道归属",
        "频道归属版权方",
        CHANNEL_LANG_COL,
    ]
    priority_col = "频道同一语种下排序优先级"
    c = channels.copy()
    c[priority_col] = pd.to_numeric(c[priority_col], errors="coerce").fillna(999999).astype(int)
    c["_origin_priority"] = c[priority_col]
    rows = []
    rng = random.Random(seed)
    for _, g in c.groupby(group_cols, dropna=False):
        g2 = g.copy()
        tie_sorted = []
        for p, gp in g2.groupby("_origin_priority", dropna=False):
            idxs = list(gp.index)
            rng.shuffle(idxs)
            tie_sorted.extend(idxs)
        g2 = g2.loc[tie_sorted].copy()
        g2 = g2.sort_values("_origin_priority", kind="mergesort")
        g2[priority_col] = range(1, len(g2) + 1)
        rows.append(g2)
    out = pd.concat(rows, ignore_index=True) if rows else c.copy()
    out = out.drop(columns=["_origin_priority"])
    return out


def _expand_language_if_empty(schedule: pd.DataFrame, languages: Sequence[str]) -> pd.DataFrame:
    rows = []
    for _, r in schedule.iterrows():
        lang = _norm_str(r.get("语种", ""))
        if lang:
            rows.append(r.to_dict())
            continue
        for lg in languages:
            rr = r.to_dict()
            rr["语种"] = lg
            rows.append(rr)
    return pd.DataFrame(rows)


def _attach_publish_num(pool: pd.DataFrame, hist30: pd.DataFrame) -> pd.DataFrame:
    join_cols = ["内容一级分类", "内容二级分类", "剧名", "一级版权方", "二级版权方", "语种"]
    h = hist30.copy()
    p = pool.copy()
    if "publish_num" in p.columns:
        p = p.drop(columns=["publish_num"])
    for c in join_cols:
        if c in h.columns:
            h[c] = h[c].map(_norm_str)
        if c in p.columns:
            p[c] = p[c].map(_norm_str)
    if "语种" in h.columns:
        h["语种"] = h["语种"].map(_norm_lang)
    if "语种" in p.columns:
        p["语种"] = p["语种"].map(_norm_lang)
    if h.empty:
        pool2 = p.copy()
        pool2["publish_num"] = 0
        return pool2
    h_count = h.groupby(join_cols, dropna=False).size().reset_index(name="publish_num")
    out = p.merge(h_count, on=join_cols, how="left")
    out["publish_num"] = out["publish_num"].fillna(0).astype(int)
    return out


def _build_channel_history_set(hist30: pd.DataFrame) -> Set[Tuple[str, ...]]:
    if hist30.empty or "频道链接" not in hist30.columns:
        return set()
    h = hist30.copy()
    for c in DRAMA_KEY_COLS + ["频道链接"]:
        if c in h.columns:
            if c == "语种":
                h[c] = h[c].map(_norm_lang)
            else:
                h[c] = h[c].map(_norm_str)
    keys = set()
    for _, r in h.iterrows():
        vals = [r.get(c, "") for c in DRAMA_KEY_COLS] + [r.get("频道链接", "")]
        keys.add(tuple(_norm_str(x) for x in vals))
    return keys


def _english_overlap_adjust(channel_row: pd.Series, req_num: int, assigned_english_num: int, lang: str) -> int:
    lang_set = _split_languages(channel_row.get(CHANNEL_LANG_COL, ""))
    if lang in {"西班牙语", "印尼语"} and "英文" in lang_set:
        return max(0, req_num - assigned_english_num)
    return req_num


def _build_full_version_pool(full_pool: pd.DataFrame, content_type: str, lang: str, seed: int) -> pd.DataFrame:
    base = full_pool[
        (full_pool["一级品类(男频/女频)"] == content_type)
        & (full_pool["内容二级分类"] == "完整版短剧")
        & (full_pool["是否可用于内部频道"] == "是")
        & (pd.to_numeric(full_pool["优先级"], errors="coerce").fillna(0).astype(int) == 1)
        & (full_pool["_lang_norm"] == lang)
    ].copy()
    if base.empty:
        base["pool_rank"] = []
        return base

    base["引进日期"] = pd.to_datetime(base["引进日期"], errors="coerce")
    latest_date = base["引进日期"].max()
    base["is_latest"] = base["引进日期"] == latest_date
    base["publish_num"] = pd.to_numeric(base["publish_num"], errors="coerce").fillna(0).astype(int)
    rating = base["评级"].fillna("")

    # a-d: publish_num == 0
    a = base[(base["is_latest"]) & (rating.isin(["S", "A"])) & (base["publish_num"] == 0)]
    b = base[
        (
            (base["is_latest"])
            & (rating == "B")
            & (base["publish_num"] == 0)
        )
        | (
            (~base["is_latest"])
            & (rating.isin(["S", "A"]))
            & (base["publish_num"] == 0)
        )
    ]
    c = base[
        (
            (base["is_latest"])
            & (~rating.isin(["S", "A", "B"]))
            & (base["publish_num"] == 0)
        )
        | (
            (~base["is_latest"])
            & (rating == "B")
            & (base["publish_num"] == 0)
        )
    ]
    d = base[(~base["is_latest"]) & (~rating.isin(["S", "A", "B"])) & (base["publish_num"] == 0)]

    # e-h: publish_num >= 1
    e = base[(base["is_latest"]) & (rating.isin(["S", "A"])) & (base["publish_num"] >= 1)]
    f = base[
        (
            (base["is_latest"])
            & (rating == "B")
            & (base["publish_num"] >= 1)
        )
        | (
            (~base["is_latest"])
            & (rating.isin(["S", "A"]))
            & (base["publish_num"] >= 1)
        )
    ]
    g = base[
        (
            (base["is_latest"])
            & (~rating.isin(["S", "A", "B"]))
            & (base["publish_num"] >= 1)
        )
        | (
            (~base["is_latest"])
            & (rating == "B")
            & (base["publish_num"] >= 1)
        )
    ]
    h = base[(~base["is_latest"]) & (~rating.isin(["S", "A", "B"])) & (base["publish_num"] >= 1)]

    start = 1
    chunks = []
    for part in [a, b, c, d, e, f, g, h]:
        if not part.empty:
            pp = part.sample(frac=1, random_state=seed + start).reset_index(drop=True)
            pp["pool_rank"] = range(start, start + len(pp))
            start += len(pp)
            chunks.append(pp)
    out = pd.concat(chunks, ignore_index=True) if chunks else base.head(0).copy()
    out["pool_rank"] = pd.to_numeric(out["pool_rank"], errors="coerce").astype(int)
    return out.sort_values("pool_rank").reset_index(drop=True)


def _build_distribution_pool(dist_pool: pd.DataFrame, source_order: Optional[Dict[str, int]] = None) -> pd.DataFrame:
    base = dist_pool[
        (dist_pool["内容一级分类"] == "短剧")
        & (dist_pool["内容二级分类"] == "分销")
    ].copy()
    if base.empty:
        base["pool_rank"] = []
        return base

    src_order = source_order or {
        "Reelshort最新剧单": 1,
        "Dramabox最新剧单": 2,
        "Reelshort收益榜单": 3,
        "七星Dramabox收益榜单": 4,
    }
    base["_src_order"] = base["来源"].map(src_order).fillna(99)

    chunks = []
    for lang, g in base.groupby("语种", dropna=False):
        m = g.sort_values(["_src_order"]).copy().reset_index(drop=True)
        m["pool_rank"] = range(1, len(m) + 1)
        chunks.append(m)
    out = pd.concat(chunks, ignore_index=True)
    return out.drop(columns=["_src_order"]).sort_values(["语种", "pool_rank"]).reset_index(drop=True)


def _renumber_priority(df: pd.DataFrame, col: str) -> pd.DataFrame:
    d = df.sort_values(col).reset_index(drop=True).copy()
    d[col] = range(1, len(d) + 1)
    return d


@dataclass
class AssignmentResult:
    assigned: pd.DataFrame
    used_pool_ids: Set[int]
    used_unique_keys: Set[Tuple[str, ...]]
    lang_drama_counts: Dict[Tuple[str, ...], int]
    warnings: List[Dict]


def _assign_round_robin(
    channels_df: pd.DataFrame,
    pool_df: pd.DataFrame,
    demand_keyword: str,
    language: str,
    channel_priority_col: str,
    content_tier_col: str,
    used_pool_ids: Optional[Set[int]] = None,
    used_unique_keys: Optional[Set[Tuple[str, ...]]] = None,
    lang_drama_counts: Optional[Dict[Tuple[str, ...], int]] = None,
    english_assigned_counter: Optional[Dict[str, int]] = None,
    channel_history_set: Optional[Set[Tuple[str, ...]]] = None,
    scan_from_head: bool = False,
    enforce_lang_unique: bool = False,
    max_per_lang_drama: Optional[int] = None,
) -> AssignmentResult:
    if used_pool_ids is None:
        used_pool_ids = set()
    if used_unique_keys is None:
        used_unique_keys = set()
    if lang_drama_counts is None:
        lang_drama_counts = {}
    if english_assigned_counter is None:
        english_assigned_counter = {}
    if channel_history_set is None:
        channel_history_set = set()

    channels = channels_df.copy()
    channels = channels[channels["_lang_set"].map(lambda s: language in s)].copy()
    if channels.empty:
        return AssignmentResult(pd.DataFrame(), used_pool_ids, used_unique_keys, lang_drama_counts, [])

    channels["need_num_raw"] = channels["本周排期要求"].map(lambda x: _extract_required_num(x, demand_keyword))
    channels["already_assigned"] = channels["频道链接"].map(lambda x: english_assigned_counter.get(_norm_str(x), 0)).fillna(0).astype(int)
    channels["need_num"] = channels.apply(
        lambda r: _english_overlap_adjust(r, int(r["need_num_raw"]), int(r["already_assigned"]), language),
        axis=1,
    )
    channels = channels[channels["need_num"] > 0].copy()
    if channels.empty:
        return AssignmentResult(pd.DataFrame(), used_pool_ids, used_unique_keys, lang_drama_counts, [])

    channels = _renumber_priority(channels, channel_priority_col)
    candidate_pool = pool_df[pool_df["_lang_norm"] == language].copy()
    if "pool_uid" not in candidate_pool.columns:
        # 各语种池行号均从 0 起，pool_uid 须含 language，否则全局 used_pool_ids 会跨语种冲突
        candidate_pool["pool_uid"] = candidate_pool.index.map(lambda x: f"{language}::idx::{x}")
    candidate_pool = candidate_pool[~candidate_pool["pool_uid"].isin(used_pool_ids)].copy()
    candidate_pool = candidate_pool.sort_values("pool_rank").reset_index()
    if candidate_pool.empty:
        warn = []
        for _, c in channels.iterrows():
            warn.append(
                {
                    "频道链接": c["频道链接"],
                    "频道语种(可多填)": c[CHANNEL_LANG_COL],
                    "需求量": int(c["need_num"]),
                    "已排量": 0,
                    "缺口": int(c["need_num"]),
                    "告警": "剧库不足",
                }
            )
        return AssignmentResult(pd.DataFrame(), used_pool_ids, used_unique_keys, lang_drama_counts, warn)

    assignment_rows = []
    warnings = []
    for _, ch in channels.iterrows():
        need = int(ch["need_num"])
        got = 0
        while got < need:
            chosen = None
            iter_rows = candidate_pool.iterrows() if scan_from_head else candidate_pool[~candidate_pool["pool_uid"].isin(used_pool_ids)].iterrows()
            for _, p in iter_rows:
                if p["pool_uid"] in used_pool_ids:
                    continue
                if _norm_str(content_tier_col) and _norm_str(content_tier_col) in p and p[content_tier_col] != ch["频道一级品类(男频/女频)"]:
                    continue
                drama_key = _drama_key_from_row(p, include_channel=False)
                history_key = tuple(list(drama_key) + [_norm_str(ch.get("频道链接", ""))])
                if history_key in channel_history_set:
                    continue
                if enforce_lang_unique and drama_key in used_unique_keys:
                    continue
                if max_per_lang_drama is not None and lang_drama_counts.get(drama_key, 0) >= max_per_lang_drama:
                    continue
                chosen = p
                break
            if chosen is None:
                break
            used_pool_ids.add(chosen["pool_uid"])
            drama_key = _drama_key_from_row(chosen, include_channel=False)
            if enforce_lang_unique:
                used_unique_keys.add(drama_key)
            if max_per_lang_drama is not None:
                lang_drama_counts[drama_key] = lang_drama_counts.get(drama_key, 0) + 1
            row = {**ch.to_dict(), **chosen.to_dict()}
            assignment_rows.append(row)
            got += 1
        if got < need:
            warnings.append(
                {
                    "频道链接": ch["频道链接"],
                    "频道语种(可多填)": ch[CHANNEL_LANG_COL],
                    "需求量": need,
                    "已排量": got,
                    "缺口": need - got,
                    "告警": "剧库不足",
                }
            )
        if language == "英文":
            english_assigned_counter[_norm_str(ch["频道链接"])] = english_assigned_counter.get(_norm_str(ch["频道链接"]), 0) + got

    assigned_df = pd.DataFrame(assignment_rows)
    return AssignmentResult(assigned_df, used_pool_ids, used_unique_keys, lang_drama_counts, warnings)


def _assign_full_version(
    channels: pd.DataFrame,
    full_pool_ranked_by_lang_tier: Dict[Tuple[str, str], pd.DataFrame],
    channel_history_set: Set[Tuple[str, ...]],
    languages: Sequence[str],
) -> Tuple[pd.DataFrame, List[Dict]]:
    used = set()
    used_unique = set()
    english_counter = {}
    all_rows = []
    warns = []
    priority_col = "频道同一语种下排序优先级"
    for lang in languages:
        lang_channels = channels[channels["_lang_set"].map(lambda s: lang in s)].copy()
        if lang_channels.empty:
            continue
        for tier in ["男频", "女频"]:
            c2 = lang_channels[lang_channels["频道一级品类(男频/女频)"] == tier].copy()
            pool = full_pool_ranked_by_lang_tier.get((lang, tier), pd.DataFrame())
            if c2.empty or pool.empty:
                for _, cc in c2.iterrows():
                    need = _extract_required_num(cc["本周排期要求"], "完整版短剧")
                    need = _english_overlap_adjust(cc, need, english_counter.get(_norm_str(cc["频道链接"]), 0), lang)
                    if need > 0:
                        warns.append(
                            {
                                "频道链接": cc["频道链接"],
                                "频道语种(可多填)": cc[CHANNEL_LANG_COL],
                                "需求量": need,
                                "已排量": 0,
                                "缺口": need,
                                "告警": "完整版短剧剧库不足",
                            }
                        )
                continue
            res = _assign_round_robin(
                channels_df=c2,
                pool_df=pool,
                demand_keyword="完整版短剧",
                language=lang,
                channel_priority_col=priority_col,
                content_tier_col="一级品类(男频/女频)",
                used_pool_ids=used,
                used_unique_keys=used_unique,
                english_assigned_counter=english_counter,
                channel_history_set=channel_history_set,
                scan_from_head=(lang != "英文"),
                enforce_lang_unique=True,
            )
            used = res.used_pool_ids
            used_unique = res.used_unique_keys
            if not res.assigned.empty:
                all_rows.append(res.assigned)
            warns.extend(res.warnings)
    return (pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame(), warns)


def _assign_distribution(
    channels: pd.DataFrame,
    dist_pool_ypp_yes: pd.DataFrame,
    dist_pool_ypp_no: pd.DataFrame,
    languages: Sequence[str],
) -> Tuple[pd.DataFrame, List[Dict]]:
    used = set()
    used_unique = set()
    lang_drama_counts = {}
    english_counter = {}
    all_rows = []
    warns = []

    # 投流剧：给归属机构 != 酷看的分销频道全量分配
    paid_pool = pd.concat([dist_pool_ypp_yes, dist_pool_ypp_no], ignore_index=True)
    paid_pool = paid_pool[paid_pool["是否为投流剧"] == "是"].copy()
    paid_channels = channels[channels["归属机构"] != "酷看"].copy()
    paid_rows = []
    for _, ch in paid_channels.iterrows():
        for _, p in paid_pool.iterrows():
            if _norm_lang(p["语种"]) in _split_languages(ch[CHANNEL_LANG_COL]):
                paid_rows.append({**ch.to_dict(), **p.to_dict()})
    if paid_rows:
        all_rows.append(pd.DataFrame(paid_rows))

    priority_col = "频道同一语种下排序优先级"
    loops = [(lang, ypp) for ypp in ("是", "否") for lang in languages]

    for lang, ypp in loops:
        c = channels[
            (channels["本周排期要求"].fillna("").str.contains("分销", regex=False))
            & (channels["_lang_set"].map(lambda s: lang in s))
            & (channels["是否过了YPP"].fillna("") == ypp)
        ].copy()
        if c.empty:
            continue
        base_pool = dist_pool_ypp_yes if ypp == "是" else dist_pool_ypp_no
        pool = base_pool[(base_pool["是否为投流剧"] == "否") & (base_pool["_lang_norm"] == lang)].copy()
        pool = pool[~pool.index.isin(used)].copy()
        pool = pool.sort_values("pool_rank").reset_index(drop=True)
        pool["pool_rank"] = range(1, len(pool) + 1)
        res = _assign_round_robin(
            channels_df=c,
            pool_df=pool,
            demand_keyword="分销",
            language=lang,
            channel_priority_col=priority_col,
            content_tier_col="",
            used_pool_ids=used,
            used_unique_keys=used_unique,
            lang_drama_counts=lang_drama_counts,
            english_assigned_counter=english_counter,
            scan_from_head=False,
            enforce_lang_unique=True,
            max_per_lang_drama=(2 if ypp == "否" else None),
        )
        used = res.used_pool_ids
        used_unique = res.used_unique_keys
        lang_drama_counts = res.lang_drama_counts
        if not res.assigned.empty:
            all_rows.append(res.assigned)
        warns.extend(res.warnings)

    return (pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame(), warns)


def _pick_day_random7(rng: random.Random, occupied: Set[int], max_per_day: int, current_count_by_day: Dict[int, int], blocked_days: Optional[Set[int]] = None) -> Optional[int]:
    order = WEEK_DAYS[:]
    rng.shuffle(order)
    blocked_days = blocked_days or set()
    for d in order:
        if d in blocked_days:
            continue
        if d in occupied:
            continue
        if current_count_by_day.get(d, 0) >= max_per_day:
            continue
        return d
    for d in order:
        if d in blocked_days:
            continue
        if current_count_by_day.get(d, 0) >= max_per_day:
            continue
        return d
    return None


def _arrange_weekly_rhythm(assignments: pd.DataFrame, seed: int) -> pd.DataFrame:
    if assignments.empty:
        out = assignments.copy()
        out["排第几天发布"] = []
        return out
    rng = random.Random(seed)
    df = assignments.copy().reset_index(drop=True)
    df["排第几天发布"] = None

    # Rule 1: paid distribution only 1/day/channel; same剧名/day<=half channels
    paid_mask = (df["内容二级分类"] == "分销") & (df["是否为投流剧"] == "是")
    paid = df[paid_mask].copy()
    if not paid.empty:
        channels = sorted(paid["频道链接"].dropna().unique().tolist())
        half_cap = max(1, math.floor(len(channels) / 2))
        day_name_count = {(d, n): 0 for d in WEEK_DAYS for n in paid["剧名"].dropna().unique()}
        ch_day_used = {(c, d): 0 for c in channels for d in WEEK_DAYS}
        for idx, row in paid.sample(frac=1, random_state=seed).iterrows():
            possible_days = WEEK_DAYS[:]
            rng.shuffle(possible_days)
            chosen = None
            for d in possible_days:
                if ch_day_used.get((row["频道链接"], d), 0) >= 1:
                    continue
                if day_name_count.get((d, row["剧名"]), 0) >= half_cap:
                    continue
                chosen = d
                break
            if chosen is None:
                chosen = possible_days[0]
            df.at[idx, "排第几天发布"] = chosen
            ch_day_used[(row["频道链接"], chosen)] = ch_day_used.get((row["频道链接"], chosen), 0) + 1
            day_name_count[(chosen, row["剧名"])] = day_name_count.get((chosen, row["剧名"]), 0) + 1

    # Rule 2: per-channel priority scheduling for remaining records
    # Priority 1: 完整版短剧, <=1/day
    # Priority 2: 分销非投流, <=2/day, day1 excludes dramabox sources,
    # and prioritize days without priority1 and without existing non-paid distribution.
    for ch, g in df[df["排第几天发布"].isna()].groupby("频道链接", dropna=False):
        used_full = set()
        dist_non_paid_count = {}

        # Priority 1: 完整版短剧（单天最多1部，随机7天）
        for idx, row in g[g["内容二级分类"] == "完整版短剧"].sample(frac=1, random_state=seed + 1).iterrows():
            d = _pick_day_random7(rng, used_full, 1, {k: 0 for k in WEEK_DAYS})
            if d is None:
                d = rng.choice(WEEK_DAYS)
            used_full.add(d)
            df.at[idx, "排第几天发布"] = d

        # Priority 2: 分销非投流（单天最多2部）
        candidates = g[(g["内容二级分类"] == "分销") & (g["是否为投流剧"] == "否") & (g["排第几天发布"].isna())]
        for idx, row in candidates.sample(frac=1, random_state=seed + 2).iterrows():
            blocked = set()
            if _norm_str(row.get("来源")) in DRAMABOX_SOURCES:
                blocked.add(1)
            # prioritize days where priority1 has no post and no existing non-paid distribution yet
            blank_days = [d for d in WEEK_DAYS if (d not in used_full and dist_non_paid_count.get(d, 0) == 0 and d not in blocked)]
            if blank_days:
                d = rng.choice(blank_days)
            else:
                avail_days = [d for d in WEEK_DAYS if dist_non_paid_count.get(d, 0) < 2 and d not in blocked]
                d = rng.choice(avail_days) if avail_days else rng.choice([x for x in WEEK_DAYS if x not in blocked] or WEEK_DAYS)
            dist_non_paid_count[d] = dist_non_paid_count.get(d, 0) + 1
            df.at[idx, "排第几天发布"] = d

    # Cross-channel cap for non-paid distribution
    non_paid = df[(df["内容二级分类"] == "分销") & (df["是否为投流剧"] == "否")].copy()
    for day in WEEK_DAYS:
        one_day = non_paid[non_paid["排第几天发布"] == day]
        if one_day.empty:
            continue
        for drama, gd in one_day.groupby("剧名", dropna=False):
            is_db = gd["来源"].isin(DRAMABOX_SOURCES)
            cap = 1 if is_db.any() else 2
            if len(gd) <= cap:
                continue
            overflow = gd.iloc[cap:]
            for idx, row in overflow.iterrows():
                alternative = [d for d in WEEK_DAYS if d != day]
                rng.shuffle(alternative)
                moved = False
                for d2 in alternative:
                    sub = df[
                        (df["排第几天发布"] == d2)
                        & (df["内容二级分类"] == "分销")
                        & (df["是否为投流剧"] == "否")
                        & (df["剧名"] == drama)
                    ]
                    cap2 = 1 if _norm_str(row.get("来源")) in DRAMABOX_SOURCES else 2
                    if len(sub) < cap2:
                        df.at[idx, "排第几天发布"] = d2
                        moved = True
                        break
                if not moved:
                    pass

    df["排第几天发布"] = pd.to_numeric(df["排第几天发布"], errors="coerce").fillna(1).astype(int)
    return df


def run_scheduler(input_excel: str, output_excel: str, seed: int = 2026, languages: Optional[List[str]] = None) -> None:
    if languages is None:
        languages = DEFAULT_LANGUAGES
    random.seed(seed)

    xls = pd.ExcelFile(input_excel)
    week_df = pd.read_excel(xls, sheet_name="1-本周排期剧单")
    channel_df = pd.read_excel(xls, sheet_name="2-频道属性")
    hist_df = pd.read_excel(xls, sheet_name="3-过去30天已发剧单")
    if "频道语言(可多填)" in channel_df.columns and CHANNEL_LANG_COL not in channel_df.columns:
        channel_df[CHANNEL_LANG_COL] = channel_df["频道语言(可多填)"]
    if CHANNEL_LANG_COL not in channel_df.columns:
        raise KeyError("2-频道属性缺少频道语种(可多填)/频道语言(可多填)字段")
    # 源数据里分销剧常见空值，按业务语义视为“非投流”
    if "是否为投流剧" in week_df.columns:
        week_df["是否为投流剧"] = week_df["是否为投流剧"].map(_norm_str).replace("", "否")

    # Point2: rebuild channel priority with tie randomization
    channel_df = _rebuild_channel_priority(channel_df, seed=seed)
    channel_df["_lang_set"] = channel_df[CHANNEL_LANG_COL].map(_split_languages)

    # Point3 Step2: expand language when empty
    week_expanded = _expand_language_if_empty(week_df, languages=languages)
    week_expanded["_lang_norm"] = week_expanded["语种"].map(_norm_lang)
    # Point3 Step3: attach publish_num
    week_expanded = _attach_publish_num(week_expanded, hist_df)
    channel_history_set = _build_channel_history_set(hist_df)

    # Point3 Step1: internal + short drama + full version channels
    full_channels = channel_df[
        (channel_df["频道归属"] == "内部")
        & (channel_df["频道一级分类(短剧/动漫/电视剧)"] == "短剧")
        & (channel_df["频道二级分类(完整版短剧/解说/分销)"].fillna("").str.contains("完整版短剧", regex=False))
    ].copy()

    # Point3 Step4: build pool by tier-language
    tier_values = [x for x in channel_df["频道一级品类(男频/女频)"].dropna().unique().tolist() if _norm_str(x)]
    full_pool_ranked = {}
    for tier in tier_values:
        for lang in languages:
            full_pool_ranked[(lang, tier)] = _build_full_version_pool(
                full_pool=week_expanded,
                content_type=tier,
                lang=lang,
                seed=seed,
            )

    full_assigned, full_warns = _assign_full_version(
        full_channels,
        full_pool_ranked,
        channel_history_set,
        languages=languages,
    )

    # Point4 Step1: internal + short drama + distribution channels
    dist_channels = channel_df[
        (channel_df["频道归属"] == "内部")
        & (channel_df["频道一级分类(短剧/动漫/电视剧)"] == "短剧")
        & (channel_df["频道二级分类(完整版短剧/解说/分销)"].fillna("").str.contains("分销", regex=False))
    ].copy()
    dist_base = _attach_publish_num(week_expanded, hist_df)
    source_order_ypp_yes = {
        "Reelshort最新剧单": 1,
        "Dramabox最新剧单": 2,
        "Reelshort收益榜单": 3,
        "七星Dramabox收益榜单": 4,
    }
    source_order_ypp_no = {
        "Reelshort收益剧单": 1,
        "Reelshort最新榜单": 2,
        # 兼容历史来源命名（仅保留Reelshort两类）
        "Reelshort收益榜单": 1,
        "Reelshort最新剧单": 2,
    }
    dist_pool_yes = _build_distribution_pool(dist_base, source_order=source_order_ypp_yes)
    dist_pool_no = _build_distribution_pool(dist_base, source_order=source_order_ypp_no)
    dist_pool_yes["pool_uid"] = dist_pool_yes.index.map(lambda x: f"yes::{x}")
    dist_pool_no["pool_uid"] = dist_pool_no.index.map(lambda x: f"no::{x}")
    dist_assigned, dist_warns = _assign_distribution(
        dist_channels,
        dist_pool_yes,
        dist_pool_no,
        languages=languages,
    )

    # Merge assignment
    all_assigned = pd.concat([full_assigned, dist_assigned], ignore_index=True, sort=False) if (not full_assigned.empty or not dist_assigned.empty) else pd.DataFrame()

    # Point1 + Point6: weekly rhythm with random 7-day order
    all_scheduled = _arrange_weekly_rhythm(all_assigned, seed=seed)

    final_cols = [
        "频道链接",
        "频道一级分类(短剧/动漫/电视剧)",
        "频道二级分类(完整版短剧/解说/分销)",
        "频道一级品类(男频/女频)",
        "频道归属",
        "频道归属版权方",
        CHANNEL_LANG_OUTPUT_COL,
        "是否过了YPP",
        "频道同一语种下排序优先级",
        "本周排期要求",
        "归属机构",
        "剧名",
        "该剧应该发布什么语种",
        "内容一级分类",
        "内容二级分类",
        "一级版权方",
        "二级版权方",
        "是否为投流剧",
        "来源",
        "排第几天发布",
    ]
    rename_map = {
        "剧名": "排期剧名",
        "该剧应该发布什么语种": "排期发布语种",
    }
    warnings_df = pd.DataFrame(full_warns + dist_warns)

    if all_scheduled.empty:
        output_df = pd.DataFrame(columns=[rename_map.get(c, c) for c in final_cols])
    else:
        all_scheduled[CHANNEL_LANG_OUTPUT_COL] = all_scheduled[CHANNEL_LANG_COL]
        if "_lang_norm" in all_scheduled.columns:
            all_scheduled["该剧应该发布什么语种"] = all_scheduled["_lang_norm"]
        else:
            all_scheduled["该剧应该发布什么语种"] = all_scheduled.get("语种")
        for c in final_cols:
            if c not in all_scheduled.columns:
                all_scheduled[c] = None
        output_df = all_scheduled[final_cols].rename(columns=rename_map)

    with pd.ExcelWriter(output_excel, engine="openpyxl") as writer:
        output_df.to_excel(writer, index=False, sheet_name="排期结果")
        warnings_df.to_excel(writer, index=False, sheet_name="告警")
        channel_df.to_excel(writer, index=False, sheet_name="频道优先级重排")


def main():
    parser = argparse.ArgumentParser(description="Weekly scheduling rhythm planner.")
    parser.add_argument("--input", required=True, help="Input Excel path (contains 3 sheets).")
    parser.add_argument("--output", required=True, help="Output Excel path.")
    parser.add_argument("--seed", type=int, default=2026, help="Random seed.")
    parser.add_argument(
        "--languages",
        default=",".join(DEFAULT_LANGUAGES),
        help="Comma-separated fallback languages when 语种 is empty.",
    )
    args = parser.parse_args()
    languages = [x.strip() for x in args.languages.split(",") if x.strip()]
    run_scheduler(input_excel=args.input, output_excel=args.output, seed=args.seed, languages=languages)
    print(f"Done. Output -> {args.output}")


if __name__ == "__main__":
    main()
