#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
inv_automat.py — КОНЕЧНЫЙ АВТОМАТ инвентаризации (механизм inv-01 → FSM).

Состояния:
  IDLE → BOOT → INVENTORY → CLAIMS → VERIFY → LEDGER → REPORT → PUBLISH → DONE
  любой проблемный переход → DEGRADED → REPORT (честный частичный прогон)
  защита от зацикливания → HALT

Таблица переходов (единственный источник движения):
  IDLE     --start-->      BOOT
  BOOT     --ok-->         INVENTORY      | --degraded--> DEGRADED
  INVENTORY--ok-->         CLAIMS         | --degraded--> DEGRADED
  CLAIMS   --ok-->         VERIFY
  VERIFY   --ok-->         LEDGER
  LEDGER   --ok-->         REPORT         | --degraded--> DEGRADED
  REPORT   --ok-->         PUBLISH        | --degraded--> DEGRADED
  PUBLISH  --ok-->         DONE
  DEGRADED --report-->     REPORT

Каждый переход дописывается в automat/ledger/runs.jsonl (append-only):
  {"ts","run_id","from","to","guard","evidence"}
Состояние автомата персистится в automat/state.json (resumable).
Drift-детект: сравнение с last_summary из прошлого прогона.

Запуск в Actions: python3 automat/inv_automat.py --user rtynbirf --repo-dir .
Запуск локально:  то же, но без INV_PUSH — PUBLISH только готовит свидетельства.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import inv_mechanism as M  # доказанный движок: сборщики, claims, отчёт

MAX_STEPS = 24

# --- единственный источник движения автомата ---
STATES = ["IDLE", "BOOT", "INVENTORY", "CLAIMS", "VERIFY", "LEDGER",
          "REPORT", "PUBLISH", "DONE", "DEGRADED", "HALT"]
TRANSITIONS = {
    "IDLE": {"start": "BOOT"},
    "BOOT": {"ok": "INVENTORY", "degraded": "DEGRADED"},
    "INVENTORY": {"ok": "CLAIMS", "degraded": "DEGRADED"},
    "CLAIMS": {"ok": "VERIFY"},
    "VERIFY": {"ok": "LEDGER"},
    "LEDGER": {"ok": "REPORT", "degraded": "DEGRADED"},
    "REPORT": {"ok": "PUBLISH", "degraded": "DEGRADED"},
    "PUBLISH": {"ok": "DONE"},
    "DEGRADED": {"report": "REPORT"},
    "DONE": {},
    "HALT": {},
}


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Automat:
    def __init__(self, user, repo_dir, clones_dir, workers, core_wait,
                 profile_cache):
        self.user = user
        self.repo_dir = repo_dir
        self.clones_dir = clones_dir
        self.workers = workers
        self.core_wait = core_wait
        self.profile_cache = profile_cache
        self.run_id = "R" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.ledger_path = os.path.join(repo_dir, "automat/ledger/runs.jsonl")
        self.state_path = os.path.join(repo_dir, "automat/state.json")
        self.report_path = os.path.join(repo_dir, "report/index.html")
        self.c = {}          # контекст между состояниями
        self.degraded_from = None
        self.steps = 0
        os.makedirs(os.path.dirname(self.ledger_path), exist_ok=True)
        os.makedirs(os.path.dirname(self.state_path), exist_ok=True)
        self.state = self._load_state()

    # ---------- персистентность
    def _load_state(self):
        try:
            with open(self.state_path, encoding="utf-8") as f:
                st = json.load(f)
            if st.get("state") in ("DONE", "HALT", None):
                st["state"] = "IDLE"
            return st
        except Exception:
            return {"state": "IDLE", "history": []}

    def _save_state(self):
        self.state["run_id"] = self.run_id
        self.state["updated"] = now_iso()
        with open(self.state_path, "w", encoding="utf-8") as f:
            json.dump(self.state, f, ensure_ascii=False, indent=1)

    def transition(self, frm, guard, to, evidence):
        rec = {"ts": now_iso(), "run_id": self.run_id, "from": frm,
               "guard": guard, "to": to, "evidence": evidence}
        with open(self.ledger_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
        self.state["state"] = to
        hist = self.state.setdefault("history", [])
        hist.append({"ts": rec["ts"], "from": frm, "guard": guard, "to": to})
        del hist[:-12]
        self._save_state()
        M.log(f"FSM {frm} --{guard}--> {to} | {json.dumps(evidence, ensure_ascii=False)[:180]}")

    # ---------- состояния
    def h_IDLE(self):
        self.transition("IDLE", "start", "BOOT",
                        {"user": self.user, "run_id": self.run_id})
        return "BOOT"

    def h_BOOT(self):
        self.c["http"] = M.Http(os.environ.get("GITHUB_TOKEN") or
                                os.environ.get("GH_TOKEN"))
        budgets = M.check_rate_limit(self.c["http"])
        core = (budgets.get("core") or {}).get("remaining") or 0
        search = (budgets.get("search") or {}).get("remaining") or 0
        ev = {"core_remaining": core, "search_remaining": search,
              "token": bool(os.environ.get("GITHUB_TOKEN"))}
        # git+raw работают и без бюджетов — деградация только при полном нуле
        guard = "ok" if (core > 0 or search > 0 or not budgets) else "degraded"
        if guard == "degraded":
            self.degraded_from = "BOOT"
        self.c["budgets"] = budgets
        return guard, ev

    def h_INVENTORY(self):
        http, ev = self.c["http"], {}
        try:
            srepos, stotal = M.search_repos(http, self.user)
        except Exception as e:
            srepos, stotal = [], 0
            ev["search_error"] = str(e)[:120]
        names = [r.get("name") for r in srepos if r.get("name")]
        gitdata = {}
        if names and not os.environ.get("INV_SKIP_CLONES"):
            os.makedirs(self.clones_dir, exist_ok=True)
            from concurrent.futures import ThreadPoolExecutor

            def work(name):
                dest = os.path.join(self.clones_dir, name + ".git")
                ok, err = M.git_clone_bare(
                    f"https://github.com/{self.user}/{name}.git", dest)
                return name, (M.git_info(dest) if ok else {"error": err})

            with ThreadPoolExecutor(max_workers=self.workers) as ex:
                for name, info in ex.map(work, names):
                    gitdata[name] = info
        try:
            commits_author = M.search_commits_author(http, self.user)
        except Exception:
            commits_author = None
        user_core = crepos = events = None
        if M.wait_for_core(http, need=20, max_wait=self.core_wait):
            try:
                user_core = M.core_user(http, self.user)
            except Exception as e:
                ev["core_user_error"] = str(e)[:120]
            try:
                crepos = M.core_repos(http, self.user)
            except Exception:
                crepos = None
            try:
                events = M.core_events(http, self.user)
            except Exception:
                events = None
        if user_core is None and self.profile_cache and \
                os.path.exists(self.profile_cache):
            try:
                with open(self.profile_cache, encoding="utf-8") as f:
                    user_core = json.load(f).get("profile")
                ev["profile_source"] = "cache"
            except Exception:
                pass
        self.c["core_live"] = bool(user_core) and "profile_source" not in ev
        repos = crepos if crepos else srepos
        ev["repos"] = len(repos)
        ev["search_total"] = stotal
        ev["core_repos"] = bool(crepos)
        ev["clones_ok"] = sum(1 for g in gitdata.values()
                              if g.get("commits") is not None)
        self.c.update(srepos=srepos, stotal=stotal, gitdata=gitdata,
                      commits_author=commits_author, user_core=user_core,
                      crepos=crepos, events=events, repos=repos,
                      names=names)
        guard = "ok" if repos else "degraded"
        if guard == "degraded":
            self.degraded_from = "INVENTORY"
        return guard, ev

    def h_CLAIMS(self):
        http, repos = self.c["http"], self.c["repos"]
        branches = {r.get("name"): (r.get("default_branch") or "HEAD")
                    for r in repos if r.get("name")}
        svg = M.fetch_text(http, f"https://github.com/users/{self.user}/contributions")
        days, levels = M.parse_contributions_svg(svg) if svg else ({}, {})
        prof_readme = M.fetch_text(
            http, f"https://raw.githubusercontent.com/{self.user}/"
                  f"{self.user}/HEAD/README.md")
        showcase = M.fetch_text(http, f"https://{self.user}.github.io/")
        showcase = M.strip_html(showcase) if showcase else None
        readmes = {}
        for i, r in enumerate(repos, 1):
            nm = r.get("name")
            if not nm:
                continue
            text, _ = M.fetch_readme(http, self.user, nm,
                                     branches.get(nm, "HEAD"))
            if text:
                readmes[nm] = text
        self.c.update(branches=branches, days=days, levels=levels,
                      prof_readme=prof_readme, showcase=showcase,
                      readmes=readmes)
        return "ok", {"readmes": len(readmes), "calendar_days": len(days) or
                      len(levels)}

    def h_VERIFY(self):
        http, repos, gitdata = self.c["http"], self.c["repos"], self.c["gitdata"]
        deep_results = []
        for t in M.DEEP_TARGETS:
            if t["repo"] not in (self.c["names"] or []):
                deep_results.append({**t, "value": None,
                                     "note": "репо нет в инвентаре"})
                continue
            deep_results.append(M.deep_check(t, gitdata, http, self.user,
                                             self.c["branches"]))
        deep_map = {}
        for d in deep_results:
            deep_map[(d["repo"], d["qty"])] = d
        for qty in ("songs", "languages", "tools"):
            hits = [d for (r, q), d in deep_map.items()
                    if q == qty and r != "*" and d.get("value") is not None]
            if len(hits) == 1:
                deep_map[("*", qty)] = hits[0]
        commits_sum = sum((g.get("commits") or 0)
                          for g in gitdata.values())
        facts = {
            "public_repos": (self.c["user_core"] or {}).get("public_repos"),
            "search_repos_total": self.c["stotal"],
            "core_repos_len": len(self.c["crepos"]) if self.c["crepos"] is not None else None,
            "repos_tab_count": None,
            "commits_author_search": self.c["commits_author"],
            "commits_sum_clones": commits_sum if gitdata else None,
            "per_repo": {n: g.get("commits") for n, g in gitdata.items()
                         if g.get("commits") is not None},
            "followers": (self.c["user_core"] or {}).get("followers"),
            "following": (self.c["user_core"] or {}).get("following"),
            "contrib_year_sum": sum(self.c["days"].values())
            if len(self.c["days"]) >= 100 else None,
        }
        claims = []
        bio = (self.c["user_core"] or {}).get("bio")
        if bio:
            claims += M.extract_claims(bio, "api:core:/users",
                                       "bio @" + self.user, user=self.user)
        if self.c["prof_readme"]:
            claims += M.extract_claims(self.c["prof_readme"], "raw:README",
                                       "README профиля @ " + self.user,
                                       user=self.user)
        if self.c["showcase"]:
            claims += M.extract_claims(self.c["showcase"], "raw:витрина",
                                       self.user + ".github.io",
                                       user=self.user)
        for r in repos:
            nm = r.get("name")
            if not nm or nm == self.user:
                continue
            desc = (r.get("description") or "")
            if desc.strip():
                claims += M.extract_claims(desc, "api:core:/repos",
                                           "описание " + nm, scope=nm,
                                           user=self.user)
            if nm in self.c["readmes"]:
                claims += M.extract_claims(self.c["readmes"][nm],
                                           "raw:README", "README " + nm,
                                           scope=nm, user=self.user)
        for c in claims:
            c.update(M.verify_claim(c, facts, deep_map))
        flags = M.consistency_flags(claims)
        agg = M.build_aggregates(repos, gitdata, facts)
        self.c.update(claims=claims, flags=flags, agg=agg, facts=facts,
                      deep_results=deep_results)
        return "ok", {"claims": len(claims), "flags": len(flags),
                      "fail": sum(1 for x in claims
                                  if x["verdict"] == "FAIL")}

    def h_LEDGER(self):
        prev = self.state.get("last_summary") or {}
        summary = {
            "run_id": self.run_id, "ts": now_iso(),
            "repos": self.c["agg"]["repos_count"],
            "commits_sum_clones": self.c["agg"]["commits_sum_clones"],
            "commits_author_search": self.c["facts"].get("commits_author_search"),
            "claims": len(self.c["claims"]),
            "claims_pass": sum(1 for x in self.c["claims"]
                               if x["verdict"] == "PASS"),
            "claims_fail": sum(1 for x in self.c["claims"]
                               if x["verdict"] == "FAIL"),
            "contradictions": len(self.c["flags"]),
            "degraded_from": self.degraded_from,
        }
        summary["delta_vs_prev"] = {
            k: (summary[k] - prev[k]) if k in prev and
            isinstance(summary.get(k), int) and isinstance(prev.get(k), int)
            else None
            for k in ("repos", "commits_sum_clones", "claims",
                      "claims_fail", "contradictions")}
        rec = {"ts": now_iso(), "run_id": self.run_id,
               "event": "run_summary", "summary": summary}
        with open(self.ledger_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        self.state["last_summary"] = summary
        self._save_state()
        self.c["summary"] = summary
        return "ok", summary

    def h_REPORT(self):
        repos_rows = []
        for r in self.c["repos"]:
            nm = r.get("name")
            g = (self.c["gitdata"].get(nm) or {})
            repos_rows.append({
                "name": nm, "full_name": r.get("full_name"),
                "html_url": r.get("html_url"),
                "description": r.get("description"),
                "language": r.get("language"),
                "license": (r.get("license") or {}).get("spdx_id"),
                "stars": r.get("stargazers_count"),
                "forks": r.get("forks_count"),
                "open_issues": r.get("open_issues_count"),
                "size_kb": r.get("size"),
                "created_at": r.get("created_at"),
                "updated_at": r.get("updated_at"),
                "pushed_at": r.get("pushed_at"),
                "days_since_push": M.days_between(r.get("pushed_at")),
                "archived": r.get("archived"), "fork": r.get("fork"),
                "default_branch": r.get("default_branch"),
                "topics": r.get("topics"), "homepage": r.get("homepage"),
                "has_issues": r.get("has_issues"),
                "git_commits": g.get("commits"),
                "git_last_commit_at": g.get("last_commit_at"),
                "git_file_count": g.get("file_count"),
                "git_error": g.get("error"),
            })
        data = {
            "meta": {"mechanism": "inv-automat FSM", "run_started":
                     self.run_id, "user": self.user, "errors":
                     self.c.get("errors", []), "calls":
                     dict(self.c["http"].calls), "git_clones":
                     len(self.c["gitdata"]),
                     "profile_source": "core /users (живой)" if
                     self.c.get("core_live") else ("кэш профиля" if
                     self.c.get("user_core") else "нет"),
                     "run_finished": now_iso()},
            "profile": self.c["user_core"] or {"login": self.user},
            "facts": self.c["facts"], "repos": repos_rows,
            "git": {n: {k: v for k, v in g.items() if k != "files"}
                    for n, g in self.c["gitdata"].items()},
            "aggregates": self.c["agg"],
            "activity": {"calendar_days": self.c["days"],
                         "calendar_levels": self.c["levels"],
                         "calendar_month_totals": M.month_totals(self.c["days"]),
                         "events": M.events_summary(self.c["events"])},
            "claims": self.c["claims"],
            "contradictions": self.c["flags"],
            "deep": self.c["deep_results"],
            "fsm": {"run_id": self.run_id, "summary": self.c["summary"],
                    "state": "REPORT", "degraded_from": self.degraded_from},
        }
        data["meta"]["calls"] = dict(self.c["http"].calls)
        os.makedirs(os.path.dirname(self.report_path), exist_ok=True)
        with open(self.report_path, "w", encoding="utf-8") as f:
            f.write(M.build_html(data))
        with open(os.path.join(os.path.dirname(self.report_path),
                               "inventory.json"), "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=1, default=str)
        return "ok", {"report_kb": os.path.getsize(self.report_path) // 1024}

    def h_PUBLISH(self):
        ok = os.path.exists(self.report_path) and \
            os.path.exists(self.ledger_path)
        return ("ok" if ok else "degraded"), {
            "report": os.path.exists(self.report_path),
            "ledger": os.path.exists(self.ledger_path),
            "note": "коммит делает workflow после выхода автомата"}

    HANDLERS = {"IDLE": h_IDLE, "BOOT": h_BOOT, "INVENTORY": h_INVENTORY,
                "CLAIMS": h_CLAIMS, "VERIFY": h_VERIFY, "LEDGER": h_LEDGER,
                "REPORT": h_REPORT, "PUBLISH": h_PUBLISH}

    def run(self):
        M.log(f"АВТОМАТ {self.run_id} стартует из состояния "
              f"{self.state['state']}")
        while self.steps < MAX_STEPS:
            self.steps += 1
            st = self.state["state"]
            if st in ("DONE", "HALT"):
                break
            if st == "DEGRADED":
                self.transition("DEGRADED", "report", "REPORT",
                                {"degraded_from": self.degraded_from,
                                 "note": "честный частичный прогон"})
                continue
            h = self.HANDLERS.get(st)
            if h is None:
                self.transition(st, "halt", "HALT",
                                {"error": f"нет обработчика {st}"})
                break
            try:
                res = h(self)
                if st == "IDLE":
                    continue
                guard, ev = res
            except Exception as e:
                guard, ev = "degraded", {"exception": repr(e)[:200]}
                self.degraded_from = st
            if st not in TRANSITIONS or guard not in TRANSITIONS[st]:
                self.transition(st, guard, "DEGRADED",
                                {"note": "переход не найден", **ev})
                self.degraded_from = st
                continue
            self.transition(st, guard, TRANSITIONS[st][guard], ev)
        else:
            self.transition(self.state["state"], "halt", "HALT",
                            {"error": "MAX_STEPS"})
        self.state["state"] = self.state["state"]
        self._save_state()
        M.log(f"АВТОМАТ финал: {self.state['state']} за {self.steps} переходов")
        return 0 if self.state["state"] == "DONE" else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--user", default="rtynbirf")
    ap.add_argument("--repo-dir", default=".")
    ap.add_argument("--clones-dir", default=None)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--core-wait", type=int, default=120)
    ap.add_argument("--profile-cache", default=None)
    args = ap.parse_args()
    clones = args.clones_dir or os.environ.get(
        "INV_CLONES_DIR", "/tmp/inv-automat-clones")
    a = Automat(args.user, args.repo_dir, clones, args.workers,
                args.core_wait, args.profile_cache)
    return a.run()


if __name__ == "__main__":
    sys.exit(main())
