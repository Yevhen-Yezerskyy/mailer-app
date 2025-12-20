# FILE: engine/common/worker.py  (обновлено — 2025-12-19)
# Смысл: тикер-планировщик задач. Фикс: больше не плодим зомби — всегда делаем proc.join() для завершившихся/убитых задач,
# и аккуратно закрываем Queue (close/join_thread).

from __future__ import annotations

import json
import os
import signal
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime
from multiprocessing import Process, Queue
from typing import Any, Callable, Dict, Optional


def _now_ts() -> float:
    return time.time()


def _iso(ts: Optional[float] = None) -> str:
    if ts is None:
        ts = _now_ts()
    return datetime.utcfromtimestamp(ts).isoformat(timespec="seconds") + "Z"


def _safe_json(obj: Any, max_len: int = 4000) -> str:
    try:
        s = json.dumps(obj, ensure_ascii=False, default=str)
    except Exception:
        s = str(obj)
    if len(s) > max_len:
        s = s[: max_len - 3] + "..."
    return s


@dataclass
class TaskSpec:
    name: str
    fn: Callable[[], Any]
    every_sec: int
    timeout_sec: Optional[int] = None
    singleton: bool = True
    heavy: bool = False
    priority: int = 50  # меньше = раньше
    jitter_sec: int = 0  # +-рандом не делаем специально (без random), но поле оставлено


@dataclass
class RunningTask:
    spec: TaskSpec
    proc: Process
    q: Queue
    started_at: float
    deadline_at: Optional[float]
    pid: int = 0


class Worker:
    def __init__(
        self,
        *,
        max_parallel: int = 50,
        tick_sec: float = 0.5,
        log_path: Optional[str] = None,
        name: str = "worker",
    ):
        self.max_parallel = int(max_parallel)
        self.tick_sec = float(tick_sec)
        self.log_path = log_path
        self.name = name

        self._specs: Dict[str, TaskSpec] = {}
        self._next_run_at: Dict[str, float] = {}
        self._running: Dict[str, RunningTask] = {}  # by task name (singleton)
        self._stop = False

        self._heavy_running_name: Optional[str] = None  # task name

        # чтобы корректно останавливаться
        signal.signal(signal.SIGTERM, self._handle_stop)
        signal.signal(signal.SIGINT, self._handle_stop)

    # -------------------- public API --------------------

    def register(
        self,
        name: str,
        fn: Callable[[], Any],
        *,
        every_sec: int,
        timeout_sec: Optional[int] = None,
        singleton: bool = True,
        heavy: bool = False,
        priority: int = 50,
    ) -> None:
        if not name or not isinstance(name, str):
            raise ValueError("name must be non-empty str")
        if name in self._specs:
            raise ValueError(f"task '{name}' already registered")
        if every_sec <= 0:
            raise ValueError("every_sec must be > 0")

        spec = TaskSpec(
            name=name,
            fn=fn,
            every_sec=int(every_sec),
            timeout_sec=int(timeout_sec) if timeout_sec is not None else None,
            singleton=bool(singleton),
            heavy=bool(heavy),
            priority=int(priority),
        )
        self._specs[name] = spec
        self._next_run_at[name] = _now_ts()  # можно стартовать сразу

        self._log(
            "registered",
            {
                "task": name,
                "every_sec": spec.every_sec,
                "timeout_sec": spec.timeout_sec,
                "singleton": spec.singleton,
                "heavy": spec.heavy,
                "priority": spec.priority,
            },
        )

    def decorator(
        self,
        name: str,
        *,
        every_sec: int,
        timeout_sec: Optional[int] = None,
        singleton: bool = True,
        heavy: bool = False,
        priority: int = 50,
    ):
        def _wrap(fn: Callable[[], Any]):
            self.register(
                name,
                fn,
                every_sec=every_sec,
                timeout_sec=timeout_sec,
                singleton=singleton,
                heavy=heavy,
                priority=priority,
            )
            return fn

        return _wrap

    def stop(self) -> None:
        self._stop = True

    def run_forever(self) -> None:
        self._log("start", {"worker": self.name, "pid": os.getpid(), "tasks": len(self._specs)})
        while not self._stop:
            try:
                self._collect_finished()
                self._kill_timeouts()
                self._schedule_starts()
            except Exception:
                # тикер не умирает никогда
                self._log("ticker_exception", {"traceback": traceback.format_exc()})
            time.sleep(self.tick_sec)
        self._log("stop", {"worker": self.name})

    # -------------------- internals --------------------

    def _handle_stop(self, *_args):
        self._stop = True

    def _can_start_anything(self) -> bool:
        # heavy running => не стартуем ничего нового
        return self._heavy_running_name is None

    def _running_count(self) -> int:
        return len(self._running)

    def _due_specs(self) -> list[TaskSpec]:
        now = _now_ts()
        specs: list[TaskSpec] = []
        for name, spec in self._specs.items():
            if self._next_run_at.get(name, 0) <= now:
                specs.append(spec)
        # приоритет: меньше раньше, потом по имени (стабильно)
        specs.sort(key=lambda s: (s.priority, s.name))
        return specs

    def _schedule_starts(self) -> None:
        if not self._specs:
            return

        # лимит параллельности
        if self._running_count() >= self.max_parallel:
            return

        due = self._due_specs()
        if not due:
            return

        for spec in due:
            # heavy блокирует старты
            if not self._can_start_anything():
                return

            # singleton: если ещё running — просто ждём следующий тик
            if spec.singleton and spec.name in self._running:
                continue

            # если heavy — стартуем только его, и сразу включаем глобальный стоп для остальных стартов
            if spec.heavy:
                if self._running_count() >= self.max_parallel:
                    return
                started = self._start_task(spec)
                if started:
                    self._heavy_running_name = spec.name
                return  # heavy стартанул => больше ничего не стартуем

            # обычные: стартуем пока есть место
            if self._running_count() >= self.max_parallel:
                return
            self._start_task(spec)

    def _start_task(self, spec: TaskSpec) -> bool:
        q: Queue = Queue(maxsize=1)
        started_at = _now_ts()
        deadline_at = (started_at + spec.timeout_sec) if spec.timeout_sec else None

        proc = Process(
            target=_child_entry,
            args=(spec.name, spec.fn, q),
            daemon=True,
        )
        try:
            proc.start()
        except Exception:
            self._log("start_failed", {"task": spec.name, "error": traceback.format_exc()})
            self._next_run_at[spec.name] = started_at + spec.every_sec
            return False

        rt = RunningTask(
            spec=spec,
            proc=proc,
            q=q,
            started_at=started_at,
            deadline_at=deadline_at,
            pid=proc.pid or 0,
        )
        if spec.singleton:
            self._running[spec.name] = rt
        else:
            # если singleton=False — уникализируем ключ по pid
            self._running[f"{spec.name}#{rt.pid}"] = rt

        self._log("started", {"task": spec.name, "pid": rt.pid, "heavy": spec.heavy, "timeout_sec": spec.timeout_sec})
        return True

    def _collect_finished(self) -> None:
        finished_keys: list[str] = []

        for key, rt in list(self._running.items()):
            if rt.proc.is_alive():
                continue

            # ВАЖНО: забрать exit-status у завершившегося процесса, иначе он останется zombie (<defunct>)
            try:
                rt.proc.join(timeout=0)
            except Exception:
                pass

            end_at = _now_ts()
            duration_ms = int((end_at - rt.started_at) * 1000)

            payload: Dict[str, Any] = {"task": rt.spec.name, "pid": rt.pid, "duration_ms": duration_ms}

            # попытка забрать результат (если есть)
            result: Any = None
            try:
                if not rt.q.empty():
                    result = rt.q.get_nowait()
            except Exception:
                result = {"ok": False, "error": "result_read_failed"}

            if isinstance(result, dict) and result.get("ok") is True:
                payload["event"] = "ok"
                if "result" in result:
                    payload["result"] = result["result"]
                self._log("ok", payload)
            elif isinstance(result, dict) and result.get("ok") is False:
                payload["event"] = "exception"
                payload["error"] = result.get("error", "unknown")
                payload["traceback"] = result.get("traceback", "")
                self._log("exception", payload)
            else:
                payload["event"] = "ended_empty"
                self._log("ended_empty", payload)

            # аккуратно прибираем очередь (ресурсы/потоки)
            try:
                rt.q.close()
            except Exception:
                pass
            try:
                rt.q.join_thread()
            except Exception:
                pass

            # планируем следующий запуск
            self._next_run_at[rt.spec.name] = end_at + rt.spec.every_sec

            # heavy завершился — снимаем блокировку стартов
            if self._heavy_running_name == rt.spec.name:
                self._heavy_running_name = None

            finished_keys.append(key)

        for k in finished_keys:
            self._running.pop(k, None)

    def _kill_timeouts(self) -> None:
        now = _now_ts()
        for key, rt in list(self._running.items()):
            if rt.deadline_at is None:
                continue
            if not rt.proc.is_alive():
                continue
            if now <= rt.deadline_at:
                continue

            self._log("timeout", {"task": rt.spec.name, "pid": rt.pid, "timeout_sec": rt.spec.timeout_sec})

            # terminate -> wait -> kill
            try:
                rt.proc.terminate()
            except Exception:
                pass

            t0 = _now_ts()
            while rt.proc.is_alive() and (_now_ts() - t0) < 2.0:
                time.sleep(0.05)

            if rt.proc.is_alive():
                try:
                    rt.proc.kill()
                except Exception:
                    pass

            # ВАЖНО: после terminate/kill тоже нужен join(), иначе zombie
            try:
                rt.proc.join(timeout=0.2)
            except Exception:
                pass

            # следующий запуск: "сейчас + every_sec"
            self._next_run_at[rt.spec.name] = _now_ts() + rt.spec.every_sec

    def _log(self, event: str, data: Dict[str, Any]) -> None:
        rec = {
            "ts": _iso(),
            "worker": self.name,
            "event": event,
            **(data or {}),
        }
        line = _safe_json(rec)

        # stdout
        try:
            print(line, flush=True)
        except Exception:
            pass

        # file
        if self.log_path:
            try:
                with open(self.log_path, "a", encoding="utf-8") as f:
                    f.write(line + "\n")
            except Exception:
                pass


def _child_entry(task_name: str, fn: Callable[[], Any], q: Queue) -> None:
    """
    Дочерний процесс: вызывает fn(), ловит любые исключения, кладёт результат в очередь (если может).
    """
    try:
        res = fn()
        payload: Dict[str, Any] = {"ok": True}
        if res is not None:
            payload["result"] = res
        try:
            q.put(payload, block=False)
        except Exception:
            pass
    except Exception as e:
        payload = {"ok": False, "error": str(e), "traceback": traceback.format_exc()}
        try:
            q.put(payload, block=False)
        except Exception:
            pass
        try:
            sys.exit(1)
        except Exception:
            os._exit(1)
